package io.quarkiverse.jms.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.XAConnectionFactory;
import jakarta.jms.XAJMSContext;
import jakarta.transaction.Status;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.jboss.logging.Logger;

import io.quarkiverse.jms.JmsDestination;
import io.quarkiverse.jms.JmsException;
import io.quarkiverse.jms.JmsReceiveBuilder;
import io.quarkiverse.jms.JmsReceiveException;
import io.quarkiverse.jms.JmsReceiver;
import io.quarkiverse.jms.JmsSendBuilder;
import io.quarkiverse.jms.JmsSendException;
import io.quarkiverse.jms.ReceiveMessage;
import io.quarkiverse.jms.SendMessage;

abstract class JmsDestinationImpl<T> implements JmsDestination<T> {

    private static final Logger LOG = Logger.getLogger(JmsDestinationImpl.class);

    final JmsClientImpl client;
    final String destinationName;
    final boolean isTopic;
    final Class<T> messageType;
    final String connectionFactoryId;
    final String defaultSelector;
    final long defaultTimeToLive;
    final long defaultDeliveryDelay;
    final int defaultDeliveryMode;
    final int defaultPriority;
    private volatile ConnectionFactory resolvedFactory;

    JmsDestinationImpl(JmsClientImpl client, String destinationName, boolean isTopic,
            Class<T> messageType, String connectionFactoryId, String defaultSelector) {
        this(client, destinationName, isTopic, messageType, connectionFactoryId, defaultSelector,
                client.defaultTimeToLive, client.defaultDeliveryDelay,
                client.defaultDeliveryMode, client.defaultPriority);
    }

    JmsDestinationImpl(JmsClientImpl client, String destinationName, boolean isTopic,
            Class<T> messageType, String connectionFactoryId, String defaultSelector,
            long defaultTimeToLive, long defaultDeliveryDelay, int defaultDeliveryMode, int defaultPriority) {
        this.client = client;
        this.destinationName = destinationName;
        this.isTopic = isTopic;
        this.messageType = messageType;
        this.connectionFactoryId = connectionFactoryId;
        this.defaultSelector = defaultSelector;
        this.defaultTimeToLive = defaultTimeToLive;
        this.defaultDeliveryDelay = defaultDeliveryDelay;
        this.defaultDeliveryMode = defaultDeliveryMode;
        this.defaultPriority = defaultPriority;
    }

    // --- Message builder ---

    @Override
    public JmsSendBuilder<T> message(T payload) {
        return new JmsSendBuilderImpl<>(this, SendMessage.of(payload));
    }

    @Override
    public JmsSendBuilder<T> message(SendMessage<? extends T> sendMessage) {
        return new JmsSendBuilderImpl<>(this, sendMessage);
    }

    // --- Receive builder ---

    @Override
    public JmsReceiveBuilder<T> select(String selector) {
        return new JmsReceiveBuilderImpl<>(this).select(selector);
    }

    @Override
    public JmsReceiveBuilder<T> receiveTimeout(Duration timeout) {
        return new JmsReceiveBuilderImpl<>(this).receiveTimeout(timeout);
    }

    // --- Convenience send ---

    @Override
    public void send(T payload) {
        doSend(SendMessage.of(payload), defaultTimeToLive, defaultDeliveryDelay,
                defaultDeliveryMode, defaultPriority, null, null, null);
    }

    @Override
    public void send(SendMessage<? extends T> sendMessage) {
        doSend(sendMessage, defaultTimeToLive, defaultDeliveryDelay,
                defaultDeliveryMode, defaultPriority, null, null, null);
    }

    @Override
    public void send(Function<JMSContext, Message> messageCreator) {
        try (JmsClientImpl.ManagedContext ctx = acquireContext()) {
            Destination destination = createDestination(ctx.context());
            Message message = messageCreator.apply(ctx.context());
            ctx.context().createProducer().send(destination, message);
        }
    }

    // --- Convenience receive ---

    @Override
    public Optional<T> receive() {
        return doReceive(messageType, defaultSelector, client.defaultReceiveTimeout);
    }

    @Override
    public Optional<ReceiveMessage<T>> receiveMessage() {
        return doReceiveMessage(messageType, defaultSelector, client.defaultReceiveTimeout);
    }

    @Override
    public <U> Optional<U> receive(Class<U> type) {
        return doReceive(type, defaultSelector, client.defaultReceiveTimeout);
    }

    @Override
    public <U> Optional<ReceiveMessage<U>> receiveMessage(Class<U> type) {
        return doReceiveMessage(type, defaultSelector, client.defaultReceiveTimeout);
    }

    // --- Convenience request/reply ---

    @Override
    public <R> R sendAndReceive(T payload, Class<R> responseType) {
        return sendAndReceive(SendMessage.of(payload), responseType);
    }

    @Override
    public <R> R sendAndReceive(SendMessage<? extends T> sendMessage, Class<R> responseType) {
        Message reply = doSendAndReceiveRaw(sendMessage, client.defaultReceiveTimeout, null,
                defaultTimeToLive, defaultDeliveryDelay,
                defaultDeliveryMode, defaultPriority, null, null, null);
        return client.converterResolver.fromMessage(reply, responseType);
    }

    @Override
    public <R> ReceiveMessage<R> sendAndReceiveMessage(T payload, Class<R> responseType) {
        return sendAndReceiveMessage(SendMessage.of(payload), responseType);
    }

    @Override
    public <R> ReceiveMessage<R> sendAndReceiveMessage(SendMessage<? extends T> sendMessage, Class<R> responseType) {
        Message reply = doSendAndReceiveRaw(sendMessage, client.defaultReceiveTimeout, null,
                defaultTimeToLive, defaultDeliveryDelay,
                defaultDeliveryMode, defaultPriority, null, null, null);
        return client.converterResolver.receivedFromMessage(reply, responseType);
    }

    @Override
    public void receiveWith(Consumer<JmsReceiver<T>> callback) {
        receiveWith(receiver -> {
            callback.accept(receiver);
            return null;
        });
    }

    @Override
    public <R> R receiveWith(Function<JmsReceiver<T>, R> callback) {
        return doReceiveWith(defaultSelector, client.defaultReceiveTimeout, callback);
    }

    @Override
    public <U> U execute(Function<JMSContext, U> callback) {
        try (JmsClientImpl.ManagedContext ctx = acquireContext()) {
            return callback.apply(ctx.context());
        }
    }

    // --- Internal methods (called by builders) ---

    void doSend(SendMessage<? extends T> sendMessage,
            long timeToLive, long deliveryDelay, int deliveryMode, int priority,
            Map<String, Object> properties, Function<Message, String> correlationIdProvider,
            Function<JMSContext, Destination> replyToFactory) {
        JmsInstrumentation.Scope scope = client.instrumentation.onSend(destinationName, isTopic);
        try (JmsClientImpl.ManagedContext ctx = acquireContext()) {
            Destination destination = createDestination(ctx.context());
            Message message = sendMessage.apply(client.converterResolver.toMessage(sendMessage.payload(), ctx.context()));
            scope.onMessage(message);
            String correlationId = resolveCorrelationId(correlationIdProvider, message);
            applyMessageConfig(message, ctx.context(), properties, correlationId, replyToFactory);
            JMSProducer producer = createProducer(ctx.context(), timeToLive, deliveryDelay, deliveryMode, priority);
            producer.send(destination, message);
        } catch (JmsException e) {
            scope.onError(e);
            throw e;
        } catch (Throwable t) {
            scope.onError(t);
            throw new JmsSendException("Failed to send JMS message to " + destinationName, t);
        } finally {
            scope.close();
        }
    }

    <U> Optional<U> doReceive(Class<U> type, String selector, Duration receiveTimeout) {
        Message received = doReceiveRaw(selector, receiveTimeout);
        if (received == null) {
            return Optional.empty();
        }
        return Optional.of(client.converterResolver.fromMessage(received, type));
    }

    <U> Optional<ReceiveMessage<U>> doReceiveMessage(Class<U> type, String selector, Duration receiveTimeout) {
        Message received = doReceiveRaw(selector, receiveTimeout);
        if (received == null) {
            return Optional.empty();
        }
        return Optional.of(client.converterResolver.receivedFromMessage(received, type));
    }

    Message doReceiveRaw(String selector, Duration receiveTimeout) {
        JmsInstrumentation.Scope scope = client.instrumentation.onReceive(destinationName, isTopic);
        try (JmsClientImpl.ManagedContext ctx = acquireContext()) {
            Destination destination = createDestination(ctx.context());
            JMSConsumer consumer = selector != null
                    ? ctx.context().createConsumer(destination, selector)
                    : ctx.context().createConsumer(destination);
            Message message = consumer.receive(receiveTimeout.toMillis());
            scope.onMessage(message);
            return message;
        } catch (JmsException e) {
            scope.onError(e);
            throw e;
        } catch (Throwable t) {
            scope.onError(t);
            throw new JmsReceiveException("Failed to receive JMS message from " + destinationName, t);
        } finally {
            scope.close();
        }
    }

    <R> R doReceiveWith(String selector, Duration defaultTimeout, Function<JmsReceiver<T>, R> callback) {
        try (JmsClientImpl.ManagedContext ctx = acquireContext()) {
            Destination destination = createDestination(ctx.context());
            JMSConsumer consumer = selector != null
                    ? ctx.context().createConsumer(destination, selector)
                    : ctx.context().createConsumer(destination);
            try (JmsReceiverImpl<T> receiver = new JmsReceiverImpl<>(consumer, client.converterResolver,
                    client.instrumentation, destinationName, isTopic, messageType, defaultTimeout)) {
                return callback.apply(receiver);
            }
        } catch (JmsException e) {
            throw e;
        } catch (Exception e) {
            throw new JmsReceiveException("Failed to receive JMS messages from " + destinationName, e);
        }
    }

    Message doSendAndReceiveRaw(SendMessage<? extends T> sendMessage, Duration receiveTimeout,
            String replySelector,
            long timeToLive, long deliveryDelay, int deliveryMode, int priority,
            Map<String, Object> properties, Function<Message, String> correlationIdProvider,
            Function<JMSContext, Destination> replyToFactory) {
        try (JmsClientImpl.ManagedContext ctx = acquireContext()) {
            Destination destination = createDestination(ctx.context());
            Message message = sendMessage.apply(client.converterResolver.toMessage(sendMessage.payload(), ctx.context()));
            if (replyToFactory == null) {
                message.setJMSReplyTo(ctx.context().createTemporaryQueue());
            }
            String correlationId = resolveCorrelationId(correlationIdProvider, message);
            applyMessageConfig(message, ctx.context(), properties, correlationId, replyToFactory);
            JMSProducer producer = createProducer(ctx.context(), timeToLive, deliveryDelay, deliveryMode, priority);
            producer.send(destination, message);

            // Build reply selector (post-send — JMSMessageID is now available)
            Destination replyTo = message.getJMSReplyTo();
            if (replySelector == null && replyToFactory != null) {
                String corrId = message.getJMSCorrelationID();
                if (corrId == null) {
                    corrId = message.getJMSMessageID();
                }
                if (corrId != null) {
                    replySelector = "JMSCorrelationID = '" + corrId + "'";
                }
            }
            JMSConsumer replyConsumer = replySelector != null
                    ? ctx.context().createConsumer(replyTo, replySelector)
                    : ctx.context().createConsumer(replyTo);
            Message reply = replyConsumer.receive(receiveTimeout.toMillis());
            if (reply == null) {
                throw new JmsReceiveException("Timed out waiting for reply on " + destinationName, null);
            }
            return reply;
        } catch (JmsException e) {
            throw e;
        } catch (Exception e) {
            throw new JmsReceiveException("Failed to send and receive JMS message on " + destinationName, e);
        }
    }

    List<T> doBrowse(String selector) {
        try (JmsClientImpl.ManagedContext ctx = acquireContext();
                QueueBrowser browser = createBrowser(ctx.context(), selector)) {
            List<T> results = new ArrayList<>();
            var enumeration = browser.getEnumeration();
            while (enumeration.hasMoreElements()) {
                results.add(client.converterResolver.fromMessage((Message) enumeration.nextElement(), messageType));
            }
            return results;
        } catch (JmsException e) {
            throw e;
        } catch (Exception e) {
            throw new JmsReceiveException("Failed to browse queue " + destinationName, e);
        }
    }

    List<ReceiveMessage<T>> doBrowseMessages(String selector) {
        try (JmsClientImpl.ManagedContext ctx = acquireContext();
                QueueBrowser browser = createBrowser(ctx.context(), selector)) {
            List<ReceiveMessage<T>> results = new ArrayList<>();
            var enumeration = browser.getEnumeration();
            while (enumeration.hasMoreElements()) {
                results.add(client.converterResolver.receivedFromMessage((Message) enumeration.nextElement(), messageType));
            }
            return results;
        } catch (JmsException e) {
            throw e;
        } catch (Exception e) {
            throw new JmsReceiveException("Failed to browse queue " + destinationName, e);
        }
    }

    private QueueBrowser createBrowser(JMSContext context, String selector) {
        return selector != null
                ? context.createBrowser(context.createQueue(destinationName), selector)
                : context.createBrowser(context.createQueue(destinationName));
    }

    private static String resolveCorrelationId(Function<Message, String> provider, Message message) {
        if (provider == null) {
            return null;
        }
        String id = provider.apply(message);
        if (id != null && id.isEmpty()) {
            return null;
        }
        return id;
    }

    // --- Helpers ---

    private void applyMessageConfig(Message message, JMSContext context,
            Map<String, Object> properties, String correlationId,
            Function<JMSContext, Destination> replyToFactory) {
        try {
            if (correlationId != null) {
                message.setJMSCorrelationID(correlationId);
            }
            if (replyToFactory != null) {
                message.setJMSReplyTo(replyToFactory.apply(context));
            }
            if (properties != null) {
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    message.setObjectProperty(entry.getKey(), entry.getValue());
                }
            }
        } catch (JMSException e) {
            throw new JmsException("Failed to apply message properties", e);
        }
    }

    private JMSProducer createProducer(JMSContext context,
            long timeToLive, long deliveryDelay, int deliveryMode, int priority) {
        JMSProducer producer = context.createProducer();
        if (timeToLive >= 0) {
            producer.setTimeToLive(timeToLive);
        }
        if (deliveryDelay >= 0) {
            producer.setDeliveryDelay(deliveryDelay);
        }
        if (deliveryMode >= 0) {
            producer.setDeliveryMode(deliveryMode);
        }
        if (priority >= 0) {
            producer.setPriority(priority);
        }
        return producer;
    }

    // --- Context management ---

    JmsClientImpl.ManagedContext acquireContext() {
        Transaction activeTransaction = getActiveTransaction();
        if (activeTransaction == null) {
            return new JmsClientImpl.ManagedContext(resolvedFactory().createContext(), false);
        }

        XAConnectionFactory xaCf = client.connectionFactoryResolver.resolveXA(connectionFactoryId);
        if (xaCf != null) {
            XAJMSContext xaContext = xaCf.createXAContext();
            try {
                activeTransaction.enlistResource(xaContext.getXAResource());
                activeTransaction.registerSynchronization(new JmsClientImpl.ContextCloser(xaContext));
            } catch (Exception e) {
                try {
                    activeTransaction.rollback();
                } catch (Exception re) {
                    e.addSuppressed(re);
                }
                xaContext.close();
                throw new JmsException("Failed to enlist JMS XA resource", e);
            }
            return new JmsClientImpl.ManagedContext(xaContext, true);
        } else {
            LOG.warnf("No XAConnectionFactory available for '%s' — falling back to a local transacted session. "
                    + "JMS and other resources will NOT commit atomically.", destinationName);
            JMSContext txContext = resolvedFactory().createContext(Session.SESSION_TRANSACTED);
            try {
                activeTransaction.registerSynchronization(
                        new JmsClientImpl.TransactedContextSynchronization(txContext));
            } catch (Exception e) {
                txContext.close();
                throw new JmsException(
                        "Failed to register JMS session synchronization", e);
            }
            return new JmsClientImpl.ManagedContext(txContext, true);
        }
    }

    private ConnectionFactory resolvedFactory() {
        ConnectionFactory cf = resolvedFactory;
        if (cf != null) {
            return cf;
        }
        cf = client.resolveFactory(connectionFactoryId);
        resolvedFactory = cf;
        return cf;
    }

    private Transaction getActiveTransaction() {
        if (client.transactionManagerInstance.isUnsatisfied()) {
            return null;
        }
        try {
            TransactionManager tm = client.transactionManagerInstance.get();
            Transaction tx = tm.getTransaction();
            if (tx != null && tx.getStatus() == Status.STATUS_ACTIVE) {
                return tx;
            }
        } catch (Exception e) {
            // No active transaction
        }
        return null;
    }

    Destination createDestination(JMSContext context) {
        return client.getOrCreateDestination(destinationName, isTopic, context);
    }
}
