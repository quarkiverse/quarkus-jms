package io.quarkiverse.jms.runtime;

import static io.quarkus.arc.Arc.container;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.XAConnectionFactory;
import jakarta.transaction.TransactionManager;

import org.jboss.logging.Logger;

import io.quarkiverse.jms.JmsListenerErrorHandler;
import io.quarkiverse.jms.JmsListenerRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;

@ApplicationScoped
public class JmsListenerRegistryImpl implements JmsListenerRegistry, ConsumerContext {

    private static final Logger LOG = Logger.getLogger(JmsListenerRegistryImpl.class);

    @Inject
    JmsConnectionFactoryResolver connectionFactoryResolver;

    @Inject
    JmsRuntimeConfig config;

    @Inject
    Instance<Vertx> vertx;

    @Inject
    Instance<JmsListenerErrorHandler> errorHandler;

    @Inject
    Instance<JmsInstrumentation> instrumentations;

    @Inject
    Instance<TransactionManager> transactionManager;

    private List<JmsListenerMetadata> listeners;
    private final List<JMSContext> jmsContexts = new CopyOnWriteArrayList<>();
    private final List<WorkerExecutor> workerExecutors = new CopyOnWriteArrayList<>();
    private final List<ConsumerGroup> consumerGroups = new CopyOnWriteArrayList<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private JmsInstrumentation instrumentation = JmsInstrumentation.NOOP;
    private int idleScaleDownPolls;
    private long receiveTimeout;
    private long shutdownTimeoutMs;

    // --- Lifecycle ---

    void startListeners(List<JmsListenerMetadata> listeners) {
        this.receiveTimeout = config.receiveTimeout().toMillis();
        this.shutdownTimeoutMs = config.listener().shutdownTimeout().toMillis();
        long idleMs = config.listener().scaleDownIdleTimeout().toMillis();
        this.idleScaleDownPolls = Math.max(1, (int) (idleMs / receiveTimeout));
        this.instrumentation = JmsInstrumentation.create(instrumentations.stream().toList());
        this.listeners = listeners;
        if (listeners.isEmpty()) {
            return;
        }
        LOG.infof("Starting %d JMS listener(s)", listeners.size());
        for (JmsListenerMetadata metadata : listeners) {
            startListener(metadata);
        }
    }

    void onStop(@Observes Shutdown shutdown) {
        LOG.debug("Stopping JMS listeners");
        stopped.set(true);

        long deadline = System.currentTimeMillis() + shutdownTimeoutMs;
        boolean allIdle = false;
        while (System.currentTimeMillis() < deadline) {
            allIdle = consumerGroups.stream().allMatch(g -> g.activeCount.get() == 0);
            if (allIdle) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!allIdle) {
            LOG.warnf("Shutdown timeout: %d message(s) still in-flight",
                    consumerGroups.stream().mapToInt(g -> g.activeCount.get()).sum());
        }

        for (JMSContext context : jmsContexts) {
            try {
                context.close();
            } catch (Exception e) {
                LOG.debug("Error closing JMS context", e);
            }
        }
        jmsContexts.clear();
        for (WorkerExecutor executor : workerExecutors) {
            try {
                executor.close();
            } catch (Exception e) {
                LOG.debug("Error closing worker executor", e);
            }
        }
        workerExecutors.clear();
        consumerGroups.clear();
    }

    // --- Programmatic registration ---

    @Override
    public Closeable register(String destination, Consumer<Message> listener) {
        return register(ListenerConfig.queue(destination), listener);
    }

    @Override
    public Closeable register(ListenerConfig listenerConfig, Consumer<Message> listener) {
        return doRegister(listenerConfig, (message, ctx) -> listener.accept(message));
    }

    @Override
    public <T> Closeable register(String destination, Class<T> type, Consumer<T> listener) {
        return register(ListenerConfig.queue(destination), type, listener);
    }

    @Override
    public <T> Closeable register(ListenerConfig listenerConfig, Class<T> type, Consumer<T> listener) {
        JmsMessageConverterResolver converter = converterResolver();
        return doRegister(listenerConfig, (message, ctx) -> listener.accept(converter.fromMessage(message, type)));
    }

    private JmsMessageConverterResolver converterResolver() {
        return container().instance(JmsMessageConverterResolver.class).get();
    }

    private ConnectionFactory resolveConnectionFactory(String identifier) {
        ConnectionFactory cf = connectionFactoryResolver.resolve(identifier);
        return config.jms11() ? new JmsContextWrappingConnectionFactory(cf) : cf;
    }

    private Closeable doRegister(ListenerConfig listenerConfig, JmsListenerInvoker invoker) {
        String ackMode = resolveAcknowledgeMode(listenerConfig.acknowledgeMode());
        boolean xa = "XA".equalsIgnoreCase(ackMode);
        ConnectionFactory cf = xa ? null : resolveConnectionFactory(listenerConfig.connectionFactory());
        int sessionMode = resolveSessionMode(ackMode);
        int minConcurrency = listenerConfig.concurrency() > 0 ? listenerConfig.concurrency()
                : config.listener().concurrency();
        int maxConcurrency = Math.max(listenerConfig.maxConcurrency() > 0 ? listenerConfig.maxConcurrency()
                : config.listener().maxConcurrency().orElse(minConcurrency), minConcurrency);

        WorkerExecutor worker = vertx.get().createSharedWorkerExecutor(
                "jms-listener-" + listenerConfig.destination(), maxConcurrency);
        workerExecutors.add(worker);

        JmsListenerMetadata metadata = new JmsListenerMetadata(
                "programmatic", "register", listenerConfig.destination(),
                listenerConfig.isTopic(), listenerConfig.selector(),
                listenerConfig.connectionFactory(), minConcurrency, listenerConfig.maxConcurrency(),
                listenerConfig.subscription(), ackMode, false, null, null);

        ConsumerGroup group = new ConsumerGroup(worker, cf,
                xa ? resolveXA(metadata) : null,
                xa ? transactionManager.get() : null,
                metadata, invoker, sessionMode,
                minConcurrency, maxConcurrency, receiveTimeout, idleScaleDownPolls, config.listener().reconnectAttempts(),
                config.listener().reconnectInterval().toMillis(),
                config.listener().reconnectMaxInterval().toMillis(),
                listenerConfig.isRunOnVirtualThread());

        consumerGroups.add(group);
        for (int i = 0; i < minConcurrency; i++) {
            spawnConsumer(group, false);
        }
        LOG.infof("Registered programmatic JMS listener on %s '%s'",
                listenerConfig.isTopic() ? "topic" : "queue", listenerConfig.destination());

        return () -> {
            LOG.infof("Closing programmatic JMS listener on %s '%s'",
                    listenerConfig.isTopic() ? "topic" : "queue", listenerConfig.destination());
            consumerGroups.remove(group);
            workerExecutors.remove(worker);
            try {
                worker.close();
            } catch (Exception e) {
                LOG.debug("Error closing worker executor", e);
            }
        };
    }

    // --- Annotation-driven listeners ---

    private void startListener(JmsListenerMetadata metadata) {
        String effectiveAckMode = resolveAcknowledgeMode(metadata.getAcknowledgeMode());
        boolean xa = "XA".equalsIgnoreCase(effectiveAckMode);
        int sessionMode = resolveSessionMode(effectiveAckMode);
        int minConcurrency = metadata.getConcurrency() > 0 ? metadata.getConcurrency() : config.listener().concurrency();
        int maxConcurrency = Math.max(metadata.getMaxConcurrency() > 0 ? metadata.getMaxConcurrency()
                : config.listener().maxConcurrency().orElse(minConcurrency), minConcurrency);

        WorkerExecutor worker = vertx.get()
                .createSharedWorkerExecutor("jms-listener-" + metadata.getDestination(), maxConcurrency);
        workerExecutors.add(worker);
        ConsumerGroup group = new ConsumerGroup(
                worker,
                xa ? null : resolveConnectionFactory(metadata.getConnectionFactory()),
                xa ? resolveXA(metadata) : null,
                xa ? transactionManager.get() : null,
                metadata,
                createInvoker(metadata),
                sessionMode,
                minConcurrency,
                maxConcurrency,
                receiveTimeout,
                idleScaleDownPolls, config.listener().reconnectAttempts(),
                config.listener().reconnectInterval().toMillis(),
                config.listener().reconnectMaxInterval().toMillis(),
                metadata.isRunOnVirtualThread());

        consumerGroups.add(group);
        for (int i = 0; i < minConcurrency; i++) {
            spawnConsumer(group, false);
        }
    }

    private XAConnectionFactory resolveXA(JmsListenerMetadata metadata) {
        XAConnectionFactory xaFactory = connectionFactoryResolver.resolveXA(metadata.getConnectionFactory());
        if (xaFactory == null) {
            throw new IllegalStateException(
                    "XA acknowledge mode requires an XAConnectionFactory for listener on " + metadata.getDestination());
        }
        if (transactionManager.isUnsatisfied()) {
            throw new IllegalStateException(
                    "XA acknowledge mode requires a TransactionManager (e.g., quarkus-narayana-jta)");
        }
        return xaFactory;
    }

    @Override
    public void spawnConsumer(ConsumerGroup group, boolean scaledUp) {
        new ConsumerWorker(group, this, scaledUp).start();
    }

    // --- ConsumerContext implementation ---

    @Override
    public void trackContext(JMSContext context) {
        jmsContexts.add(context);
    }

    @Override
    public boolean isStopped() {
        return stopped.get();
    }

    @Override
    public void onListenerError(JMSContext context, String destination, Throwable error, Message message) {
        LOG.errorf(error, "Error processing JMS message on %s", destination);
        handleError(context, message, destination, error);
    }

    @Override
    public void reconnectWithBackoff(ConsumerGroup group) {
        long delay = group.reconnectIntervalMs;
        int attempts = 0;
        while (!stopped.get()) {
            if (group.reconnectAttempts >= 0 && attempts >= group.reconnectAttempts) {
                LOG.errorf("Giving up reconnection for listener on %s after %d attempts",
                        group.metadata.getDestination(), attempts);
                return;
            }
            try {
                Thread.sleep(delay);
                // create context and closed to test the connection
                group.cf.createContext(Session.AUTO_ACKNOWLEDGE).close();
                LOG.infof("Reconnected listener on %s after backoff", group.metadata.getDestination());
                return;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                attempts++;
                LOG.debugf("Reconnect attempt failed for %s, retrying in %dms",
                        group.metadata.getDestination(), delay);
                delay = Math.min(delay * 2, group.reconnectMaxIntervalMs);
            }
        }
    }

    @Override
    public JmsInstrumentation instrumentation() {
        return instrumentation;
    }

    // --- Helpers ---

    private void handleError(JMSContext context, Message message, String destination, Throwable error) {
        if (errorHandler.isResolvable()) {
            try {
                errorHandler.get().handleError(context, message, destination, error);
            } catch (Exception handlerEx) {
                LOG.error("Error in JmsListenerErrorHandler", handlerEx);
            }
        }
    }

    private JmsListenerInvoker createInvoker(JmsListenerMetadata metadata) {
        try {
            Class<?> invokerClass = Thread.currentThread().getContextClassLoader()
                    .loadClass(metadata.getInvokerClassName());
            return (JmsListenerInvoker) invokerClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create invoker for " +
                    metadata.getBeanClassName() + "." + metadata.getMethodName(), e);
        }
    }

    private String resolveAcknowledgeMode(String acknowledgeMode) {
        if (acknowledgeMode == null || acknowledgeMode.isEmpty() || "DEFAULT".equalsIgnoreCase(acknowledgeMode)) {
            return config.listener().acknowledgeMode();
        }
        return acknowledgeMode;
    }

    private int resolveSessionMode(String acknowledgeMode) {
        return switch (acknowledgeMode.toUpperCase()) {
            case "AUTO" -> Session.AUTO_ACKNOWLEDGE;
            case "CLIENT" -> Session.CLIENT_ACKNOWLEDGE;
            case "DUPS_OK" -> Session.DUPS_OK_ACKNOWLEDGE;
            case "TRANSACTED" -> Session.SESSION_TRANSACTED;
            default -> Session.AUTO_ACKNOWLEDGE;
        };
    }

    public boolean isHealthy() {
        return !jmsContexts.isEmpty() || (listeners == null || listeners.isEmpty());
    }
}
