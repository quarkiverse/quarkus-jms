package io.quarkiverse.jms.runtime.instrumentation;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.JMSException;
import jakarta.jms.Message;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.Propagator;
import io.micrometer.observation.transport.ReceiverContext;
import io.micrometer.observation.transport.SenderContext;
import io.quarkiverse.jms.runtime.JmsInstrumentation;

@ApplicationScoped
public class JmsObservationInstrumentation implements JmsInstrumentation {

    @Inject
    ObservationRegistry registry;

    private static final Propagator.Setter<Message> SETTER = (carrier, key, value) -> {
        try {
            carrier.setStringProperty(key.replace('-', '_'), value);
        } catch (JMSException e) {
            // ignore
        }
    };

    private static final Propagator.Getter<Message> GETTER = new Propagator.Getter<>() {
        @Override
        public String get(Message carrier, String key) {
            try {
                return carrier.getStringProperty(key.replace('-', '_'));
            } catch (JMSException e) {
                return null;
            }
        }
    };

    @Override
    public Scope onSend(String destination, boolean isTopic) {
        SenderContext<Message> context = new SenderContext<>(SETTER, Kind.PRODUCER);
        Observation observation = Observation.createNotStarted(destination + " publish", () -> context, registry)
                .lowCardinalityKeyValue("messaging.system", "jms")
                .lowCardinalityKeyValue("messaging.destination.name", destination)
                .lowCardinalityKeyValue("messaging.destination.kind", isTopic ? "topic" : "queue");
        return new ObservationScope(observation, context);
    }

    @Override
    public Scope onReceive(String destination, boolean isTopic) {
        ReceiverContext<Message> context = new ReceiverContext<>(GETTER, Kind.CONSUMER);
        Observation observation = Observation.createNotStarted(destination + " receive", () -> context, registry)
                .lowCardinalityKeyValue("messaging.system", "jms")
                .lowCardinalityKeyValue("messaging.destination.name", destination)
                .lowCardinalityKeyValue("messaging.destination.kind", isTopic ? "topic" : "queue");
        return new ObservationScope(observation, context);
    }

    @Override
    public Scope onProcess(String destination, boolean isTopic) {
        ReceiverContext<Message> context = new ReceiverContext<>(GETTER, Kind.CONSUMER);
        Observation observation = Observation.createNotStarted(destination + " process", () -> context, registry)
                .lowCardinalityKeyValue("messaging.system", "jms")
                .lowCardinalityKeyValue("messaging.destination.name", destination)
                .lowCardinalityKeyValue("messaging.destination.kind", isTopic ? "topic" : "queue")
                .lowCardinalityKeyValue("messaging.operation.type", "process");
        return new ObservationScope(observation, context);
    }

    private static class ObservationScope implements Scope {

        private final Observation observation;
        private final Observation.Context context;
        private Observation.Scope scope;
        private boolean started; // safe: scope instances are confined to a single thread

        ObservationScope(Observation observation, Observation.Context context) {
            this.observation = observation;
            this.context = context;
        }

        @Override
        public void onMessage(Message message) {
            if (message == null) {
                return;
            }
            if (context instanceof SenderContext sender) {
                sender.setCarrier(message);
            } else if (context instanceof ReceiverContext receiver) {
                receiver.setCarrier(message);
            }
            try {
                String messageId = message.getJMSMessageID();
                if (messageId != null) {
                    observation.highCardinalityKeyValue("messaging.message.id", messageId);
                }
            } catch (JMSException e) {
                // ignore
            }
            observation.start();
            started = true;
            scope = observation.openScope();
        }

        @Override
        public void onError(Throwable t) {
            if (started) {
                observation.error(t);
            }
        }

        @Override
        public void close() {
            if (!started) {
                return;
            }
            if (scope != null) {
                scope.close();
            }
            observation.stop();
        }
    }
}
