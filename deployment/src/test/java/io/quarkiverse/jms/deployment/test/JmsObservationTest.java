package io.quarkiverse.jms.deployment.test;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

public class JmsObservationTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .withApplicationRoot(jar -> jar
                    .addClasses(ObservedListener.class, TestRegistryProducer.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    JmsClient jmsClient;

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    ObservedListener listener;

    @Inject
    TestObservationRegistry observationRegistry;

    @BeforeEach
    void clearObservations() {
        observationRegistry.clear();
    }

    @Test
    void sendCreatesPublishObservation() {
        jmsClient.queue("obs-send").send("test");

        TestObservationRegistryAssert.assertThat(observationRegistry)
                .hasObservationWithNameEqualTo("obs-send publish")
                .that()
                .hasBeenStarted()
                .hasBeenStopped()
                .hasLowCardinalityKeyValue("messaging.system", "jms")
                .hasLowCardinalityKeyValue("messaging.destination.name", "obs-send")
                .hasLowCardinalityKeyValue("messaging.destination.kind", "queue");
    }

    @Test
    void listenerCreatesProcessObservation() {
        try (JMSContext ctx = connectionFactory.createContext()) {
            ctx.createProducer().send(ctx.createQueue("obs-queue"), "observed-msg");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> listener.getMessages().contains("observed-msg"));

        TestObservationRegistryAssert.assertThat(observationRegistry)
                .hasObservationWithNameEqualTo("obs-queue process")
                .that()
                .hasBeenStarted()
                .hasBeenStopped()
                .hasLowCardinalityKeyValue("messaging.system", "jms")
                .hasLowCardinalityKeyValue("messaging.destination.name", "obs-queue")
                .hasLowCardinalityKeyValue("messaging.operation.type", "process");
    }

    @Test
    void receiveCreatesReceiveObservation() {
        jmsClient.queue("obs-recv").send("for-receive");

        jmsClient.queue("obs-recv").receive(String.class);

        TestObservationRegistryAssert.assertThat(observationRegistry)
                .hasObservationWithNameEqualTo("obs-recv receive")
                .that()
                .hasBeenStarted()
                .hasBeenStopped()
                .hasLowCardinalityKeyValue("messaging.system", "jms")
                .hasLowCardinalityKeyValue("messaging.destination.name", "obs-recv")
                .hasLowCardinalityKeyValue("messaging.destination.kind", "queue");
    }

    @ApplicationScoped
    public static class TestRegistryProducer {

        @Produces
        @ApplicationScoped
        @Alternative
        @Priority(1)
        TestObservationRegistry testObservationRegistry() {
            return TestObservationRegistry.create();
        }
    }

    @ApplicationScoped
    public static class ObservedListener {

        private final List<String> messages = new CopyOnWriteArrayList<>();

        @JmsListener("obs-queue")
        public void onMessage(String message) {
            messages.add(message);
        }

        public List<String> getMessages() {
            return messages;
        }
    }
}
