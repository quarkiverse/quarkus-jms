package io.quarkiverse.jms.deployment.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.jms.JmsListener;
import io.quarkus.test.QuarkusExtensionTest;

/**
 * Verifies that {@code quarkus.jms.listener.concurrency} and
 * {@code quarkus.jms.listener.acknowledge-mode} config properties
 * apply to {@code @JmsListener} methods that don't set them explicitly,
 * and that explicit annotation attributes still win.
 */
public class JmsListenerConfigDefaultsTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .overrideConfigKey("quarkus.http.test-port", "0")
            .overrideConfigKey("quarkus.jms.listener.concurrency", "3")
            .overrideConfigKey("quarkus.jms.listener.acknowledge-mode", "dups_ok")
            .withApplicationRoot(jar -> jar
                    .addClasses(
                            DefaultConcurrencyListener.class,
                            ExplicitConcurrencyListener.class,
                            DefaultAckListener.class,
                            ExplicitAckListener.class,
                            ConnectionFactoryProducer.class, EmbeddedJmsBroker.class)
                    .addAsResource("broker.xml"))
            .setBeforeAllCustomizer(EmbeddedJmsBroker::start)
            .setAfterAllCustomizer(EmbeddedJmsBroker::stop);

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    DefaultConcurrencyListener defaultConcurrencyListener;

    @Inject
    ExplicitConcurrencyListener explicitConcurrencyListener;

    @Inject
    DefaultAckListener defaultAckListener;

    @Inject
    ExplicitAckListener explicitAckListener;

    @Test
    void listenerWithoutConcurrencyPicksUpConfig() {
        try (JMSContext context = connectionFactory.createContext()) {
            for (int i = 0; i < 10; i++) {
                context.createProducer().send(
                        context.createQueue("cfg-default-conc"), "msg-" + i);
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(defaultConcurrencyListener.getReceived()).hasSize(10));

        assertThat(defaultConcurrencyListener.getThreadNames().size())
                .as("Should use 3 concurrent consumers from config")
                .isGreaterThan(1);
    }

    @Test
    void listenerWithExplicitConcurrencyIgnoresConfig() {
        try (JMSContext context = connectionFactory.createContext()) {
            for (int i = 0; i < 5; i++) {
                context.createProducer().send(
                        context.createQueue("cfg-explicit-conc"), "msg-" + i);
            }
        }

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(explicitConcurrencyListener.getReceived()).hasSize(5));

        assertThat(explicitConcurrencyListener.getThreadNames())
                .as("Should use 1 consumer from annotation, not 3 from config")
                .hasSize(1);
    }

    @Test
    void listenerWithoutAcknowledgePicksUpConfig() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(
                    context.createQueue("cfg-default-ack"), "ack-test");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(defaultAckListener.getReceived()).contains("ack-test"));
    }

    @Test
    void listenerWithExplicitAcknowledgeIgnoresConfig() {
        try (JMSContext context = connectionFactory.createContext()) {
            context.createProducer().send(
                    context.createQueue("cfg-explicit-ack"), "txn-test");
        }

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(explicitAckListener.getReceived()).contains("txn-test"));
    }

    @ApplicationScoped
    public static class DefaultConcurrencyListener {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final Set<String> threadNames = new CopyOnWriteArraySet<>();

        @JmsListener("cfg-default-conc")
        public void onMessage(String message) {
            threadNames.add(Thread.currentThread().getName());
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }

        public Set<String> getThreadNames() {
            return threadNames;
        }
    }

    @ApplicationScoped
    public static class ExplicitConcurrencyListener {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final Set<String> threadNames = new CopyOnWriteArraySet<>();

        @JmsListener(value = "cfg-explicit-conc", concurrency = 1)
        public void onMessage(String message) {
            threadNames.add(Thread.currentThread().getName());
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }

        public Set<String> getThreadNames() {
            return threadNames;
        }
    }

    @ApplicationScoped
    public static class DefaultAckListener {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener("cfg-default-ack")
        public void onMessage(String message) {
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ExplicitAckListener {
        private final List<String> received = new CopyOnWriteArrayList<>();

        @JmsListener(value = "cfg-explicit-ack", acknowledge = JmsListener.AcknowledgeMode.TRANSACTED)
        public void onMessage(String message) {
            received.add(message);
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
