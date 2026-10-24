package io.quarkiverse.jms;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a JMS message listener. The annotated method is invoked
 * for each message received on the specified destination.
 *
 * <p>
 * Supported parameter types:
 * <ul>
 * <li>{@link jakarta.jms.Message} or subtypes ({@link jakarta.jms.TextMessage}, etc.)</li>
 * <li>{@link String} — text message body</li>
 * <li>{@code byte[]} — bytes message body</li>
 * <li>Any POJO — deserialized from JSON via {@link JmsMessageConverter}</li>
 * </ul>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(JmsListeners.class)
public @interface JmsListener {

    /**
     * The destination name (queue or topic).
     */
    String value();

    /**
     * Whether the destination is a topic. Defaults to {@code false} (queue).
     */
    boolean topic() default false;

    /**
     * JMS message selector expression.
     */
    String selector() default "";

    /**
     * Named {@link jakarta.jms.ConnectionFactory} identifier.
     * When empty, the default {@code ConnectionFactory} bean is used.
     */
    String connectionFactory() default "";

    /**
     * Number of concurrent consumers. Only applicable to queues.
     * When {@link #maxConcurrency()} is set to a higher value, this is the
     * minimum number of consumers that are always active.
     * {@code 0} (default) means use {@code quarkus.jms.listener.concurrency}.
     */
    int concurrency() default 0;

    /**
     * Maximum number of concurrent consumers for dynamic scaling.
     * When greater than {@link #concurrency()}, the container scales
     * between the two values based on load. When {@code -1} (default),
     * the concurrency is fixed.
     */
    int maxConcurrency() default -1;

    /**
     * Durable subscription name. Only applicable to topics.
     */
    String subscription() default "";

    /**
     * Acknowledge mode for this listener.
     * When not set, uses {@code quarkus.jms.listener.acknowledge-mode}.
     */
    AcknowledgeMode acknowledge() default AcknowledgeMode.AUTO;

    enum AcknowledgeMode {
        AUTO,
        CLIENT,
        DUPS_OK,
        TRANSACTED,
        XA
    }
}
