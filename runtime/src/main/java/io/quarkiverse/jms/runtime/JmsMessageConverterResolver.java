package io.quarkiverse.jms.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;

import io.quarkiverse.jms.JmsConversionException;
import io.quarkiverse.jms.JmsMessageConverter;
import io.quarkiverse.jms.ReceiveMessage;
import io.quarkus.arc.All;

/**
 * Resolves the appropriate {@link JmsMessageConverter} for a given type.
 * Resolution order: custom converters (by {@link jakarta.annotation.Priority}),
 * then {@link BuiltInJmsMessageConverter}, then {@link JacksonJmsMessageConverter}.
 * Results are cached per type.
 */
@ApplicationScoped
public class JmsMessageConverterResolver {

    private final BuiltInJmsMessageConverter builtIn;
    private final JmsMessageConverter jackson;
    private final List<JmsMessageConverter> customConverters;
    private final ConcurrentHashMap<Class<?>, JmsMessageConverter> cache = new ConcurrentHashMap<>();

    @Inject
    public JmsMessageConverterResolver(BuiltInJmsMessageConverter builtIn,
            Instance<JacksonJmsMessageConverter> jacksonInstance,
            @All List<JmsMessageConverter> allConverters) {
        this.builtIn = builtIn;
        this.jackson = jacksonInstance.isResolvable() ? jacksonInstance.get() : null;
        this.customConverters = new ArrayList<>();
        for (JmsMessageConverter converter : allConverters) {
            if (converter != builtIn && converter != jackson) {
                customConverters.add(converter);
            }
        }
    }

    public Message toMessage(Object payload, JMSContext context) {
        return resolve(payload.getClass()).toMessage(payload, context);
    }

    public <T> T fromMessage(Message message, Class<T> type) {
        return resolve(type).fromMessage(message, type);
    }

    public <T> ReceiveMessage<T> receivedFromMessage(Message message, Class<T> type) {
        return new ReceiveMessage<>(fromMessage(message, type), message);
    }

    private JmsMessageConverter resolve(Class<?> type) {
        return cache.computeIfAbsent(type, t -> {
            for (JmsMessageConverter converter : customConverters) {
                if (converter.supports(t)) {
                    return converter;
                }
            }
            if (builtIn.supports(t)) {
                return builtIn;
            }
            if (jackson != null) {
                return jackson;
            }
            throw new JmsConversionException(
                    "No JMS message converter found for type " + t.getName()
                            + ". Add the quarkus-jackson dependency or provide a custom JmsMessageConverter.",
                    null);
        });
    }
}
