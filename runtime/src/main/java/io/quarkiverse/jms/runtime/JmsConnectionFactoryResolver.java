package io.quarkiverse.jms.runtime;

import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.XAConnectionFactory;

import io.quarkiverse.jms.spi.JmsXaRecovery;
import io.quarkus.arc.ClientProxy;
import io.smallrye.common.annotation.Identifier;

/**
 * Resolves and caches {@link ConnectionFactory} beans from the CDI container,
 * supporting named instances via {@link Identifier}.
 * <p>
 * Results are cached per identifier — subsequent calls with the same
 * identifier return the cached factory without re-querying CDI.
 */
@Singleton
public class JmsConnectionFactoryResolver {

    @Inject
    @Any
    Instance<ConnectionFactory> connectionFactories;

    @Inject
    @Any
    Instance<XAConnectionFactory> xaFactories;

    @Inject
    Instance<JmsXaRecovery> xaRecovery;

    private final ConcurrentHashMap<String, ConnectionFactory> cfCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, XAConnectionFactory> xaCache = new ConcurrentHashMap<>();

    /**
     * Resolve a {@link ConnectionFactory} by identifier. Results are cached.
     */
    public ConnectionFactory resolve(String identifier) {
        String key = key(identifier);
        return cfCache.computeIfAbsent(key, k -> select(connectionFactories, identifier).get());
    }

    /**
     * Resolve as {@link XAConnectionFactory} by identifier. First looks for an
     * explicit {@link XAConnectionFactory} bean; falls back to unwrapping the
     * {@link ConnectionFactory} proxy if the underlying instance implements it.
     *
     * @return the XA factory, or {@code null} if not XA-capable
     */
    public XAConnectionFactory resolveXA(String identifier) {
        String key = key(identifier);
        return xaCache.computeIfAbsent(key, k -> {
            Instance<XAConnectionFactory> instance = select(xaFactories, identifier);
            if (instance.isResolvable()) {
                return instance.get();
            }
            // CDI proxy only implements the declared bean type (ConnectionFactory),
            // unwrap to check if the actual instance also implements XAConnectionFactory
            ConnectionFactory cf = resolve(identifier);
            Object unwrapped = ClientProxy.unwrap(cf);
            if (unwrapped instanceof XAConnectionFactory xa) {
                if (xaRecovery.isResolvable()) {
                    xaRecovery.get().register(xa);
                }
                return xa;
            }
            return null;
        });
    }

    private static String key(String identifier) {
        return identifier != null && !identifier.isEmpty() ? identifier : "__default__";
    }

    private <T> Instance<T> select(Instance<T> instances, String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return instances.select(Default.Literal.INSTANCE);
        }
        return instances.select(Identifier.Literal.of(identifier));
    }
}
