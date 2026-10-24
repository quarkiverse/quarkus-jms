package io.quarkiverse.jms.spi;

import jakarta.jms.XAConnectionFactory;

/**
 * Registers an {@link XAConnectionFactory} for XA transaction recovery.
 * <p>
 * Quarkus JMS calls {@link #register(XAConnectionFactory)} once for each
 * resolved {@code XAConnectionFactory}. Implementations should register the
 * factory with the transaction manager's recovery module so that in-doubt
 * branches can be resolved after a crash.
 */
public interface JmsXaRecovery {

    void register(XAConnectionFactory xaFactory);
}
