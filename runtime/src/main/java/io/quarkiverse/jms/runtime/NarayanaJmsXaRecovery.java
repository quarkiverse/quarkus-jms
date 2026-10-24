package io.quarkiverse.jms.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.XAConnectionFactory;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.narayana.jta.jms.JmsXAResourceRecoveryHelper;
import org.jboss.tm.XAResourceRecoveryRegistry;

import io.quarkiverse.jms.spi.JmsXaRecovery;

@ApplicationScoped
public class NarayanaJmsXaRecovery implements JmsXaRecovery {

    @Inject
    XAResourceRecoveryRegistry registry;

    @Override
    public void register(XAConnectionFactory xaFactory) {
        boolean recoveryEnabled = ConfigProvider.getConfig()
                .getOptionalValue("quarkus.transaction-manager.enable-recovery", Boolean.class)
                .orElse(false);
        if (!recoveryEnabled) {
            return;
        }
        JmsXAResourceRecoveryHelper helper = new JmsXAResourceRecoveryHelper(xaFactory);
        registry.addXAResourceRecovery(helper::getXAResources);
    }
}
