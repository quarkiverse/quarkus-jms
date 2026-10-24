package io.quarkiverse.it.jms.xapooled;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class DatabaseInit {

    @Inject
    DataSource dataSource;

    void onStart(@Observes StartupEvent ev) {
        try (var conn = dataSource.getConnection();
                var stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS xa_messages (id SERIAL PRIMARY KEY, payload TEXT NOT NULL)");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize database", e);
        }
    }
}
