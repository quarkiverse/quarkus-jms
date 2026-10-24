package io.quarkiverse.it.jms.xapooled;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/pooled-xa")
@Produces(MediaType.APPLICATION_JSON)
public class PooledXaResource {

    @Inject
    ConnectionFactory connectionFactory;

    @Inject
    DataSource dataSource;

    @POST
    @Path("/send-and-insert")
    @Transactional
    public void sendAndInsert(String message) {
        try (JMSContext context = connectionFactory.createContext(Session.SESSION_TRANSACTED)) {
            context.createProducer().send(context.createQueue("pooled-xa-queue"), message);
        }
        dbInsert("pooled-sent:" + message);
    }

    @POST
    @Path("/send-and-rollback")
    @Transactional
    public void sendAndRollback(String message) {
        try (JMSContext context = connectionFactory.createContext(Session.SESSION_TRANSACTED)) {
            context.createProducer().send(context.createQueue("pooled-xa-queue"), message);
        }
        dbInsert("pooled-should-not-persist:" + message);
        throw new RuntimeException("Intentional rollback");
    }

    @POST
    @Path("/receive-and-insert")
    @Transactional
    public String receiveAndInsert() {
        try (JMSContext context = connectionFactory.createContext(Session.SESSION_TRANSACTED);
                JMSConsumer consumer = context.createConsumer(context.createQueue("pooled-xa-queue"))) {
            Message msg = consumer.receive(5000);
            if (msg == null) {
                return null;
            }
            String body = msg.getBody(String.class);
            dbInsert("pooled-received:" + body);
            return body;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @GET
    @Path("/db-messages")
    public List<String> dbMessages() {
        try (var conn = dataSource.getConnection();
                var stmt = conn.createStatement();
                var rs = stmt.executeQuery("SELECT payload FROM xa_messages ORDER BY id")) {
            List<String> messages = new ArrayList<>();
            while (rs.next()) {
                messages.add(rs.getString("payload"));
            }
            return messages;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void dbInsert(String payload) {
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
