package io.quarkiverse.it.jms.xa;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.quarkiverse.jms.JmsClient;

@Path("/xa-client")
@Produces(MediaType.APPLICATION_JSON)
public class JmsClientXaResource {

    @Inject
    JmsClient jmsClient;

    @Inject
    DataSource dataSource;

    /**
     * Sends a JMS message and writes to DB in the same XA transaction.
     * Both commit or both rollback.
     */
    @POST
    @Path("/send-and-insert")
    @Transactional
    public void sendAndInsert(String message) {
        jmsClient.queue("xa-client-queue").send(message);
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "sent:" + message);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Receives a JMS message and writes to DB in the same XA transaction.
     * Both commit or both rollback.
     */
    @POST
    @Path("/receive-and-insert")
    @Transactional
    public String receiveAndInsert() {
        Optional<String> msg = jmsClient.queue("xa-client-queue")
                .receiveTimeout(Duration.ofSeconds(5)).receive(String.class);
        if (msg.isEmpty()) {
            return null;
        }
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "received:" + msg.get());
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return msg.get();
    }

    /**
     * Receives a JMS message and writes to DB, then throws an exception
     * to trigger XA rollback. The message should be redelivered and
     * the DB insert should be rolled back.
     */
    @POST
    @Path("/receive-and-rollback")
    @Transactional
    public void receiveAndRollback() {
        Optional<String> msg = jmsClient.queue("xa-rollback-queue")
                .receiveTimeout(Duration.ofSeconds(5)).receive(String.class);
        if (msg.isEmpty()) {
            return;
        }
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "should-not-persist:" + msg.get());
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Intentional failure to trigger XA rollback");
    }

    /**
     * Sends a JMS message and writes to DB, then throws an exception
     * to trigger XA rollback. The message should not be sent and
     * the DB insert should be rolled back.
     */
    @POST
    @Path("/send-and-rollback")
    @Transactional
    public void sendAndRollback(String message) {
        jmsClient.queue("xa-client-queue").send(message);
        try (var conn = dataSource.getConnection();
                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, "should-not-persist-send:" + message);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Intentional failure to trigger XA rollback");
    }

    /**
     * Receives from the rollback queue without failing — used to verify
     * the message was redelivered after rollback.
     */
    @POST
    @Path("/receive-after-rollback")
    public String receiveAfterRollback() {
        Optional<String> msg = jmsClient.queue("xa-rollback-queue")
                .receiveTimeout(Duration.ofSeconds(5)).receive(String.class);
        return msg.orElse(null);
    }

    @POST
    @Path("/receive-batch-and-insert")
    @Transactional
    public int receiveBatchAndInsert() {
        return jmsClient.queue("xa-batch-queue", String.class)
                .receiveTimeout(Duration.ofSeconds(2))
                .receiveWith(receiver -> {
                    int count = 0;
                    Optional<String> msg;
                    while ((msg = receiver.receive()).isPresent()) {
                        try (var conn = dataSource.getConnection();
                                var stmt = conn.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
                            stmt.setString(1, "batch:" + msg.get());
                            stmt.executeUpdate();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        count++;
                    }
                    return count;
                });
    }

    @POST
    @Path("/send-pojo/{queue}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void sendPojo(@PathParam("queue") String queue, OrderDto order) {
        jmsClient.queue(queue).send(order);
    }

    @GET
    @Path("/receive-pojo/{queue}")
    public OrderDto receivePojo(@PathParam("queue") String queue) {
        return jmsClient.queue(queue, OrderDto.class)
                .receive()
                .orElse(null);
    }

    public record OrderDto(String id, String product, int quantity) {
    }

    @GET
    @Path("/db-messages")
    public List<String> dbMessages() {
        try (var conn = dataSource.getConnection();
                var stmt = conn.createStatement();
                var rs = stmt.executeQuery("SELECT payload FROM xa_messages ORDER BY id")) {
            List<String> messages = new java.util.ArrayList<>();
            while (rs.next()) {
                messages.add(rs.getString("payload"));
            }
            return messages;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
