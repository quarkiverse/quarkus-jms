package io.quarkiverse.it.jms.activemq;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.quarkiverse.jms.JmsClient;
import io.quarkiverse.jms.JmsQueue;
import io.smallrye.common.annotation.Identifier;

@Path("/jms")
@Produces(MediaType.TEXT_PLAIN)
public class JmsResource {

    @Inject
    JmsClient jmsClient;

    @Inject
    @Identifier("xa-commit")
    JmsQueue<String> xaCommitQueue;

    @Inject
    @Identifier("xa-rollback")
    JmsQueue<String> xaRollbackQueue;

    @Inject
    JmsListenerApp listenerApp;

    @Inject
    DataSource dataSource;

    @POST
    @Path("/send/{queue}")
    public void send(@PathParam("queue") String queue, String message) {
        jmsClient.queue(queue).send(message);
    }

    @GET
    @Path("/receive/{queue}")
    public String receive(@PathParam("queue") String queue) {
        return jmsClient.queue(queue)
                .receive(String.class)
                .orElse(null);
    }

    @GET
    @Path("/listener/received")
    public String listenerReceived() {
        return String.join("\n", listenerApp.getReceived());
    }

    @GET
    @Path("/listener/xa-received")
    public String listenerXaReceived() {
        return String.join("\n", listenerApp.getXaReceived());
    }

    @POST
    @Path("/xa/send-and-insert")
    @Transactional
    public void xaSendAndInsert(String message) {
        xaCommitQueue.send(message);
        insertXaMessage("client-xa:" + message);
    }

    @POST
    @Path("/xa/send-and-rollback")
    @Transactional
    public void xaSendAndRollback(String message) {
        xaRollbackQueue.send(message);
        insertXaMessage("should-not-persist-client:" + message);
        throw new RuntimeException("Intentional failure to trigger XA rollback");
    }

    @GET
    @Path("/xa/messages")
    public String xaMessages() {
        try (var connection = dataSource.getConnection();
                var stmt = connection.createStatement();
                var rs = stmt.executeQuery("SELECT payload FROM xa_messages ORDER BY id")) {
            List<String> messages = new ArrayList<>();
            while (rs.next()) {
                messages.add(rs.getString("payload"));
            }
            return String.join("\n", messages);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void insertXaMessage(String payload) {
        try (var connection = dataSource.getConnection();
                var stmt = connection.prepareStatement("INSERT INTO xa_messages (payload) VALUES (?)")) {
            stmt.setString(1, payload);
            stmt.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
