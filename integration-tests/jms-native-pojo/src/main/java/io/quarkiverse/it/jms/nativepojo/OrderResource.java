package io.quarkiverse.it.jms.nativepojo;

import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.quarkiverse.jms.JmsClient;

@Path("/orders")
public class OrderResource {

    @Inject
    JmsClient jmsClient;

    @Inject
    OrderListener listener;

    @POST
    @Path("/send")
    public void send(String body) {
        String[] parts = body.split(",", 3);
        Order order = new Order(parts[0], parts[1], Integer.parseInt(parts[2]));
        jmsClient.queue("pojo-orders").send(order);
    }

    @GET
    @Path("/received")
    @Produces(MediaType.TEXT_PLAIN)
    public String received() {
        return listener.getReceived().stream()
                .map(o -> o.getId() + "," + o.getProduct() + "," + o.getQuantity())
                .collect(Collectors.joining("\n"));
    }
}
