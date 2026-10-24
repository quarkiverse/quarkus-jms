package io.quarkiverse.it.jms.nativepojo;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkiverse.jms.JmsListener;

@ApplicationScoped
public class OrderListener {

    private final List<Order> received = new CopyOnWriteArrayList<>();

    @JmsListener("pojo-orders")
    public void onOrder(Order order) {
        received.add(order);
    }

    public List<Order> getReceived() {
        return received;
    }
}
