package subscriber;

import common.BrokerPackage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class Subscriber {
    private final InetSocketAddress address;
    private final DatagramSocket ds;
    private final Map<String, List<Consumer<String>>> subscriptions = new HashMap<>();

    public Subscriber(InetSocketAddress addr) throws SocketException {
        ds = new DatagramSocket();
        address = addr;
    }

    public void subscribe(String topic, Consumer<String> callback) {
        List<Consumer<String>> list = subscriptions.get(topic);
        if (Objects.isNull(list)) {
            list = new CopyOnWriteArrayList<>();
            list.add(callback);
            subscriptions.put(topic, list);
            serverSubscribe(topic);
        } else {
            list.add(callback);
        }
    }

    public void unsubscribe(String topic, Consumer<String> callback) {
        List<Consumer<String>> list = subscriptions.get(topic);
        int size = 0;
        if (Objects.nonNull(list)) {
            list.remove(callback);
            size = list.size();
        }
        if (size == 0) {
            serverUnsubscribe(topic);
        }
    }

    private void serverSubscribe(String topic) {
        BrokerPackage p = new BrokerPackage(topic, null, BrokerPackage.Verb.SUBSCRIBE);
        send(p);
    }

    private void serverUnsubscribe(String topic) {
        BrokerPackage p = new BrokerPackage(topic, null, BrokerPackage.Verb.UNSUBSCRIBE);
        send(p);
    }

    private void send(BrokerPackage p) {
        try {
            byte[] bytes = p.getBytes();
            DatagramPacket dp = new DatagramPacket(bytes, bytes.length, address);
            ds.send(dp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loop() {
        while (true) {
            BrokerPackage p = receive();
            parsePackage(p);
        }
    }

    private void parsePackage(BrokerPackage p) {
        List<Consumer<String>> list = subscriptions.get(p.getTopic());
        if (Objects.nonNull(list)) {
            list.forEach(stringConsumer -> stringConsumer.accept(p.getData()));
        }
    }

    public BrokerPackage receive() {
        try {
            DatagramPacket dp = receiveDatagram();
            InetSocketAddress addr = (InetSocketAddress) dp.getSocketAddress();
            BrokerPackage p = BrokerPackage.ofBytes(dp.getData());
            if (p.getVerb() == BrokerPackage.Verb.POST) {
                return p;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private DatagramPacket receiveDatagram() throws IOException {
        byte[] buf = new byte[1024];
        DatagramPacket dp = new DatagramPacket(buf, 1024);
        ds.receive(dp);
        return dp;
    }
}
