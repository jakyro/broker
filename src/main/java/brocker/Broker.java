package brocker;

import common.BrokerPackage;

import java.io.IOException;
import java.net.*;
import java.util.*;

public class Broker {
    private final DatagramSocket ds;
    private final Map<String, List<InetSocketAddress>> subscribers = new HashMap<>();

    public Broker(int port) throws SocketException {
        ds = new DatagramSocket(port);
    }

    public BrokerPackage receive() {
        try {
            DatagramPacket dp = receiveDatagram();
            InetSocketAddress addr = (InetSocketAddress) dp.getSocketAddress();
            BrokerPackage p = BrokerPackage.ofBytes(dp.getData());
            switch (p.getVerb()) {
                case SUBSCRIBE:
                    subscribe(addr, p.getTopic());
                    break;
                case UNSUBSCRIBE:
                    unsubscribe(addr, p.getTopic());
                    break;
                default:
                    return p;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void unsubscribe(InetSocketAddress addr, String topic) {
        System.out.println("Unsubscribe");
        List<InetSocketAddress> list = subscribers.get(topic);
        if (Objects.nonNull(list)) {
            list.remove(addr);
        }
    }

    private DatagramPacket receiveDatagram() throws IOException {
        byte[] buf = new byte[1024];
        DatagramPacket dp = new DatagramPacket(buf, 1024);
        ds.receive(dp);
        return dp;
    }

    private void subscribe(InetSocketAddress subscriber, String topic) {
        System.out.println("Subscribe");
        System.out.println(subscriber);
        System.out.println(topic);
        List<InetSocketAddress> list = subscribers.get(topic);
        if (Objects.isNull(list)) {
            list = new ArrayList<>();
            list.add(subscriber);
            subscribers.put(topic, list);
        } else {
            list.add(subscriber);
        }
    }

    public void broadcast(String topic, String message) {
        List<InetSocketAddress> list = subscribers.get(topic);
        if (Objects.nonNull(list)) {
            System.out.println(list.size());
            list.forEach(inetAddress -> sendTo(inetAddress, topic, message));
        } else {
            System.out.println(String.format("No subscribers for topic `%s`", topic));
        }
    }

    private void sendTo(InetSocketAddress address, String topic, String message) {
        BrokerPackage p = new BrokerPackage(topic, message, BrokerPackage.Verb.POST);
        try {
            byte[] bytes = p.getBytes();
            DatagramPacket dp = new DatagramPacket(bytes, bytes.length, address);
            ds.send(dp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
