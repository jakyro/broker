package publisher;

import common.BrokerPackage;

import java.io.IOException;
import java.net.*;

public class Publisher {
    private DatagramSocket ds;
    private InetAddress ip;

    public Publisher() {
        try {
            ds = new DatagramSocket();
            ip = InetAddress.getByName("127.0.0.1");
        } catch (SocketException | UnknownHostException ignored) {
        }
    }

    public void publish(String topic, String data) {
        BrokerPackage p = new BrokerPackage(topic, data, BrokerPackage.Verb.POST);
        try {
            byte[] bytes = p.getBytes();
            DatagramPacket dp = new DatagramPacket(bytes, bytes.length, ip, 3000);
            ds.send(dp);
        } catch (IOException ignored) {
        }
    }
}
