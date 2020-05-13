import subscriber.Subscriber;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;

public class MqttSubscriber {
    private static Socket socket;

    public static void main(String[] args) throws Exception {
        socket = connect();
        new MqttSubscriber().run();
    }

    private static Socket connect() throws IOException, InterruptedException {
        byte[] data = connectPacket("my-client-id");
        Socket socket = new Socket("broker.hivemq.com", 1883);
        OutputStream output = socket.getOutputStream();
        output.write(data);
        InputStream input = socket.getInputStream();
        Thread.sleep(500);
        int len = input.available();
        byte[] resp = new byte[len];
        input.read(resp);
        System.out.println(Arrays.toString(resp));
        return socket;
    }

    private static byte[] connectPacket(String clientId) throws IOException {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        byte b = 1 << 4;
        data.write(b);
        byte[] payload = connectPackagePayload(clientId);
        // payload length
        writePayloadLength(data, payload.length);
        data.write(payload);
        return data.toByteArray();
    }

    private static byte[] connectPackagePayload(String clientId) throws IOException {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        String protocol = "MQTT";
        int keepAlive = 60;
        // protocol length
        writeAsTwo(data, protocol.length());
        data.write(protocol.getBytes());
        // version
        data.write(4);
        // flags
        data.write(2);
        // keep alive
        writeAsTwo(data, keepAlive);
        // client id length
        writeAsTwo(data, clientId.length());
        // client id
        data.write(clientId.getBytes());
        return data.toByteArray();
    }

    private static byte[] publishPackage(String topic, String message) throws IOException {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        data.write(0x30);
        byte[] payload = publishPackagePayload(topic, message);
        // payload length
        writePayloadLength(data, payload.length);
        data.write(payload);
        return data.toByteArray();
    }

    private static void writePayloadLength(ByteArrayOutputStream data, int length) {
        while (length > 0) {
            int e = length % 128;
            length /= 128;
            if (length > 0) {
                e |= 128;
            }
            data.write(e);
        }
    }

    private static byte[] publishPackagePayload(String topic, String message) throws IOException {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        writeAsTwo(data, topic.length());
        data.write(topic.getBytes());
        data.write(message.getBytes());
        return data.toByteArray();
    }

    private static void writeAsTwo(OutputStream stream, int value) throws IOException {
        stream.write(value >> 4);
        stream.write(value & 0xFF);
    }

    public void run() throws SocketException {
        InetSocketAddress addr = new InetSocketAddress("localhost", 3000);
        Subscriber subscriber = new Subscriber(addr);
        subscriber.subscribe("data_stream", this::mqttSend);
        subscriber.loop();
    }

    private void mqttSend(String data) {
        try {
            byte[] m1 = publishPackage("halivali", data);
            socket.getOutputStream().write(m1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
