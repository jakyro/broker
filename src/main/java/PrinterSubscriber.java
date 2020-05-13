import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import subscriber.Subscriber;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.function.Consumer;

public class PrinterSubscriber {
    public static void main(String[] args) throws SocketException {
        new PrinterSubscriber().run();
    }

    public void run() throws SocketException {
        InetSocketAddress addr = new InetSocketAddress("localhost", 3000);
        Subscriber subscriber = new Subscriber(addr);
        subscriber.subscribe("data_stream", new MyConsumer());
        subscriber.loop();
    }

    static class MyConsumer implements Consumer<String> {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        @Override
        public void accept(String s) {
            JsonElement je = JsonParser.parseString(s);
            System.out.println(gson.toJson(je));
        }
    }
}
