import brocker.Broker;
import common.BrokerPackage;

import java.io.IOException;
import java.util.Objects;

public class Main {
    public static void main(String[] args) throws IOException {
        Broker broker = new Broker(3000);
        while (true) {
            BrokerPackage p = broker.receive();
            if (Objects.nonNull(p)) {
                broker.broadcast(p.getTopic(), p.getData());
            }
        }
    }
}
