import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import common.SensorsModel;
import publisher.Publisher;
import subscriber.Subscriber;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JoinSubscriber {
    Publisher publisher;

    private final long PERIOD = 100;
    private final Map<Long, List<SensorsModel>> intervals = new ConcurrentHashMap<>();

    public static void main(String[] args) throws SocketException {
        new JoinSubscriber().run();
    }

    public void run() throws SocketException {
        InetSocketAddress addr = new InetSocketAddress("localhost", 3000);
        Subscriber subscriber = new Subscriber(addr);
        publisher = new Publisher();
        subscriber.subscribe("iot_stream", this::parse);
        subscriber.subscribe("sensors_stream", this::parse);
        subscriber.subscribe("legacy_stream", this::parse);
        subscriber.loop();
    }

    ObjectMapper mapper = new ObjectMapper();

    public void parse(String s) {
        try {
            SensorsModel model = mapper.readValue(s, SensorsModel.class);
            List<SensorsModel> list = getInterval(model.timestamp);
            list.add(model);
            cleanupAndPublish(model.timestamp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void cleanupAndPublish(long timestamp) {
        long interval = timestamp / PERIOD;
        Set<Long> keys = intervals.keySet();
        for (Long key : keys) {
            if (key < interval - 2) {
                computeAndPublish(intervals.get(key));
                intervals.remove(key);
            }
        }
    }

    private void computeAndPublish(List<SensorsModel> sensorsModels) {
        SensorsModel model = new SensorsModel();
        model.wind = sensorsModels.stream().mapToDouble(e -> e.wind).average().orElse(0);
        model.pressure = sensorsModels.stream().mapToDouble(e -> e.pressure).average().orElse(0);
        model.temp = sensorsModels.stream().mapToDouble(e -> e.temp).average().orElse(0);
        model.humidity = sensorsModels.stream().mapToDouble(e -> e.humidity).average().orElse(0);
        model.light = sensorsModels.stream().mapToDouble(e -> e.light).average().orElse(0);
        model.timestamp = sensorsModels.get(0).timestamp;
        try {
            System.out.println("Publish");
            System.out.println(intervals.size());
            publisher.publish("data_stream", mapper.writeValueAsString(model));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private List<SensorsModel> getInterval(long timestamp) {
        long interval = timestamp / PERIOD;
        List<SensorsModel> list = intervals.get(interval);
        if (Objects.isNull(list)) {
            list = new ArrayList<>();
            intervals.put(interval, list);
        }
        return list;
    }
}
