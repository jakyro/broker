import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import common.SensorsModel;
import subscriber.Subscriber;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.time.temporal.ChronoUnit.MILLIS;

public class ForecastSubscriber {
    public static void main(String[] args) throws SocketException {
        new ForecastSubscriber().run();
    }

    public void run() throws SocketException {
        InetSocketAddress addr = new InetSocketAddress("localhost", 3000);
        Subscriber subscriber = new Subscriber(addr);
        subscriber.subscribe("data_stream", new MyConsumer());
        subscriber.loop();
    }

    class MyConsumer implements Consumer<String> {
        ObjectMapper mapper = new ObjectMapper();

        @Override
        public void accept(String s) {
            try {
                SensorsModel model = mapper.readValue(s, SensorsModel.class);
                compute(model);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private final List<Double> wind = new ArrayList<>();
    private final List<Double> pressure = new ArrayList<>();
    private final List<Double> temp = new ArrayList<>();
    private final List<Double> humidity = new ArrayList<>();
    private final List<Double> light = new ArrayList<>();
    LocalDateTime start = LocalDateTime.now();

    private void compute(SensorsModel model) {
        if (isInterval()) {
            forecast();
            reset();
        }
        handlePacket(model);
    }

    private boolean isInterval() {
        return start.until(LocalDateTime.now(), MILLIS) >= 1000;
    }

    private void forecast() {
        SensorsModel model = new SensorsModel();
        model.wind = wind.stream().mapToDouble(value -> value).average().orElse(0);
        model.pressure = pressure.stream().mapToDouble(value -> value).average().orElse(0);
        model.temp = temp.stream().mapToDouble(value -> value).average().orElse(0);
        model.humidity = humidity.stream().mapToDouble(value -> value).average().orElse(0);
        model.light = light.stream().mapToDouble(value -> value).average().orElse(0);
        String status = getForecastByEvent(model);
        System.out.println(status);
    }

    private void reset() {
        wind.clear();
        pressure.clear();
        temp.clear();
        humidity.clear();
        light.clear();
        start = LocalDateTime.now();
    }

    private void handlePacket(SensorsModel model) {
        pushIfNonZero(wind, model.wind);
        pushIfNonZero(pressure, model.pressure);
        pushIfNonZero(temp, model.temp);
        pushIfNonZero(humidity, model.humidity);
        pushIfNonZero(light, model.light);
    }

    private void pushIfNonZero(List<Double> list, double val) {
        if (val == 0) {
            list.add(val);
        }
    }

    public static String getForecastByEvent(SensorsModel event) {
        if (event.temp < -2 && event.light < 128 && event.pressure < 720) {
            return "SNOW";
        }
        if (event.temp < -2 && event.light > 128 && event.pressure < 680) {
            return "WET_SNOW";
        }
        if (event.temp < -8) {
            return "SNOW";
        }
        if (event.temp < -15 && event.wind > 45) {
            return "BLIZZARD";
        }
        if (event.temp > 0 && event.pressure < 710 && event.humidity > 70 && event.wind < 20) {
            return "SLIGHT_RAIN";
        }
        if (event.temp > 0 && event.pressure < 690 && event.humidity > 70 && event.wind < 20) {
            return "HEAVY_RAIN";
        }
        if (event.temp > 30 && event.pressure < 770 && event.humidity > 80 && event.light > 192) {
            return "HOT";
        }
        if (event.temp > 30 && event.pressure < 770 && event.humidity > 50 && event.light > 192) {
            return "CONVECTION_OVEN";
        }
        if (event.temp > 25 && event.pressure < 750 && event.humidity > 70 && event.light > 192 && event.wind < 10) {
            return "WARM";
        }
        if (event.temp > 25 && event.pressure < 750 && event.humidity > 70 && event.light > 192 && event.wind > 10) {
            return "SLIGHT_BREEZE";
        }
        if (event.light < 128) {
            return "CLOUDY";
        }
        if (event.temp > 30 && event.pressure < 660 && event.humidity > 85 && event.wind > 45) {
            return "MONSOON";
        }
        return "JUST_A_NORMAL_DAY";
    }
}
