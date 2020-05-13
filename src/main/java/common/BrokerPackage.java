package common;

public class BrokerPackage extends SerializablePacket {
    public enum Verb {
        SUBSCRIBE,
        UNSUBSCRIBE,
        POST
    }

    String topic;
    String data;
    Verb verb;

    public BrokerPackage(String topic, String data, Verb verb) {
        this.topic = topic;
        this.data = data;
        this.verb = verb;
    }

    public String getTopic() {
        return topic;
    }

    public String getData() {
        return data;
    }

    public Verb getVerb() {
        return verb;
    }
}
