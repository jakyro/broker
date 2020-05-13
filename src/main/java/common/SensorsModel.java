package common;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SensorsModel {
    @JsonProperty("wind")
    public double wind;
    @JsonProperty("pressure")
    public double pressure;
    @JsonProperty("temp")
    public double temp;
    @JsonProperty("humidity")
    public double humidity;
    @JsonProperty("light")
    public double light;
    @JsonProperty("timestamp")
    public long timestamp;
}
