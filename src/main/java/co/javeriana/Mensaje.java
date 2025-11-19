package co.javeriana;

public class Mensaje {
    private final String topico;
    private final String payload;
    private final long timestamp;

    public Mensaje(String topico, String payload, long timestamp) {
        this.topico = topico;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public String getTopico() { return topico; }
    public String getPayload() { return payload; }
    public long getTimestamp() { return timestamp; }
}
