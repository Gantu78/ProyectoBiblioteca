package co.javeriana;

import java.util.HashMap;
import java.util.Map;

public class Utils {
    /** Parse key=value pairs from a string with ';' separator, starting after the first token. */
    public static Map<String,String> parseKeyValues(String carga) {
        Map<String,String> map = new HashMap<>();
        if (carga == null) return map;
        String[] parts = carga.split(";");
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i];
            int eq = part.indexOf('=');
            if (eq > 0) {
                String k = part.substring(0, eq).trim();
                String v = part.substring(eq+1).trim();
                map.put(k, v);
            }
        }
        return map;
    }

    public static Long extractTs(String carga) {
        Map<String,String> kv = parseKeyValues(carga);
        String ts = kv.get("ts");
        if (ts == null) return null;
        try {
            return Long.parseLong(ts);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
