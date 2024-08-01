package redisdatastructures;

import java.util.HashMap;
import java.util.Map;

public class RedisKeys {
    private static Map<String, String> keys = new HashMap<>();

    public static void addKey(String key, String type){
        keys.put(key, type);
    }

    public static String get(String key){
        return keys.get(key);
    }

    public static boolean containsKey(String key){
        return keys.containsKey(key);
    }
}
