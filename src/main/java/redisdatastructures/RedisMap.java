package redisdatastructures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RedisMap {
    public record Value(String value, boolean canExpire, long expiry){
        @Override
        public String toString() {
            return "Value{" + "value='" + value + '\'' +
                    ", canExpire=" + canExpire +
                    ", expiry=" + expiry +
                    "}\n";
        }
    }
    private static ConcurrentHashMap<String, Value> redisMap = new ConcurrentHashMap<>();

    public static Value getValue(String key){
        return RedisMap.redisMap.get(key);
    }

    public static void setValue(String key, Value value){
        RedisMap.redisMap.put(key, value);
    }

    public static List<String> getKeys(){
        return new ArrayList<>(RedisMap.redisMap.keySet());
    }
    public static boolean containsKey(String key){
        return RedisMap.redisMap.containsKey(key);
    }
}
