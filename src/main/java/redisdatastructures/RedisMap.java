package redisdatastructures;

import java.sql.Timestamp;
import java.util.concurrent.ConcurrentHashMap;

public class RedisMap {
    public record Value(String value, Timestamp ts, boolean canExpire, long expiry){ }
    private static ConcurrentHashMap<String, Value> redisMap = new ConcurrentHashMap<>();

    public static Value getValue(String key){
        return RedisMap.redisMap.get(key);
    }

    public static void setValue(String key, Value value){
        RedisMap.redisMap.put(key, value);
    }
}
