package redisdatastructures.stream;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RedisStream {

    private static ConcurrentMap<String, Stream> redisStream = new ConcurrentHashMap<>();

    public static Stream getStream(String key){
        return redisStream.get(key);
    }

    public static Stream createStream(String key){
        redisStream.put(key, new Stream());
        return redisStream.get(key);
    }


    public static boolean containsKey(String key){
        return redisStream.containsKey(key);
    }

}
