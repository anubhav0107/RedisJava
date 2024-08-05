package stramhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class StreamThreadHandler {

    public static final ConcurrentHashMap<String, List<CountDownLatch>> streamLatches = new ConcurrentHashMap<>();

    public static void registerStreamLatch(String streamKey, CountDownLatch latch){
        streamLatches.computeIfAbsent(streamKey, k -> new ArrayList<>()).add(latch);
    }
}
