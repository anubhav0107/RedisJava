package stramhandlers;

import redisdatastructures.RedisKeys;
import redisdatastructures.stream.Entries;
import redisdatastructures.stream.RedisStream;
import redisdatastructures.stream.Stream;
import resp.RespConvertor;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StreamHandler {

    public static String handleXAdd(List<Object> list) {
        try {
            String streamKey = (String) list.get(1);
            String entryKey = (String) list.get(2);

            if (entryKey.equals("*")) {
                entryKey = "*-*";
            }
            String entryId = entryKey.split("-")[0];
            String entrySequence = entryKey.split("-")[1];

            if (!validateEntryKey(entryId, entrySequence)) {
                return RespConvertor.toErrorString("The ID specified in XADD must be greater than 0-0");
            }

            if (RedisStream.containsKey(streamKey) && !validateStream(RedisStream.getStream(streamKey), entryId, entrySequence)) {
                return RespConvertor.toErrorString("The ID specified in XADD is equal or smaller than the target stream top item");
            }

            Stream stream;
            if (!RedisStream.containsKey(streamKey)) {
                stream = RedisStream.createStream(streamKey);
            } else {
                stream = RedisStream.getStream(streamKey);
            }

            Entries entries;
            long entryIdLong;
            if (entryId.equals("*")) {
                entryIdLong = Timestamp.valueOf(LocalDateTime.now()).getTime();
            } else {
                entryIdLong = Long.parseLong(entryId);
            }

            entries = stream.getEntries(entryIdLong);

            Map<String, String> entry = prepareMap(list.subList(3, list.size()));

            long newSequence = 0;
            if (entrySequence.equals("*")) {
                newSequence = entries.addEntry(entry, entryIdLong);
            } else {
                newSequence = entries.addEntry(Long.parseLong(entrySequence), entry);
            }
            RedisKeys.addKey(streamKey, "stream");

            synchronized (StreamThreadHandler.streamLatches) {
                List<CountDownLatch> latches = StreamThreadHandler.streamLatches.get(streamKey);
                if (latches != null) {
                    for (CountDownLatch latch : latches) {
                        latch.countDown();
                    }
                    StreamThreadHandler.streamLatches.remove(streamKey);
                }
            }

            return RespConvertor.toBulkString(entryIdLong + "-" + newSequence);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return RespConvertor.toBulkString(null);
    }

    private static boolean validateStream(Stream stream, String entryId, String entrySequence) {
        if (entryId.equals("*") || entrySequence.equals("*")) {
            return true;
        }
        Long lastEntries = stream.getLastId();
        if (lastEntries > Long.parseLong(entryId)) {
            return false;
        } else
            return lastEntries != Long.parseLong(entryId) || stream.getEntries(lastEntries).getLastSequence() < Long.parseLong(entrySequence);
    }

    private static boolean validateEntryKey(String entryId, String entrySequence) {
        return !entryId.equals("0") || !entrySequence.equals("0");
    }

    private static Map<String, String> prepareMap(List<Object> list) {
        Map<String, String> pairMap = Collections.synchronizedMap(new LinkedHashMap<>());
        for (int i = 0, j = i + 1; i < list.size() - 1; i = i + 2) {
            pairMap.put((String) list.get(i), (String) list.get(j));
        }
        return pairMap;
    }

    public static String handleXRange(List<Object> list) {
        try {
            String streamKey = (String) list.get(1);
            Stream stream = RedisStream.getStream(streamKey);

            String start = (String) list.get(2);
            if (start.equals("-")) {
                start = String.valueOf(stream.getFirstId());
            }
            String end = (String) list.get(3);
            if (end.equals("+")) {
                end = String.valueOf(stream.getLastId());
            }

            String[] startArr = start.split("-");
            String[] endArr = end.split("-");

            long startId = Long.parseLong(startArr[0]);
            long endId = Long.parseLong(endArr[0]);

            ConcurrentSkipListMap<Long, Entries> streamRange = new ConcurrentSkipListMap<>();

            Entries startEntries = stream.getEntries(startId);
            long startSequence;
            if (startArr.length == 2) {
                startSequence = Long.parseLong(startArr[1]);
            } else {
                startSequence = 0;
            }
            Entries endEntries = stream.getEntries(endId);
            long endSequence;
            if (endArr.length == 2) {
                endSequence = Long.parseLong(endArr[1]);
            } else {
                endSequence = endEntries.getLastSequence();
            }

            if (startId == endId) {
                streamRange.put(startId, startEntries.getRange(startSequence, endSequence));
            } else {
                long higherIndexInStream = stream.getHigherId(startId);
                long lowerIndexInStream = stream.getLowerId(endId);

                streamRange.put(startId, startEntries.getTailRange(startSequence, true));
                if (lowerIndexInStream >= higherIndexInStream) {
                    streamRange.putAll(stream.getRange(startId, true, lowerIndexInStream, true));
                }
                streamRange.put(endId, endEntries.getHeadRange(endSequence));
            }

            return prepareXRangeResponse(streamRange);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return RespConvertor.toBulkString(null);
    }

    private static String prepareXRangeResponse(ConcurrentSkipListMap<Long, Entries> streamRange) {
        List<String> responseList = new ArrayList<>();

        for (Map.Entry<Long, Entries> entries : streamRange.entrySet()) {
            Long entryId = entries.getKey();
            Entries entry = entries.getValue();
            for (Map.Entry<Long, Map<String, String>> sequenceEntry : entry.entries.entrySet()) {
                String entryMapResponse = prepareEntryMapResponse(sequenceEntry.getValue());
                String key = entryId + "-" + sequenceEntry.getKey();
                responseList.add(RespConvertor.toRESPArray(List.of(RespConvertor.toBulkString(key), entryMapResponse), false));
            }
        }
        return RespConvertor.toRESPArray(responseList, false);
    }

    private static String prepareEntryMapResponse(Map<String, String> entry) {
        List<String> entryMapResponseList = new ArrayList<>();

        for (Map.Entry<String, String> en : entry.entrySet()) {
            entryMapResponseList.add(en.getKey());
            entryMapResponseList.add(en.getValue());
        }
        return RespConvertor.toRESPArray(entryMapResponseList, true);
    }

    public static String handleXRead(List<Object> list) {
        try {
            int i = 0;
            String command;
            boolean blocking = false;
            long blockTimeout = 0;
            while (i < list.size() && !(command = (String) list.get(i)).equalsIgnoreCase("streams")) {
                if (command.equalsIgnoreCase("block")) {
                    blocking = true;
                    blockTimeout = Long.parseLong((String) list.get(++i));
                }
                i++;
            }


            Map<String, String> streamMap = getStreamMap(list, i + 1);
            List<String> xReadResponseList = new ArrayList<>();

            boolean hasData = false;
            long startTime = System.currentTimeMillis();

            CountDownLatch latch = new CountDownLatch(1);
            for (String streamKey : streamMap.keySet()) {
                StreamThreadHandler.registerStreamLatch(streamKey, latch);
            }

            if (blocking && (System.currentTimeMillis() - startTime < blockTimeout)) {
                // Wait for new data with timeout
                latch.await(Math.max(1, blockTimeout - (System.currentTimeMillis() - startTime)), TimeUnit.MILLISECONDS);
            }
            
            for (Map.Entry<String, String> entry : streamMap.entrySet()) {
                String response = getStreamReadResponse(entry.getKey(), entry.getValue());

                if (response != null && !response.isEmpty() && response.charAt(1) != '0') {
                    xReadResponseList.add(RespConvertor.toRESPArray(List.of(RespConvertor.toBulkString(entry.getKey()), response), false));
                    hasData = true;
                }
            }
            if (!xReadResponseList.isEmpty()) {
                return RespConvertor.toRESPArray(xReadResponseList, false);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return RespConvertor.toBulkString(null);
    }

    private static String getStreamReadResponse(String streamKey, String startEntriesKey) {
        String[] startEntriesArr = startEntriesKey.split("-");
        Stream stream = RedisStream.getStream(streamKey);
        Entries startEntries = stream.getEntries(Long.parseLong(startEntriesArr[0]));

        ConcurrentSkipListMap<Long, Entries> streamReadMap = new ConcurrentSkipListMap<>();
        streamReadMap.put(Long.parseLong(startEntriesArr[0]), startEntries.getTailRange(Long.parseLong(startEntriesArr[1]), false));

        streamReadMap.putAll(stream.getRange(Long.parseLong(startEntriesArr[0]), false));

        return streamReadMap.size() > 0 ? prepareXRangeResponse(streamReadMap) : null;
    }

    private static Map<String, String> getStreamMap(List<Object> list, int startIndex) {
        int keyStartIdx = startIndex;
        int idStartIdx = (list.size() + startIndex) / 2;
        Map<String, String> streamMap = new LinkedHashMap<>();
        for (int i = keyStartIdx, j = idStartIdx; j < list.size(); i++, j++) {
            String startEntries = (String) list.get(j);
            if (!startEntries.contains("-")) {
                startEntries += "-0";
            }
            streamMap.put((String) list.get(i), startEntries);
        }
        return streamMap;
    }
}
