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
            return RespConvertor.toBulkString(entryIdLong + "-" + newSequence);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private static boolean validateStream(Stream stream, String entryId, String entrySequence) {
        if (entryId.equals("*") || entrySequence.equals("*")) {
            return true;
        }
        Long lastEntries = stream.getLastId();
        if (lastEntries > Long.parseLong(entryId)) {
            return false;
        } else if (lastEntries == Long.parseLong(entryId) && stream.getEntries(lastEntries).getLastSequence() >= Long.parseLong(entrySequence)) {
            return false;
        }
        return true;
    }

    private static boolean validateEntryKey(String entryId, String entrySequence) {
        if (entryId.equals("0") && entrySequence.equals("0")) {
            return false;
        }
        return true;
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
            String[] start = ((String) list.get(2)).split("-");
            String[] end = ((String) list.get(3)).split("-");
            long startId = Long.parseLong(start[0]);
            long endId = Long.parseLong(end[0]);

            Stream stream = RedisStream.getStream(streamKey);

            ConcurrentSkipListMap<Long, Entries> streamRange = new ConcurrentSkipListMap<>();

            Entries startEntries = stream.getEntries(startId);
            long startSequence;
            if (start.length == 2) {
                startSequence = Long.parseLong(start[1]);
            } else {
                startSequence = 0;
            }
            Entries endEntries = stream.getEntries(endId);
            long endSequence;
            if (end.length == 2) {
                endSequence = Long.parseLong(end[1]);
            } else {
                endSequence = endEntries.getLastSequence();
            }

            if (startId == endId) {
                streamRange.put(startId, startEntries.getRange(startSequence, endSequence));
            } else {
                long higherIndexInStream = stream.getHigherId(startId);
                long lowerIndexInStream = stream.getLowerId(endId);
                if (lowerIndexInStream >= higherIndexInStream) {
                    streamRange.putAll(stream.getRange(higherIndexInStream, lowerIndexInStream));
                }

                streamRange.put(startId, startEntries.getTailRange(startSequence));
                streamRange.put(endId, endEntries.getHeadRange(endSequence));
            }

            return prepareXRangeResponse(streamRange);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private static String prepareXRangeResponse(ConcurrentSkipListMap<Long, Entries> streamRange) {
        List<String> responseList = new ArrayList<>();

        for(Map.Entry<Long, Entries> entries : streamRange.entrySet()){
            Long entryId = entries.getKey();
            Entries entry = entries.getValue();
            for(Map.Entry<Long, Map<String, String>> sequenceEntry : entry.entries.entrySet()){
                String entryMapResponse = prepareEntryMapResponse(sequenceEntry.getValue());
                String key = entryId + "-" + sequenceEntry.getKey();
                responseList.add(RespConvertor.toRESPArray(List.of(RespConvertor.toBulkString(key), entryMapResponse), false));
            }
        }
        return RespConvertor.toRESPArray(responseList, false);
    }

    private static String prepareEntryMapResponse(Map<String, String> entry){
        List<String> entryMapResponseList = new ArrayList<>();

        for(Map.Entry<String, String> en : entry.entrySet()){
            entryMapResponseList.add(en.getKey());
            entryMapResponseList.add(en.getValue());
        }
        return RespConvertor.toRESPArray(entryMapResponseList, true);
    }
}
