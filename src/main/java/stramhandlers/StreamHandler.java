package stramhandlers;

import redisdatastructures.RedisKeys;
import redisdatastructures.stream.Entries;
import redisdatastructures.stream.RedisStream;
import redisdatastructures.stream.Stream;
import resp.RespConvertor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamHandler {

    public static String handleXAdd(List<Object> list) {
        try {
            String streamKey = (String) list.get(1);
            String entryKey = (String) list.get(2);
            if(entryKey == "*"){
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

            Stream stream = RedisStream.getStream(streamKey);
            Entries entries;
            if(entryKey.equals("*")){
                entries = stream.addEntries();
            }else{
                entries = stream.getEntries(Long.parseLong(entryId));
            }

            Map<String, String> entry = prepareMap(list.subList(3, list.size()));
            long newSequence = 0;
            if(entrySequence.equals("*")){
                newSequence = entries.addEntry(entry, Long.parseLong(entryId));
            }else{
                newSequence = entries.addEntry(Long.parseLong(entrySequence), entry);
            }
            RedisKeys.addKey(streamKey, "stream");
            return  RespConvertor.toBulkString(entryId + "-" + newSequence);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private static boolean validateStream(Stream stream, String entryId, String entrySequence) {
        if(entryId.equals("*")){
            return true;
        }
        Long lastEntries = stream.getLastId();
        if(lastEntries > Long.parseLong(entryId)){
            return false;
        }else if(lastEntries == Long.parseLong(entryId) && stream.getEntries(lastEntries).getLastSequence() >=  Long.parseLong(entrySequence)){
            return false;
        }

        return true;
    }

    private static boolean validateEntryKey(String entryId, String entrySequence) {
        if(entryId.equals("0") && entrySequence.equals("0")){
            return false;
        }
        return true;
    }

    private static Map<String, String> prepareMap(List<Object> list) {
        Map<String, String> pairMap = new HashMap<>();
        for(int i = 0, j = i + 1; i < list.size() - 1; i = i + 2){
            pairMap.put((String)list.get(i), (String)list.get(j));
        }
        return pairMap;
    }
}
