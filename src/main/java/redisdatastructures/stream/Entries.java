package redisdatastructures.stream;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

public class Entries {
    private static ConcurrentSkipListMap<Long, Map<String, String>> entries;

    public Entries(){
        entries = new ConcurrentSkipListMap<>();
    }

    public Long getLastSequence(){
        try {
            return entries.lastKey();
        }catch (NoSuchElementException e){
            return -1L;
        }
    }

    public Long getFirstSequence(){
        try {
            return entries.firstKey();
        }catch (NoSuchElementException e){
            return -1L;
        }
    }

    public long addEntry(Map<String, String> entry, Long entryId){
        long newSequence;
        if(entryId == 0 && getLastSequence() == -1){
            newSequence = 1;
        }else {
            newSequence = getLastSequence() + 1;
        }
        entries.put(newSequence, entry);
        return newSequence;
    }

    public long addEntry(Long key, Map<String, String> entry){
        entries.put(key, entry);
        return key;
    }

    public boolean containsSequence(Long key){
        return entries.containsKey(key);
    }


}
