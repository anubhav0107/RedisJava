package redisdatastructures.stream;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

public class Entries {
    public ConcurrentSkipListMap<Long, Map<String, String>> entries;

    public Entries(){
        this.entries = new ConcurrentSkipListMap<>();
    }

    public Entries(ConcurrentSkipListMap<Long, Map<String, String>> newEntries){
        this.entries = newEntries;
    }

    public Long getLastSequence(){
        try {
            return this.entries.lastKey();
        }catch (NoSuchElementException e){
            return -1L;
        }
    }

    public Long getFirstSequence(){
        try {
            return this.entries.firstKey();
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
        this.entries.put(newSequence, entry);
        return newSequence;
    }

    public long addEntry(Long key, Map<String, String> entry){
        this.entries.put(key, entry);
        return key;
    }

    public boolean containsSequence(Long key){
        return this.entries.containsKey(key);
    }

    public Entries getRange(Long start, Long end){
        return new Entries(new ConcurrentSkipListMap<>(this.entries.subMap(start, true, end, true)));
    }

    public Entries getTailRange(Long start){
        return new Entries(new ConcurrentSkipListMap<>(this.entries.tailMap(start, true)));
    }

    public Entries getHeadRange(Long end){
        return new Entries(new ConcurrentSkipListMap<>(this.entries.headMap(end, true)));
    }
}
