package redisdatastructures.stream;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentSkipListMap;

public class Stream {
    public ConcurrentSkipListMap<Long, Entries> stream;

    public Stream(){
        stream = new ConcurrentSkipListMap<>();
    }

    public Entries addEntries(){
        long key = Timestamp.valueOf(LocalDateTime.now()).getTime();
        return getEntries(key);
    }

    public void addEntries(Long key, Entries entries){
        stream.put(key, entries);
    }

    public Entries getEntries(Long key){
        if(!stream.containsKey(key)){
            stream.put(key, new Entries());
        }
        return stream.get(key);
    }


    public Long getLastId(){
        return stream.lastKey();
    }

    public Long getFirstId(){
        return stream.firstKey();
    }

    public boolean containsEntry(Long id, Long sequence){
        return stream.containsKey(id) && stream.get(id).containsSequence(sequence);
    }

    public ConcurrentSkipListMap<Long, Entries> getRange(Long start, boolean startInclusive, Long end, boolean endInclusive){
        return new ConcurrentSkipListMap<>(stream.subMap(start, startInclusive, end, endInclusive));
    }

    public ConcurrentSkipListMap<Long, Entries> getRange(Long start, boolean startInclusive){
        return new ConcurrentSkipListMap<>(stream.subMap(start, startInclusive, stream.lastKey(), true));
    }

    public Long getHigherId(Long start){
        return stream.higherKey(start);
    }

    public Long getLowerId(Long end){
        return stream.lowerKey(end);
    }
}
