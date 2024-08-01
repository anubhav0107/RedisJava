package redisdatastructures.stream;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentSkipListMap;

public class Stream {
    private static ConcurrentSkipListMap<Long, Entries> stream;

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
}
