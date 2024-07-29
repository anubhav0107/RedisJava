package redisdatastructures;

import config.RDBConfig;
import rdb.RDBCreator;
import rdb.RDBParser;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RedisMap {
    public record Value(String value, boolean canExpire, long expiry){
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Value{");
            sb.append("value='").append(value).append('\'');
            sb.append(", canExpire=").append(canExpire);
            sb.append(", expiry=").append(expiry);
            sb.append("}\n");
            return sb.toString();
        }
    }
    private static ConcurrentHashMap<String, Value> redisMap = new ConcurrentHashMap<>();

    public static Value getValue(String key){
        return RedisMap.redisMap.get(key);
    }

    public static void setValue(String key, Value value){
        RedisMap.redisMap.put(key, value);
    }

    public static List<String> getKeys(){
        return new ArrayList<>(RedisMap.redisMap.keySet());
    }
    public static void saveToRDB(){
        try {
            String rdbFilePath = RDBConfig.getInstance().getFullPath();
            File rdbFile = new File(rdbFilePath);
            if(!rdbFile.exists()){
                rdbFile.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(rdbFilePath);
            DataOutputStream dos = new DataOutputStream(fos);

            RDBCreator rdbCreator = new RDBCreator(dos, RedisMap.redisMap);
            rdbCreator.writeRDB();

            dos.close();
        }catch(IOException e){
            System.out.println(e.getMessage());
        }
    }
}
