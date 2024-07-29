package test.rdbtest;

import config.RDBConfig;
import rdb.RDBCreator;
import rdb.RDBParser;
import redisdatastructures.RedisMap;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RDBTest {
    public static void testCreation() throws IOException {
        String filePath = "/Users/anubhavgarikapadu/projects/test.rdb";
        File rdbFile = new File(filePath);
        if(!rdbFile.exists()){
            rdbFile.createNewFile();
        }
        FileOutputStream fos = new FileOutputStream(filePath);
        DataOutputStream out = new DataOutputStream(fos);

        ConcurrentHashMap<String, RedisMap.Value> mockMap = new ConcurrentHashMap<>();

        mockMap.put("Key1", new RedisMap.Value("Valuebgbd1", false, 0));
        mockMap.put("Key2ergegwevewg", new RedisMap.Value("Valdewhgwhgwegdbue2", true, 14121314));
        mockMap.put("Key3ewger", new RedisMap.Value("Vadsfbdslue3", true, 23423));
        mockMap.put("4", new RedisMap.Value("Vadflue4", false, 0));

        RDBCreator rdbCreator = new RDBCreator(out, mockMap);
        rdbCreator.writeRDB();
    }

    public static void testRDBParser() throws IOException{

    }

    public static void main(String[] args) throws IOException{
        testCreation();
        testRDBParser();
    }
}
