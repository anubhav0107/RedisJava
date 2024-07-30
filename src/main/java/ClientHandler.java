import config.RDBConfig;
import rdb.RDBParser;
import redisdatastructures.RedisMap;
import resp.RespConvertor;
import resp.RespParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class ClientHandler implements Runnable {

    Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        BufferedReader in = null;
        PrintWriter out = null;
        try {
            System.out.println("Inside Handler\n");
            in = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
            out = new PrintWriter(this.clientSocket.getOutputStream(), true);
            RespParser rp = new RespParser(in);
            while(!this.clientSocket.isClosed()) {
                Object object = rp.parse();
                String output = handleParsedRESPObject(object);
                out.write(output);
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private String handleParsedRESPObject(Object object) {
        if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            String command = (String) list.get(0);
            switch (command.toUpperCase()) {
                case "ECHO":
                    return handleEcho(list);
                case "SET":
                    return handleSet(list);
                case "GET":
                    return handleGet(list);
                case "CONFIG":
                    return handleConfig(list);
                case "KEYS":
                    return handleKeys(list);
                case "INCR":
                    return handleIncrement(list);
                default:
                    return "+PONG\r\n";
            }
        }
        return null;
    }

    private String handleIncrement(List<Object> list) {
        try{
            String key = (String) list.get(1);
            RedisMap.Value value = RedisMap.getValue(key);
            int intVal = 0;
            boolean canExpire = false;
            long expiry = 0;
            if(value != null){
                String val = value.value();
                try{
                    intVal = Integer.parseInt(val);
                    canExpire = value.canExpire();
                    expiry = value.expiry();
                }catch(Exception e){
                    return RespConvertor.toErrorString("value is not an integer or out of range");
                }
            }

            intVal++;
            RedisMap.Value newValue = new RedisMap.Value(String.valueOf(intVal), canExpire, expiry);
            RedisMap.setValue(key, newValue);
            return RespConvertor.toIntegerString(intVal);
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleKeys(List<Object> list) {
        try{
            String pattern = (String) list.get(1);
            String bulkArrayResponse = "";
            if(pattern.equalsIgnoreCase("*")){
                bulkArrayResponse = RespConvertor.toRESPArray(RedisMap.getKeys());
            }
            return bulkArrayResponse;
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleConfig(List<Object> list) {
        try {
            if (RDBConfig.isRDBEnabled) {
                String configParam = (String) list.get(2);
                List<String> input;
                if (configParam.equalsIgnoreCase("dir")) {
                    input = List.of(configParam, RDBConfig.getInstance().getDir());
                } else {
                    input = List.of(configParam, RDBConfig.getInstance().getDbFileName());
                }
                return RespConvertor.toRESPArray(input);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleGet(List<Object> list) {
        try {
            String key = (String) list.get(1);
            RedisMap.Value value = RedisMap.getValue(key);
            long now = (Timestamp.valueOf(LocalDateTime.now()).getTime());
            if (value == null || (value.canExpire() && now >= value.expiry())) {
                return RespConvertor.toBulkString(null);
            }
            return RespConvertor.toBulkString(value.value());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleEcho(List<Object> list) {
        if (list.size() > 1) {
            String text = (String) list.get(1);
            return RespConvertor.toBulkString(text);
        }
        return "";
    }

    private String handleSet(List<Object> list) throws IllegalArgumentException {
        try {
            if (list.size() > 2) {
                String key = (String) list.get(1);
                String val = (String) list.get(2);
                long expiry = 0;
                boolean canExpire = false;
                if (list.size() > 4) {
                    canExpire = true;
                    expiry = Long.parseLong((String) list.get(4));
                }
                RedisMap.Value value = new RedisMap.Value(val, canExpire, (Timestamp.valueOf(LocalDateTime.now()).getTime()) + expiry);
                RedisMap.setValue(key, value);
                return "+OK\r\n";
            } else {
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
}
