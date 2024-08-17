import config.RDBConfig;
import config.ReplicationConfig;
import redisdatastructures.RedisKeys;
import redisdatastructures.RedisMap;
import resp.RespConvertor;
import resp.RespParser;
import stramhandlers.StreamHandler;

import javax.swing.text.StringContent;
import java.io.*;
import java.net.Socket;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

public class ClientHandler implements Runnable {

    final Socket clientSocket;
    boolean isMulti;

    Queue<Object> multiQueue;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.isMulti = false;
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
            while (!this.clientSocket.isClosed()) {
                Object object = rp.parse();
                if (object == null) {
                    continue;
                }
                String output = handleParsedRESPObject(object, out);
                if (output != null) {
                    out.write(output);
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (in != null) in.close();
                if (out != null) out.close();
            } catch (IOException e) {
                System.out.println("IOException during cleanup: " + e.getMessage());
            }
        }
    }

    private String handleParsedRESPObject(Object object, PrintWriter out) {
        if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            String command = (String) list.get(0);
            if (this.isMulti && (!command.equalsIgnoreCase("EXEC") && !command.equalsIgnoreCase("DISCARD"))) {
                this.multiQueue.offer(object);
                return "+QUEUED\r\n";
            }
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
                case "MULTI":
                    return handleMulti(list);
                case "EXEC":
                    return handleExec(list, out);
                case "DISCARD":
                    return handleDiscard(list);
                case "TYPE":
                    return handleType(list);
                case "XADD":
                    return StreamHandler.handleXAdd(list);
                case "XRANGE":
                    return StreamHandler.handleXRange(list);
                case "XREAD":
                    return StreamHandler.handleXRead(list);
                case "INFO":
                    return handleInfo(list);
                case "REPLCONF":
                    return handleReplConf(list);
                case "PSYNC":
                    pSyncHandler(list, out);
                    break;
                default:
                    return "+PONG\r\n";
            }
        }
        return null;
    }

    private void pSyncHandler(List<Object> list, PrintWriter out) {
        try {
            StringBuilder response = new StringBuilder("+FULLRESYNC ").append(ReplicationConfig.getMasterReplicationId()).append(" ").append(ReplicationConfig.getMasterOffset()).append("\r\n");
            out.write(response.toString());
            out.flush();
            transferRDB(out);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void transferRDB(PrintWriter out) {
        try {
            String emptyRDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
            byte[] contents = Base64.getDecoder().decode(emptyRDB);
            StringBuilder output = new StringBuilder("$").append(contents.length).append("\r\n");
            out.write(output.toString());
            out.flush();
            this.clientSocket.getOutputStream().write(contents);
            this.clientSocket.getOutputStream().flush();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private String handleReplConf(List<Object> list) {
        try {
            String command = (String) list.get(1);

            if (command.equalsIgnoreCase("listening-port")) {
                String slavePort = (String) list.get(2);
                ReplicationConfig.addSlavePort(Integer.parseInt(slavePort));
            } else if (command.equalsIgnoreCase("capa")) {
                for (int i = 2; i < list.size(); i++) {
                    ReplicationConfig.addCapabilitiesToSlave((String) list.get(i));
                }
            }
            return "+OK\r\n";
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleInfo(List<Object> list) {
        try {
            String command = (String) list.get(1);
            if (command.equalsIgnoreCase("replication")) {
                String role = ReplicationConfig.isSlave() ? "slave" : "master";
                StringBuilder infoResponse = new StringBuilder("role:").append(role);
                if (!ReplicationConfig.isSlave()) {
                    infoResponse.append("\nmaster_replid:").append(ReplicationConfig.getMasterReplicationId())
                            .append("\nmaster_repl_offset:").append(ReplicationConfig.getMasterOffset());
                }
                return RespConvertor.toBulkString(infoResponse.toString());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleType(List<Object> list) {
        try {
            String key = (String) list.get(1);
            if (RedisKeys.containsKey(key)) {
                return "+" + RedisKeys.get(key) + "\r\n";
            }
            return "+none\r\n";
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleDiscard(List<Object> list) {
        try {
            if (!this.isMulti) {
                return RespConvertor.toErrorString("DISCARD without MULTI");
            }
            this.isMulti = false;
            this.multiQueue = null;
            return "+OK\r\n";
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleExec(List<Object> list, PrintWriter out) {
        try {
            if (!this.isMulti) {
                return RespConvertor.toErrorString("EXEC without MULTI");
            }
            this.isMulti = false;
            List<String> responseList = new ArrayList<>();
            while (!this.multiQueue.isEmpty()) {
                Object object = this.multiQueue.poll();
                responseList.add(handleParsedRESPObject(object, out));
            }
            return RespConvertor.toRESPArray(responseList, false);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleMulti(List<Object> list) {
        try {
            this.isMulti = true;
            multiQueue = new ArrayDeque<>();
            return "+OK\r\n";
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleIncrement(List<Object> list) {
        try {
            String key = (String) list.get(1);
            RedisMap.Value value = RedisMap.getValue(key);
            int intVal = 0;
            boolean canExpire = false;
            long expiry = 0;
            if (value != null) {
                String val = value.value();
                try {
                    intVal = Integer.parseInt(val);
                    canExpire = value.canExpire();
                    expiry = value.expiry();
                } catch (Exception e) {
                    return RespConvertor.toErrorString("value is not an integer or out of range");
                }
            }
            intVal++;
            RedisMap.Value newValue = new RedisMap.Value(String.valueOf(intVal), canExpire, expiry);
            RedisMap.setValue(key, newValue);
            return RespConvertor.toIntegerString(intVal);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleKeys(List<Object> list) {
        try {
            String pattern = (String) list.get(1);
            String bulkArrayResponse = "";
            if (pattern.equalsIgnoreCase("*")) {
                bulkArrayResponse = RespConvertor.toRESPArray(RedisMap.getKeys(), true);
            }
            return bulkArrayResponse;
        } catch (Exception e) {
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
                return RespConvertor.toRESPArray(input, true);
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
                RedisKeys.addKey(key, "string");
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
