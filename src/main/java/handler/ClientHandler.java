package handler;

import config.RDBConfig;
import config.ReplicationConfig;
import redisdatastructures.RedisKeys;
import redisdatastructures.RedisMap;
import resp.RespConvertor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClientHandler {

    private static CountDownLatch latch;
    private static int numReplicasResponded = -1;

    public static void pSyncHandler(List<Object> list, PrintWriter out, Socket clientSocket) {
        try {
            StringBuilder response = new StringBuilder("+FULLRESYNC ").append(ReplicationConfig.getMasterReplicationId()).append(" ").append(ReplicationConfig.getMasterOffset()).append("\r\n");
            out.write(response.toString());
            out.flush();
            transferRDB(out, clientSocket);
            ReplicationConfig.addSlaveConnection(clientSocket);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void sendGetAckToReplica(Socket clientSocket, PrintWriter out) {

        try {
            String getAckCommand = RespConvertor.toRESPArray(List.of("REPLCONF", "GETACK", "*"), true);
            out.write(getAckCommand);
            out.flush();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void transferRDB(PrintWriter out, Socket clientSocket) {
        try {
            String emptyRDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
            byte[] contents = Base64.getDecoder().decode(emptyRDB);
            StringBuilder output = new StringBuilder("$").append(contents.length).append("\r\n");
            out.write(output.toString());
            out.flush();
            clientSocket.getOutputStream().write(contents);
            clientSocket.getOutputStream().flush();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static String handleReplConf(List<Object> list, Socket socket) {
        try {
            String command = (String) list.get(1);
            System.out.println(command);
            if (command.equalsIgnoreCase("listening-port")) {
                String slavePort = (String) list.get(2);
                ReplicationConfig.addSlavePort(Integer.parseInt(slavePort));
            } else if (command.equalsIgnoreCase("capa")) {
                for (int i = 2; i < list.size(); i++) {
                    ReplicationConfig.addCapabilitiesToSlave((String) list.get(i));
                }
            } else if(command.equalsIgnoreCase("ACK")){
                int totalBytesProcessed = Integer.parseInt((String) list.get(2));
                ReplicationConfig.updateSlaveConnection(socket, totalBytesProcessed);
                latch.countDown();
                numReplicasResponded++;
                return null;
            }
            return "+OK\r\n";
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static String handleInfo(List<Object> list) {
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

    public static String handleType(List<Object> list) {
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

    public static String handleIncrement(List<Object> list) {
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

    public static String handleKeys(List<Object> list) {
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

    public static String handleConfig(List<Object> list) {
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

    public static String handleGet(List<Object> list) {
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

    public static String handleEcho(List<Object> list) {
        if (list.size() > 1) {
            String text = (String) list.get(1);
            return RespConvertor.toBulkString(text);
        }
        return "";
    }

    public static String handleSet(List<Object> list) throws IllegalArgumentException {
        try {
            if (list.size() > 2) {
                String key = (String) list.get(1);
                String val = (String) list.get(2);
                System.out.println("key: " + key);
                System.out.println("val: " + val);
                long expiry = 0;
                boolean canExpire = false;
                if (list.size() > 4) {
                    canExpire = true;
                    expiry = Long.parseLong((String) list.get(4));
                }
                RedisMap.Value value = new RedisMap.Value(val, canExpire, (Timestamp.valueOf(LocalDateTime.now()).getTime()) + expiry);
                RedisMap.setValue(key, value);
                RedisKeys.addKey(key, "string");
                propagateToSlave(list);
                return "+OK\r\n";
            } else {
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static void propagateToSlave(List<Object> command) {
        List<String> commandString = command.stream()
                .map(Object::toString)
                .collect(Collectors.toList());
        Map<Socket, Integer> slaveSockets = ReplicationConfig.getSlaveConnections();
        numReplicasResponded = 0;
        for (Socket slaveSocket : slaveSockets.keySet()) {
            try {
                PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(slaveSocket.getInputStream()));
                String respCommand = RespConvertor.toRESPArray(commandString, true);
                out.write(respCommand);
                out.flush();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static String handleWait(List<Object> list) {
        try {
            int waitTime = Integer.parseInt((String) list.get(2));
            int countDown = Integer.parseInt((String) list.get(1));
            Map<Socket, Integer> slaveSockets = ReplicationConfig.getSlaveConnections();
            for (Socket slaveSocket : slaveSockets.keySet()) {
                try {
                    PrintWriter out = new PrintWriter(slaveSocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(slaveSocket.getInputStream()));

                    String getAckCommand = RespConvertor.toRESPArray(List.of("REPLCONF", "GETACK", "*"), true);
                    out.write(getAckCommand);
                    out.flush();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
            latch = new CountDownLatch(countDown);
            latch.await(waitTime, TimeUnit.MILLISECONDS);

            numReplicasResponded = numReplicasResponded == -1 ? ReplicationConfig.countReplicas() : numReplicasResponded;
            return RespConvertor.toIntegerString(numReplicasResponded);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
}
