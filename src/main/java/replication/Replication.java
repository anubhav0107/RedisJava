package replication;

import config.ReplicationConfig;
import handler.ClientHandler;
import resp.RespConvertor;
import resp.RespParser;
import stramhandlers.StreamHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

public class Replication {
    public static void handshake(int port) {
        try {
            Socket socket = new Socket(ReplicationConfig.getMasterIP(), Integer.parseInt(ReplicationConfig.getMasterPort()));
            StringBuilder response = new StringBuilder();
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String ping = RespConvertor.toRESPArray(List.of("PING"), true);
            out.print(ping);
            out.flush();
            // Read the server's response
            String line = in.readLine();

            String replConf1 = RespConvertor.toRESPArray(List.of("REPLCONF", "listening-port", String.valueOf(port)), true);
            out.print(replConf1);
            out.flush();
            line = in.readLine();

            String replConf2 = RespConvertor.toRESPArray(List.of("REPLCONF", "capa", "psync2"), true);
            out.print(replConf2);
            out.flush();
            line = in.readLine();

            String pSync = RespConvertor.toRESPArray(List.of("PSYNC", "?", "-1"), true);
            out.print(pSync);
            out.flush();
            line = in.readLine();
            System.out.println(line);
            parseRDBHandshake(in);

            new Thread(() -> listenToSocket(socket, in, out)).start();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void parseRDBHandshake(BufferedReader in) {
        try{
            RespParser rp = new RespParser(in);
            int firstByte = in.read();
            if(firstByte == '$'){
                String len = rp.parseInteger();
                System.out.println(len);
                char[] rdb = new char[Integer.parseInt(len) - 1];
                in.read(rdb);
                System.out.println(rdb);
            }

        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    private static void listenToSocket(Socket socket, BufferedReader in, PrintWriter out) {
        try {
            RespParser rp = new RespParser(in);
            while (!socket.isClosed()) {
                Object object = rp.parse();
                if (object == null) {
                    continue;
                }
                String output = handleParsedRESPObject(object);
                if (output != null) {
                    out.write(output);
                    out.flush();
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String handleParsedRESPObject(Object object) {
        if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            String command = (String) list.get(0);
            System.out.println("Command: " + command);
            switch (command.toUpperCase()) {
                case "SET":
                    ClientHandler.handleSet(list);
                    break;
                case "INCR":
                    ClientHandler.handleIncrement(list);
                    break;
                case "XADD":
                    StreamHandler.handleXAdd(list);
                    break;
                case "REPLCONF":
                    return handleReplConfAck(list);
                default:
                    System.out.println("+PONG\r\n");
            }
        }
        return null;
    }

    private static String handleReplConfAck(List<Object> list) {
        try {
            return RespConvertor.toRESPArray(List.of("REPLCONF", "ACK", "0"), true);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
}
