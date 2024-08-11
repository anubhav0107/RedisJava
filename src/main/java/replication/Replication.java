package replication;

import resp.RespConvertor;
import tcpclient.TCPClient;

import java.util.List;

public class Replication {
    public static void handshake(int port) {
        try {
            TCPClient client = new TCPClient();
            String ping = RespConvertor.toRESPArray(List.of("PING"), true);
            String response = client.sendMessage(ping);
            System.out.println(response);

            String replConf1 = RespConvertor.toRESPArray(List.of("REPLCONF", "listening-port", String.valueOf(port)), true);
            response = client.sendMessage(replConf1);
            System.out.println(response);

            String replConf2 = RespConvertor.toRESPArray(List.of("REPLCONF", "capa", "psync2"), true);
            response = client.sendMessage(replConf2);
            System.out.println(response);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
