package replication;

import resp.RespConvertor;
import tcpclient.TCPClient;

import java.util.List;

public class Replication {
    public static void handshake(int port) {
        try {
            TCPClient client = new TCPClient();
            String ping = RespConvertor.toRESPArray(List.of("PING"), true);
            client.sendMessage(ping);

            String replConf1 = RespConvertor.toRESPArray(List.of("REPLCONF", "listening-port", String.valueOf(port)), true);
            client.sendMessage(replConf1);

            String replConf2 = RespConvertor.toRESPArray(List.of("REPLCONF", "capa", "psync2"), true);
            client.sendMessage(replConf2);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
