package replication;

import resp.RespConvertor;
import tcpclient.TCPClient;

import java.util.List;

public class Replication {
    public static void handshake() {
        try {
            String ping = RespConvertor.toRESPArray(List.of("PING"), true);
            TCPClient client = new TCPClient();
            client.sendMessage(ping);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
