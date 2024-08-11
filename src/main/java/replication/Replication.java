package replication;

import config.ReplicationConfig;
import resp.RespConvertor;
import tcpclient.TCPClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

public class Replication {
    public static void handshake(int port) {
        try {
            Socket socket = new Socket(ReplicationConfig.getMasterIP(), Integer.parseInt(ReplicationConfig.getMasterPort()));
            socket.setSoTimeout(500);
            StringBuilder response = new StringBuilder();
            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                String ping = RespConvertor.toRESPArray(List.of("PING"), true);
                out.print(ping);
                out.flush();
                // Read the server's response
                String line = in.readLine();
                System.out.println(line);

                String replConf1 = RespConvertor.toRESPArray(List.of("REPLCONF", "listening-port", String.valueOf(port)), true);
                out.print(replConf1);
                out.flush();
                // Read the server's response
                line = in.readLine();
                System.out.println(line);

                String replConf2 = RespConvertor.toRESPArray(List.of("REPLCONF", "capa", "psync2"), true);
                out.print(replConf2);
                out.flush();
                // Read the server's response
                line = in.readLine();
                System.out.println(line);

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            socket.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
