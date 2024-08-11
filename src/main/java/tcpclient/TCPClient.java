package tcpclient;

import config.ReplicationConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TCPClient {

    private String serverIP;

    private int serverPort;

    public TCPClient() {
        this.serverIP = ReplicationConfig.getMasterIP();
        this.serverPort = Integer.parseInt(ReplicationConfig.getMasterPort());
    }


    public String sendMessage(String message) {
        try (Socket socket = new Socket(this.serverIP, this.serverPort)) {
            socket.setSoTimeout(500);
            StringBuilder response = new StringBuilder();
            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                // Send the message to the server
                out.print(message);
                out.flush();
                // Read the server's response
                String line = in.readLine();
                socket.close();
                return line + "\n";

            } catch (Exception e) {
                return response.toString().trim();
            }

        } catch (Exception e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }
}
