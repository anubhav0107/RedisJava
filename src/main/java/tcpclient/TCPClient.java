package tcpclient;

import config.ReplicationConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TCPClient {

    private String serverIP;

    private int serverPort;

    public TCPClient(){
        this.serverIP = ReplicationConfig.getMasterIP();
        this.serverPort = Integer.parseInt(ReplicationConfig.getMasterPort());
    }


    public String sendMessage(String message){
        try (Socket socket = new Socket(this.serverIP, this.serverPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))){

            out.print(message);
            out.flush();
            StringBuilder response = new StringBuilder();
            int c;
            while((c = in.read()) != -1){
                response.append((char)c);
            }

            return response.toString();

        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        return null;
    }
}
