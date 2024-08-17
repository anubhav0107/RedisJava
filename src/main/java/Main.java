import config.RDBConfig;
import config.ReplicationConfig;
import replication.Replication;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");

        //  Uncomment this block to pass the first stage
        String rdbDir = "";
        String rdbFileName = "";
        int port = 6379;
        for (int i = 0, j = 1; j < args.length; i = i + 2, j = j + 2) {
            if (args[i].equalsIgnoreCase("--dir")) {
                rdbDir = args[j];
            } else if (args[i].equalsIgnoreCase("--dbfilename")) {
                rdbFileName = args[j];
            } else if (args[i].equalsIgnoreCase("--port")) {
                port = Integer.parseInt(args[j]);
            } else if (args[i].equalsIgnoreCase("--replicaof")) {
                ReplicationConfig.setIsSlave(true);
                String[] masterServerDtls = args[j].split(" ");
                ReplicationConfig.setMasterIP(masterServerDtls[0]);
                ReplicationConfig.setMasterPort(masterServerDtls[1]);
            }
        }
        ReplicationConfig.initializeReplicationId();

        if (ReplicationConfig.isSlave()) {
            Replication.handshake(port);
        }

        RDBConfig.initializeInstance(rdbDir, rdbFileName);
        ServerSocket serverSocket = null;
        Socket clientSocket = null;

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(200);
            serverSocket = new ServerSocket(port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            while (true) {
                clientSocket = serverSocket.accept();
                ClientHandler clientHandler = new ClientHandler(clientSocket);
                executorService.execute(clientHandler);
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

}
