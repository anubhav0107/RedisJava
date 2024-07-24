import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable{

    Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }


    @Override
    public void run() {
        BufferedReader in = null;
        PrintWriter out = null;
        try{
            in = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
            out = new PrintWriter(this.clientSocket.getOutputStream(), true);
            String nextLine = "";
            while((nextLine = in.readLine()) != null){
                if(nextLine.equalsIgnoreCase("PING")){
                    out.print("+PONG\r\n");
                    out.flush();
                }
            }
        }catch(IOException e){
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
