import handler.ClientHandler;
import resp.RespConvertor;
import resp.RespParser;
import stramhandlers.StreamHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class ClientThread implements Runnable {

    final Socket clientSocket;
    boolean isMulti;

    Queue<Object> multiQueue;

    public ClientThread(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.isMulti = false;
    }

    @Override
    public void run() {
        BufferedReader in = null;
        PrintWriter out = null;
        try {
            System.out.println("Inside Handler\n");
            in = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
            out = new PrintWriter(this.clientSocket.getOutputStream(), true);
            RespParser rp = new RespParser(in);
            while (!this.clientSocket.isClosed()) {
                Object object = rp.parse();
                if (object == null) {
                    continue;
                }
                String output = handleParsedRESPObject(object, out);
                if (output != null) {
                    out.write(output);
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (in != null) in.close();
                if (out != null) out.close();
            } catch (IOException e) {
                System.out.println("IOException during cleanup: " + e.getMessage());
            }
        }
    }

    private String handleParsedRESPObject(Object object, PrintWriter out) {
        if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            String command = (String) list.get(0);
            if (this.isMulti && (!command.equalsIgnoreCase("EXEC") && !command.equalsIgnoreCase("DISCARD"))) {
                this.multiQueue.offer(object);
                return "+QUEUED\r\n";
            }
            switch (command.toUpperCase()) {
                case "ECHO":
                    return ClientHandler.handleEcho(list);
                case "SET":
                    return ClientHandler.handleSet(list);
                case "GET":
                    return ClientHandler.handleGet(list);
                case "CONFIG":
                    return ClientHandler.handleConfig(list);
                case "KEYS":
                    return ClientHandler.handleKeys(list);
                case "INCR":
                    return ClientHandler.handleIncrement(list);
                case "MULTI":
                    return handleMulti(list);
                case "EXEC":
                    return handleExec(list, out);
                case "DISCARD":
                    return handleDiscard(list);
                case "TYPE":
                    return ClientHandler.handleType(list);
                case "XADD":
                    return StreamHandler.handleXAdd(list);
                case "XRANGE":
                    return StreamHandler.handleXRange(list);
                case "XREAD":
                    return StreamHandler.handleXRead(list);
                case "INFO":
                    return ClientHandler.handleInfo(list);
                case "REPLCONF":
                    return ClientHandler.handleReplConf(list, this.clientSocket);
                case "PSYNC":
                    ClientHandler.pSyncHandler(list, out, this.clientSocket);
                    break;
                case "WAIT":
                    return ClientHandler.handleWait(list);
                default:
                    return "+PONG\r\n";
            }
        }
        return null;
    }

    private String handleExec(List<Object> list, PrintWriter out) {
        try {
            if (!this.isMulti) {
                return RespConvertor.toErrorString("EXEC without MULTI");
            }
            this.isMulti = false;
            List<String> responseList = new ArrayList<>();
            while (!this.multiQueue.isEmpty()) {
                Object object = this.multiQueue.poll();
                responseList.add(handleParsedRESPObject(object, out));
            }
            return RespConvertor.toRESPArray(responseList, false);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleMulti(List<Object> list) {
        try {
            this.isMulti = true;
            this.multiQueue = new ArrayDeque<>();
            return "+OK\r\n";
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private String handleDiscard(List<Object> list) {
        try {
            if (!this.isMulti) {
                return RespConvertor.toErrorString("DISCARD without MULTI");
            }
            this.isMulti = false;
            this.multiQueue = null;
            return "+OK\r\n";
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
}
