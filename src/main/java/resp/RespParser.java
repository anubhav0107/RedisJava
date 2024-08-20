package resp;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RespParser {
    BufferedReader in;

    public RespParser(BufferedReader in) {
        this.in = in;
    }

    public Object parse() throws IOException {
        int firstByte = in.read();
        switch (firstByte) {
            case -1:
                return null;
            case '+':
                return parseSimpleString();
            case '-':
                return parseError();
            case ':':
                return parseInteger();
            case '$':
                return parseBulkString();
            case '*':
                return parseArray();
            default:
                in.readLine();
                List<Object> list = new ArrayList<>();
                list.add("PING");
                return list;
        }
    }

    public List<Object> parseArray() throws IOException {
        long length = Long.parseLong(parseInteger());
        if (length == -1) {
            return null;
        }
        List<Object> array = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            array.add(parse());
        }
        return array;
    }

    public String parseBulkString() throws IOException {
        long length = Long.parseLong(parseInteger());
        if (length == -1) {
            return null;
        }
        char[] bytes = new char[(int) length];
        in.read(bytes);
        in.read();
        in.read();
        return new String(bytes);
    }

    public String parseInteger() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1 && c != '\r') {
            sb.append((char) c);
        }
        in.read();
        return sb.toString();
    }

    public String parseError() throws IOException {
        return parseSimpleString();
    }

    public String parseSimpleString() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1 && c != '\r') {
            sb.append((char) c);
        }
        in.read();
        return sb.toString();
    }
}
