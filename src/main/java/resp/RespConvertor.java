package resp;

import java.util.List;

public class RespConvertor {

    public static String toBulkString(String input){
        if (input == null) {
            return "$-1\r\n";  // Null bulk string
        }
        return "$" + input.length() + "\r\n" + input + "\r\n";
    }

    public static String toRESPArray(List<String> input, boolean bulkConversionRequired){
        int len = input.size();
        StringBuilder output = new StringBuilder(String.format("*%d\r\n", len));
        for(String in : input){
            if(bulkConversionRequired){
                in = toBulkString(in);
            }
            output.append(in);
        }
        return output.toString();
    }

    public static String toIntegerString(int value){
        return ":"+value+"\r\n";
    }

    public static String toErrorString(String error){
        return "-ERR " + error + "\r\n";
    }
}
