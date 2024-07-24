package resp;

public class RespConvertor {
    public static String toBulkString(String input){
        if (input == null) {
            return "$-1\r\n";  // Null bulk string
        }
        return "$" + input.length() + "\r\n" + input + "\r\n";
    }
}
