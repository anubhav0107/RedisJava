package test.respparsertest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import resp.RespConvertor;
import resp.RespParser;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class RespParserTest {

    @Test
    public static void testResp() throws IOException {
        String respData = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        BufferedReader inputStream = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(respData.getBytes())));
        RespParser parser = new RespParser(inputStream);
        Object result = parser.parse();
        System.out.println(result);
    }

    @Test
    public static void testRespConvertor(){
        String input = "Hey";
        String output = RespConvertor.toBulkString(input);

        Assertions.assertEquals(output, "$3\r\nHey\r\n");
    }

    public static void main(String[] args) throws IOException {
        testResp();
    }
}
