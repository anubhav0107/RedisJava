package rdb;

import config.RDBConfig;
import redisdatastructures.RedisMap;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RDBParser {
    static DataInputStream inStream;

    public static void parseRDB() {
        try {
            inStream = new DataInputStream(new FileInputStream(RDBConfig.INSTANCE.getFullPath()));
            String header = parseHeader();
            Map<String, String> metaData = parseMetaData();

            parseDB();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static String parseHeader() throws IOException {
        return new String(inStream.readNBytes(9));
    }

    private static Map<String, String> parseMetaData() throws IOException {
        Map<String, String> metaData = new HashMap<>();
        while (true) {
            int currByte = inStream.readUnsignedByte();
            if (currByte == 0xFA) {
                int checkDBStartByte = inStream.readUnsignedByte();

                int keySize = parseSize(checkDBStartByte);
                String key = new String(inStream.readNBytes(keySize));

                String value = "";
                int valSize = parseSize(inStream.readUnsignedByte());
                if(valSize == 0){
                    value = String.valueOf(inStream.readUnsignedByte());
                }else {
                    value = new String(inStream.readNBytes(valSize));
                }
                metaData.put(key, value);
            } else {
                break;
            }
        }
        return metaData;
    }

    private static void parseDB() throws IOException {
        int dbIndex = parseSize(inStream.readUnsignedByte());
        int type = inStream.readUnsignedByte();
        boolean resizePresent = false;
        if (type == 0xFB) {
            int hashTableSize = parseSize(inStream.readUnsignedByte());
            int expiresHashTableSize = parseSize(inStream.readUnsignedByte());
            resizePresent = true;
        }
        while (true) {
            if (!resizePresent) {
                type = inStream.readUnsignedByte();
            } else {
                resizePresent = false;
            }

            if (type == 0xFF) {
                break;
            }
            boolean canExpire = false;
            long expiry = 0;
            if (type == 0xFC || type == 0xFD) {
                canExpire = true;
                if (type == 0xFC) {
                    expiry = readLittleEndianLong();
                } else {
                    expiry = readLittleEndianInt();
                }
                inStream.readUnsignedByte();
                type = inStream.readUnsignedByte();
            }

            int keySize = parseSize(type);

            if (keySize == 0) {
                continue;
            }
            String key = new String(inStream.readNBytes(keySize));


            int valSize = parseSize(inStream.readUnsignedByte());

            String value = new String(inStream.readNBytes(valSize));

            if (!key.isEmpty()) {
                RedisMap.setValue(key, new RedisMap.Value(value, canExpire, expiry));
            }

        }
    }

    private static long readLittleEndianInt() throws IOException {
        return ((long) inStream.readUnsignedByte() |
                ((long) inStream.readUnsignedByte() << 8) |
                ((long) inStream.readUnsignedByte() << 16) |
                ((long) inStream.readUnsignedByte() << 24));
    }

    private static long readLittleEndianLong() throws IOException {
        return ((long) inStream.readUnsignedByte() |
                ((long) inStream.readUnsignedByte() << 8) |
                ((long) inStream.readUnsignedByte() << 16) |
                ((long) inStream.readUnsignedByte() << 24) |
                ((long) inStream.readUnsignedByte() << 32) |
                ((long) inStream.readUnsignedByte() << 40) |
                ((long) inStream.readUnsignedByte() << 48) |
                ((long) inStream.readUnsignedByte() << 56));
    }

    private static int parseSize(int sizeEncodingByte) throws IOException {
        int encoding = sizeEncodingByte & 0xC0;
        int size = 0;
        switch (encoding) {
            case 0x00:
                size = (sizeEncodingByte) & 0x3F;
                break;
            case 0x40:
                int nextByte = inStream.readUnsignedByte();
                int prevByte = ((sizeEncodingByte) & 0x3F);
                size = ((prevByte & 0xFF) << 8) | (nextByte & 0xFF);
                break;
            case 0x80:
                size = inStream.readInt();
                break;
            case 0xC0:
                break;
            default:
                throw new IOException("Bad Read");
        }
        return size;
    }
}
