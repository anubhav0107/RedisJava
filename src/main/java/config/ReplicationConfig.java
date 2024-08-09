package config;

import java.util.Random;

public class ReplicationConfig {
    private static boolean isSlave;
    private static String masterIP;
    private static String masterPort;

    public static String getMasterReplicationId() {
        return masterReplicationId;
    }

    public static long getMasterOffset() {
        return masterOffset;
    }

    private static String masterReplicationId;

    private static long masterOffset;

    public static String getMasterIP() {
        return masterIP;
    }

    public static void setMasterIP(String masterIP) {
        ReplicationConfig.masterIP = masterIP;
    }

    public static String getMasterPort() {
        return masterPort;
    }

    public static void setMasterPort(String masterPort) {
        ReplicationConfig.masterPort = masterPort;
    }

    private ReplicationConfig(boolean isSlave){
        this.isSlave = isSlave;
    }

    public static boolean isSlave() {
        return isSlave;
    }

    public static void setIsSlave(boolean isSlave) {
        ReplicationConfig.isSlave = isSlave;
    }

    public static void initializeReplicationId(){
        if(!isSlave){
            masterReplicationId = generateRandomAlphanumericString(40);
            masterOffset = 0;
        }
    }

    public static String generateRandomAlphanumericString(int length) {
        String characters = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(characters.length());
            sb.append(characters.charAt(randomIndex));
        }

        return sb.toString();
    }

}
