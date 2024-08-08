package config;

public class ReplicationConfig {
    private static boolean isSlave;
    private static String masterIP;
    private static String masterPort;

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
}
