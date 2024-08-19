package config;

import java.net.Socket;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class ReplicationConfig {
    private static boolean isSlave;
    private static String masterIP;
    private static String masterPort;
    private static String masterReplicationId;
    private static long masterOffset;


    private static ConcurrentSkipListMap<Integer, Set<String>> slavePorts = new ConcurrentSkipListMap<>();

    private static ConcurrentHashMap.KeySetView<Socket, Boolean> slaveConnections = ConcurrentHashMap.newKeySet();

    private ReplicationConfig(boolean isSlave) {
        this.isSlave = isSlave;
    }

    public static void addSlaveConnection(Socket slaveSocket){
        slaveConnections.add(slaveSocket);
    }

    public static Set<Socket> getSlaveConnections(){
        return slaveConnections;
    }

    public static void addCapabilitiesToSlave(String capability){
        slavePorts.get(slavePorts.lastKey()).add(capability);
    }

    public static void addSlavePort(Integer port) {
        ReplicationConfig.slavePorts.put(port, new HashSet<>());
    }

    public static String getMasterReplicationId() {
        return masterReplicationId;
    }

    public static long getMasterOffset() {
        return masterOffset;
    }

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

    public static boolean isSlave() {
        return isSlave;
    }

    public static void setIsSlave(boolean isSlave) {
        ReplicationConfig.isSlave = isSlave;
    }

    public static void initializeReplicationId() {
        if (!isSlave) {
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
