package config;

import rdb.RDBParser;

public class RDBConfig {
    public static RDBConfig INSTANCE;
    private static String dir;
    private static String dbFileName;

    public static boolean isRDBEnabled = false;

    private RDBConfig(String dir, String dbFileName) {
        RDBConfig.dir = dir;
        RDBConfig.dbFileName = dbFileName;
    }

    public static void initializeInstance(String rdbDir, String rdbFileName) {
        System.out.println(rdbDir + " :: " + rdbFileName);
        if(!rdbDir.isEmpty() && !rdbFileName.isEmpty()){
            System.out.println(rdbDir + " :: " + rdbFileName);
            INSTANCE = new RDBConfig(dir, dbFileName);
            isRDBEnabled = true;
            RDBParser.parseRDB();
        }
    }

    public static RDBConfig getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("RDBConfig has not been initialized. Call initializeInstance first.");
        }
        return INSTANCE;
    }

    public String getDir() {
        return dir;
    }

    public String getDbFileName() {
        return dbFileName;
    }

    public String getFullPath() {
        return dir + "/" + dbFileName;
    }
}
