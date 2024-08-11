package config;

import rdb.RDBParser;

public class RDBConfig {
    public static RDBConfig INSTANCE;
    public static boolean isRDBEnabled = false;
    private static String dir;
    private static String dbFileName;

    private RDBConfig(String dir, String dbFileName) {
        RDBConfig.dir = dir;
        RDBConfig.dbFileName = dbFileName;
    }

    public static void initializeInstance(String rdbDir, String rdbFileName) {
        if (!rdbDir.isEmpty() && !rdbFileName.isEmpty()) {
            INSTANCE = new RDBConfig(rdbDir, rdbFileName);
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
