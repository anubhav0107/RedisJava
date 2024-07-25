package config;

public class RDBConfig {
    public static RDBConfig INSTANCE;
    private static String dir;
    private static String dbFileName;

    public static boolean isRDBEnabled;

    private RDBConfig(String dir, String dbFileName) {
        this.dir = dir;
        this.dbFileName = dbFileName;
    }

    public static void initializeInstance(String[] args) {
        String dir = System.getProperty("user.dir"); // Default to current working directory
        String dbFileName = "dump.rdb"; // Default filename
        if(args.length >= 4){
            dir = args[1];
            dbFileName = args[3];
            isRDBEnabled = true;
        }else{
            isRDBEnabled = false;
        }
        INSTANCE = new RDBConfig(dir, dbFileName);
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
