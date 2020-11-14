package ingestion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;


public class Main {

    // Store users' configurations - default settings written here
    static boolean useServerInfluxDB = false;
    static String data_file = "TEMPERATURE_nodup.csv";
    static String data_file_path;
    static String log_folder = "logs/";
    static String dbName;

    // Index chosen
    static int index_no = -1;
    static String[] index_types = {"inmem", "tsi1"};

    // Creating the database interactor
    static DatabaseInteractions dbi;

    public static void main(String[] args) {

        try {

            // Checking if the inserted parameters are enough
            if (args.length != 5) {
                System.out.println("Please, insert args");
                return;
            }

            // Getting what the user inserted
            getInfo(args);

            // Instantiate general logger and printing index
            Logger logger = instantiateLogger("ingestion");
            logger.info("Index: " + index_types[index_no]);

            // Loading the credentials to the new database
            logger.info("Instantiating database interactor");
            dbi = new DatabaseInteractions(dbName, data_file_path, useServerInfluxDB);

            // Marking start of tests
            logger.info("---Start of Tests!---");

            // Opening a connection to the InfluxDB database
            logger.info("Connecting to the InfluxDB database...");
            dbi.createDBConnection();

            // ==START OF TEST==
            dbi.insertOneTuple(logger);

            // ==END OF TEST==
            logger.info("--End of insertion--");

            // Close connections
            dbi.closeDBConnection();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dbi.closeDBConnection();
        }
    }


    //-----------------------UTILITY----------------------------------------------

    // Interactions with the user to understand his/her preferences
    public static void getInfo (String args[]) {

        // Getting information from user
        useServerInfluxDB = (args[0].compareTo("s")==0);
        dbName = args[1];

        // Checking if the default file is requested
        if (args[2].compareTo("d")==0) {
            args[2] = data_file;
        }
        // Checking if it is a file
        File f = new File("data/"+args[2]);
        if(f.exists() && !f.isDirectory()) {
            data_file_path = "data/"+args[2];
        }

        // Storing the name of the log folder
        log_folder += args[3]+"/";

        // Getting index
        index_no = returnStringIndex(index_types, args[4]);
    }

    // Instantiating the logger for the general information or errors
    public static Logger instantiateLogger (String file_name) throws IOException {

        // Instantiating general logger
        String log_complete_path = log_folder + file_name + "_" + index_types[index_no] + ".xml";
        Logger logger = Logger.getLogger("GeneralLog");
        logger.setLevel(Level.ALL);

        // Loading properties of log file
        Properties preferences = new Properties();
        try {
            FileInputStream configFile = new FileInputStream("resources/logging.properties");
            preferences.load(configFile);
            LogManager.getLogManager().readConfiguration(configFile);
        } catch (IOException ex) {
            System.out.println("[WARN] Could not load configuration file");
        }

        // Instantiating file handler
        FileHandler gl_fh = new FileHandler(log_complete_path);
        logger.addHandler(gl_fh);

        // Returning the logger
        return logger;
    }

    // Returns the index_no of the specified string in the string array
    public static int returnStringIndex(String[] list, String keyword) {
        for (int i=0; i<list.length; i++) {
            if (list[i].compareTo(keyword) == 0) {
                return i;
            }
        }
        return -1;
    }

}
