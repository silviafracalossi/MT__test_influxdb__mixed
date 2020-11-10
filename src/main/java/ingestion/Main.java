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

    // Creating the database interactor
    static DatabaseInteractions dbi;

    public static void main(String[] args) {

        try {

            // Checking if the inserted parameters are enough
            if (args.length != 3) {
                System.out.println("Please, insert args");
                return;
            }

            // Getting what the user inserted
            getInfo(args);

            // Instantiate general logger
            Logger general_logger = instantiateLogger("ingestion");

            // Loading the credentials to the new database
            general_logger.info("Instantiating database interactor");
            dbi = new DatabaseInteractions(data_file_path, useServerInfluxDB);

            // Marking start of tests
            general_logger.info("---Start of Tests!---");

            // Opening a connection to the InfluxDB database
            general_logger.info("Connecting to the InfluxDB database...");
            dbi.createDBConnection();
            dbi.createDatabase();

            // ==START OF TEST==
            dbi.insertOneTuple(general_logger);

            // ==END OF TEST==
            general_logger.info("--End of insertion--");

            // Clean database and close connections
            endOfTest();

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

        // Checking if the default file is requested
        if (args[1].compareTo("d")==0) {
            args[1] = data_file;
        }
        // Checking if it is a file
        File f = new File("data/"+args[1]);
        if(f.exists() && !f.isDirectory()) {
            data_file_path = "data/"+args[1];
        }

        // Storing the name of the log folder
        log_folder += args[2]+"/";
    }

    // Instantiating the logger for the general information or errors
    public static Logger instantiateLogger (String file_name) throws IOException {

        // Instantiating general logger
        String log_complete_path = log_folder + file_name + ".xml";
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

    // Cleans the database and closes all the connections to it
    public static void endOfTest() {
        dbi.closeDBConnection();
    }
}
