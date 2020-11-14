package querying;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxTable;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.util.*;
import java.io.*;

public class Main {

    // DB variables
    static QueryApi queryApi;
    static InfluxDB influxDB = null;
    static InfluxDBClient influxDBClient;

    // Information from user
    static String log_folder = "logs/";

    // Databases URLs
    static final String serverURL = "http://ironmaiden.inf.unibz.it:8086";
    static final String localURL = "http://localhost:8086";
    static String requestedURL = "";

    // Databases Username, Password and Database name
    static final String username = "root";
    static final String password = "root";

    // Database Objects names
    static String dbName;
    static String bucket_name;
    static final String measurement = "temperature";
    static final String retention_policy_name = "testPolicy";

    // Time range
    static String now;

    // Other connection variables
    private static char[] token = "my-token".toCharArray();
    private static String org = "my-org";
    private static String bucket = "my-bucket";

    // Index chosen
    static int index_no = -1;
    static String[] index_types = {"inmem", "tsi1"};

    // Logger names date formatter
    static Logger logger;


    public static void main(String[] args) {

        try {

            // Checking if the inserted parameters are enough
            if (args.length != 4) {
                System.out.println("Please, insert args");
                return;
            }

            // Getting what the user inserted
            getInfo(args);

            // Defining bucket name
            bucket_name = dbName + "/" + retention_policy_name;

            // Instantiate loggers and printing index name
            logger = instantiateLogger("querying");
            logger.info("Index: " + index_types[index_no]);

            // Opening a connection to the InfluxDB database
            logger.info("Connecting to the InfluxDB database...");
            createDBConnection();

            // Getting the max timestamp
            getNow();

            // Repeating infinitely the query
            logger.info("Starting queries execution");
            while(true) {
                lastTwoDays_timedMovingAverage();
                lastThirtyMinutes_avgMaxMin();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeDBConnection();
        }
    }


    //-----------------------QUERIES----------------------------------------------

    // 0. Get Now - the max timestamp in the dataset
    public static void getNow() {
        String count_query = "SELECT * FROM "+measurement+" ORDER BY time DESC LIMIT 1";
        QueryResult queryResult = influxDB.query(new Query(count_query, dbName));
        now = queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(0).toString();
    }


    // Every 2 minutes of data, computes the average of the current temperature value
    //      and the ones of the previous 4 minutes on last 2 days of data
    public static void lastTwoDays_timedMovingAverage() {

        // Printing method name
        System.out.println("2) lastTwoDays_timedMovingAverage");

        // Creating the query
        String lastTwoDays_query = "import \"experimental\"\n" +
                "from(bucket:\"" + bucket_name + "\")" +
                " |> range(start: experimental.subDuration( \n" +
                "                  d: 2d, \n" +
                "                  from: "+now+")) \n" +
                " |> filter(fn:(r) => " +
                "       r._measurement == \"" + measurement + "\"" +
                " )" +
                " |> timedMovingAverage(every: 2m, period: 4m)";

        // Executing the query
        logger.info("Executing timedMovingAverage on LastTwoDays");
        List<FluxTable> tables = queryApi.query(lastTwoDays_query);
        logger.info("Completed execution");
    }


    // 3. Calculate mean, max and min on last (arbitrary) 30 minutes of data
    public static void lastThirtyMinutes_avgMaxMin() {

        // Printing method name
        System.out.println("3) lastThirtyMinutes_avgMaxMin");

        // Creating the query
        String lastThirtyMinutes_query = "" +
                " SELECT MEAN(value), MAX(value), MIN(value) " +
                " FROM " + measurement +
                " WHERE time >= '" + now + "' - 30m ";

        // Executing the query
        logger.info("Executing AvgMaxMin on LastThirtyMinutes");
        QueryResult queryResult = influxDB.query(new Query(lastThirtyMinutes_query, dbName));
        logger.info("Completed execution");
    }

    //-----------------------UTILITY----------------------------------------------

    public static void getInfo(String args[]) {
        requestedURL = (args[0].compareTo("l") == 0) ? localURL : serverURL;
        dbName = args[1];
        log_folder += args[2]+"/";
        index_no = returnStringIndex(index_types, args[3]);
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

    //----------------------DATABASE----------------------------------------------

    // Connecting to the InfluxDB database
    public static void createDBConnection() {

        // Connecting to the DB
        influxDB = InfluxDBFactory.connect(requestedURL, username, password);

        // Pinging the DB
        Pong response = influxDB.ping();

        // Printing a message in case of failed connection
        if (response.getVersion().equalsIgnoreCase("unknown")) {
            logger.severe("Failed connecting to the Database InfluxDB");
        } else {

            // Setting a larger read timeout
            OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();
            okHttpClientBuilder.readTimeout(1, TimeUnit.HOURS);
            InfluxDBClientOptions options = new InfluxDBClientOptions.Builder()
                .url(requestedURL)
                .authenticateToken(token)
                .org(org)
                .bucket(bucket)
                .okHttpClient(okHttpClientBuilder)
                .build();

            // Connecting the influxdb client for flux too
            influxDBClient = InfluxDBClientFactory.create(options);
            queryApi = influxDBClient.getQueryApi();
        }
    }

    // Closing the connections to the database
    public static void closeDBConnection() {
        try {
            influxDB.close();
            influxDBClient.close();
        } catch (NullPointerException e) {
            logger.severe("Closing DB connection - NullPointerException");
        }
    }
}