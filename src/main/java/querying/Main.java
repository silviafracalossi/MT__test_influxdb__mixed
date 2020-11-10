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
    static final String dbName = "test_table_n";
    static final String measurement = "temperature";
    static final String retention_policy_name = "testPolicy";
    static final String bucket_name = dbName + "/" + retention_policy_name;
    static String overall_start_time = "2017-12-01T00:00:00Z";
    static String overall_stop_time = "2017-12-05T00:00:00Z";

    // Other connection variables
    private static char[] token = "my-token".toCharArray();
    private static String org = "my-org";
    private static String bucket = "my-bucket";

    // Information about data
    static String data_loaded = "";

    // Index chosen
    static int index_no = -1;
    static String[] index_types = {"inmem", "tsi1"};

    // Logger names date formatter
    static Logger logger;


    public static void main(String[] args) {

        try {

            // Checking if the inserted parameters are enough
            if (args.length != 2) {
                System.out.println("Please, insert args");
                return;
            }

            // Getting what the user inserted
            getInfo(args);

            // Changing the range based on the dataset loaded
            if (data_loaded.compareTo("1GB") == 0) {
                overall_start_time = "2009-01-01T00:00:00Z";
                overall_stop_time = "2010-06-05T21:00:00Z";
            }

            // Instantiate loggers
            logger = instantiateLogger("querying");

            // Opening a connection to the postgreSQL database
            logger.info("Connecting to the InfluxDB database...");
            createDBConnection();

            // Executing queries
            logger.info("Starting queries execution");

            // Repeating infinitely the query
            while(true) {
                allData_windowsAnalysis();
                lastTwoDays_timedMovingAverage();
                lastThirtyMinutes_avgMaxMin();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeDBConnection();
        }
    }

    // For windows of 30 minutes, calculate mean, max and min.
    public static void allData_windowsAnalysis() {

        // Printing method name
        System.out.println("1) allData_windowsAnalysis");

        // Creating the query
        String window_size = "30m";
        String time_range = "start: " + overall_start_time + ", stop: " + overall_stop_time;
        String allData_query = "mean = from(bucket: \"" + bucket_name + "\")\n" +
                "  |> range(" + time_range + ")\n" +
                "  |> filter(fn: (r) =>  r._measurement == \"" + measurement + "\" and (r._field == \"value\"))\n" +
                "  |> window(every: " + window_size + ") \n" +
                "  |> mean()\n" +
                "  |> drop(columns: [\"_field\", \"_measurement\", \"table\"])\n" +

                "max = from(bucket: \"" + bucket_name + "\")\n" +
                "  |> range(" + time_range + ")\n" +
                "  |> filter(fn: (r) =>  r._measurement == \"" + measurement + "\" and (r._field == \"value\"))\n" +
                "  |> window(every: " + window_size + ") \n" +
                "  |> max()\n" +
                "  |> drop(columns: [\"_field\", \"_measurement\", \"table\",\"_time\"])\n" +

                "min = from(bucket: \"" + bucket_name + "\")\n" +
                "  |> range(" + time_range + ")\n" +
                "  |> filter(fn: (r) =>  r._measurement == \"" + measurement + "\" and (r._field == \"value\"))\n" +
                "  |> window(every: " + window_size + ") \n" +
                "  |> min()\n" +
                "  |> drop(columns: [\"_field\", \"_measurement\", \"table\",\"_time\"])\n" +

                "first_join = join(tables: {mean:mean, max:max}, on: [\"_start\", \"_stop\"])\n" +

                "join(tables: {first_join:first_join, min:min}, on: [\"_start\", \"_stop\"])\n" +
                " |> yield()\n";

        // Executing the query
        logger.info("Executing windowsAnalysis on AllData");
        List<FluxTable> tables = queryApi.query(allData_query);
        logger.info("Completed execution");
    }


    // Every 2 minutes of data, computes the average of the current temperature value
    //      and the ones of the previous 4 minutes on last 2 days of data
    public static void lastTwoDays_timedMovingAverage() {

        // Printing method name
        System.out.println("2) lastTwoDays_timedMovingAverage");

        // Creating the query
        String start_two_days = "2017-12-03T00:00:00Z";
        String end_two_days = "2017-12-05T00:00:00Z";
        String lastTwoDays_query = "" +
                "from(bucket:\"" + bucket_name + "\")" +
                " |> range(start: " + start_two_days + ", stop: " + end_two_days + ")" +
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
        int year = (data_loaded.compareTo("1GB") == 0) ? 2009 : 2017;
        String start_thirty_minutes = year + "-12-01T11:00:00Z";
        String end_thirty_minutes = year + "-12-01T11:30:00Z";
        String lastThirtyMinutes_query = "" +
                " SELECT MEAN(value), MAX(value), MIN(value) " +
                " FROM " + measurement +
                " WHERE time > '" + start_thirty_minutes + "' " +
                "   AND time < '" + end_thirty_minutes + "' ";

        // Executing the query
        logger.info("Executing AvgMaxMin on LastThirtyMinutes");
        QueryResult queryResult = influxDB.query(new Query(lastThirtyMinutes_query, dbName));
        logger.info("Completed execution");
    }

    //-----------------------UTILITY----------------------------------------------

    public static void getInfo(String args[]) {
        requestedURL = (args[0].compareTo("l") == 0) ? localURL : serverURL;
        log_folder += args[1]+"/";
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