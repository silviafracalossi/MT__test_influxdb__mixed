package ingestion;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class DatabaseInteractions {

  // DB variables
  static InfluxDB influxDB = null;
  static boolean useServerInfluxDB = false;

  // Databases URLs
  static final String serverURL = "http://ironmaiden.inf.unibz.it:8086";
  static final String localURL = "http://localhost:8086";

  // Databases Username, Password and Database name
  static final String username = "root";
  static final String password = "root";
  static final String dbName = "test_table_n";

  // Retention policy definition
  static String retention_policy_name = "testPolicy";
  static String duration = "INF";
  static String replication = "1";

  // Location of file containing data
  String data_file_path;


  // Constructor
  public DatabaseInteractions(String data_file_path, Boolean useServerInfluxDB) {
    this.useServerInfluxDB=useServerInfluxDB;
    this.data_file_path=data_file_path;
  }

  // Iterating through data, inserting it one at a time
  public void insertOneTuple(Logger logger) {

    // Defining variables useful for method
    String[] fields;
    int rows_inserted = 0;

    try {

      // Preparing file scanner
      Scanner reader = new Scanner(new File(data_file_path));

      // Signaling start of test
      logger.info("--Start of test--");
      while (reader.hasNextLine()) {

        // Retrieving the data and preparing insertion script
        fields = reader.nextLine().split(",");

        // Creating point and writing it to the DB
        Point point = Point.measurement("temperature")
                .time(Long.parseLong(fields[0]), TimeUnit.NANOSECONDS)
                .addField("value", Integer.parseInt(fields[1]))
                .build();

        // Inserting data + Catching exception in case of database not working
        try {
          influxDB.write(dbName, "testPolicy", point);
          logger.info("Query executed: ("+fields[0]+","+fields[1]+")\n");
          rows_inserted++;
        } catch (InfluxDBException e) {
          System.out.println("Problems with executing the query on the DB");
          logger.severe("Problems with executing query: ("+fields[0]+","+fields[1]+")\n");
        }
      }

      // Closing the file reader
      reader.close();

    } catch (FileNotFoundException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
      logger.severe("Insertion: \"1\" - problems with the execution");
    }

    // Checking the number of rows inserted
    int rows_count = getRowsInDatabase();
    if (rows_count == rows_inserted) {
      logger.info("Total rows inserted: "+rows_inserted);
    } else {
      logger.severe("Supposed rows inserted: "+rows_inserted+" but found "+rows_count);
    }
  }

  //----------------------DATABASE UTILITY--------------------------------------

  // Connecting to the InfluxDB database
  public static boolean createDBConnection() {
    String pos_complete_url;
    if (useServerInfluxDB) {
      influxDB = InfluxDBFactory.connect(serverURL, username, password);
    } else {
      influxDB = InfluxDBFactory.connect(localURL, username, password);
    }

    // Pinging the DB
    Pong response = influxDB.ping();
    return !(response.getVersion().equalsIgnoreCase("unknown"));
  }

  // Creating the table "test_table" in the database
  public static void createDatabase () {
    removeDatabase();
    influxDB.createDatabase(dbName);

    // CREATE RETENTION POLICY testPolicy ON test_table DURATION INF REPLICATION 1
    String query_string = "CREATE RETENTION POLICY "+retention_policy_name+" ON "+dbName+
            " DURATION "+duration+" REPLICATION "+replication+" DEFAULT";
    influxDB.query(new Query(query_string, dbName));
    influxDB.setRetentionPolicy("testPolicy");
  }

  // Get the number of rows present in the database
  public static int getRowsInDatabase() {
    QueryResult queryResult;
    String count_query = "SELECT COUNT(*) FROM \"temperature\"";
    queryResult = influxDB.query(new Query(count_query, dbName));

    String count_in_string = (queryResult.getResults().get(0).getSeries()
            .get(0).getValues().get(0).get(1)) + "";
    int count = Integer.parseInt((count_in_string).substring(0, count_in_string.length() - 2));
    return (count > -1) ? count : 0;
  }

  // Dropping the table "test_table" from the database
  public static void removeDatabase() {
    try {
      influxDB.deleteDatabase(dbName);
    } catch (NullPointerException e) {
      System.out.println("Test table was already removed");
    }
  }

  // Closing the connections to the database
  public static void closeDBConnection() {
    try {
      influxDB.close();
    } catch (NullPointerException e) {
      System.out.println("Closing DB connection - NullPointerException");
    }
  }
}
