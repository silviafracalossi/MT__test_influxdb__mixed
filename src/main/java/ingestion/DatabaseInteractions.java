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
  static final String username = "";
  static final String password = "";
  static String dbName;

  // Location of file containing data
  String data_file_path;


  // Constructor
  public DatabaseInteractions(String dbName, String data_file_path, Boolean useServerInfluxDB) {
    this.dbName=dbName;
    this.useServerInfluxDB=useServerInfluxDB;
    this.data_file_path=data_file_path;
  }

  // Iterating through data, inserting it one at a time
  public void insertOneTuple(Logger logger) {

    // Defining variables useful for method
    String[] fields;

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
          System.out.println("Inserted");
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
  }

  //----------------------DATABASE UTILITY--------------------------------------

  // Connecting to the InfluxDB database
  public static boolean createDBConnection() {
    if (useServerInfluxDB) {
      influxDB = InfluxDBFactory.connect(serverURL, username, password);
    } else {
      influxDB = InfluxDBFactory.connect(localURL, username, password);
    }

    // Pinging the DB
    Pong response = influxDB.ping();
    return !(response.getVersion().equalsIgnoreCase("unknown"));
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
