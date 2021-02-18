# Test - InfluxDB - Mixed

Tester of the InfluxDB ability of ingesting and querying time series data at the same time.

## Repository Structure
-   `data/`, containing the printers parsed logs files in the format of CSV files;
-   `logs/`, containing the log information of all the tests done;
-   `resources/`, containing the InfluxDB driver, the database credentials file and the logger properties;
-   `src/main/java`, containing the java source files;
-   `standalone/`, containing the JAR standalone version of this repository;
-   `target/`, containing the generated .class files after compiling the java code.

## Requirements
The repository is a Maven project. Therefore, the dependency that will automatically be downloaded is:
-   InfluxDB JDBC Driver (2.8)

## Installation and running the project
-   Create the folder `data`;
    -   Inside the folder, copy-paste the printers parsed log files, whose timestamp is defined in nanoseconds;
-   Run the project
    -   Execute `bash compile_and_run.bash [l/s] [table_name] [inmem/tsi]`

## Preparing the standalone version on the server
-   Connect to the unibz VPN through Cisco AnyConnect;
-   Open a terminal:
    -   Execute `ssh -t sfracalossi@ironlady.inf.unibz.it "cd /data/sfracalossi ; bash"`;
    -   Execute `mkdir influxdb`;
    -   Execute `mkdir influxdb/standalone_mixed`;
    -   Execute `mkdir influxdb/standalone_mixed/resources`;
    -   Execute `mkdir influxdb/standalone_mixed/logs`;
    -   Execute `mkdir influxdb/standalone_mixed/data`;
    -   Execute `mkdir influxdb/standalone_mixed/standalone`;
-   Send the JAR and the help files from another terminal (not connected through SSH):
    -   Execute `scp standalone/NDataIngestionTest.jar sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_mixed/standalone`;
    -   Execute `scp standalone/IngestionMixed.jar sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_mixed/standalone`;
    -   Execute `scp standalone/QueryingMixed.jar sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_mixed/standalone`;
    -   Execute `scp resources/logging.properties sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_mixed/resources`;
-   Send the data file:
    -   Execute `scp data/TEMPERATURE_HalfGB_ns.csv sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_mixed/data`;
    -   Execute `scp data/TEMPERATURE_1GB_ns.csv sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_mixed/data`;
-   Sending the scripts:
    -   Execute `scp compile_and_run.bash sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_mixed`;
    -   Execute `scp script.bash sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_mixed`;
-   Execute the JAR file (use the terminal connected through SSH):
    -   Execute `cd influxdb/standalone_mixed`;
    -   Execute `nohup bash compile_and_run.bash [l/s] [table_name] [inmem/tsi1] > logs/out.txt &`
