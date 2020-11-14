# == Printing received params ([l/s] [table_name] [inmem/tsi])
echo "==Printing selection=="
echo "Server or local: $1"
echo "Database Table Name: $2"
echo "Index: $3"
echo ""
#
# == Creating the log folder
echo "Creating folder..."
folder=$(date +'%Y-%m-%d__%H.%M.%S')
mkdir -m 777 -p "logs/${folder}"
mkdir -m 777 -p "logs/${folder}/extra"
#
# == Executing N Ingestion ([M] [N] [l/s] [table_name] [file_name_in_data_folder]) #TEMPERATURE_HalfGB_ns.csv
echo "Executing NDataIngestionTest.jar..."
java -jar standalone/NDataIngestionTest.jar 200 200 $1 $2 TEMPERATURE_nodup.csv > "logs/${folder}/extra/out__N_ingestion.txt"
rm -r logs/2020*_200
echo "Ingestion completed!"
echo ""
#
# == Executing Ingestion Part ([l/s] [dbName] [data_file_name] [log_folder] [inmem/tsi])
echo "Executing IngestionMixed.jar ..."
nohup java -jar standalone/IngestionMixed.jar $1 $2 TEMPERATURE_nodup.csv $folder $3 > "logs/${folder}/extra/out__ingestion.txt" 2> "logs/${folder}/extra/err__ingestion.txt" &
#
# == Executing Querying Part ([l/s] [dbName] [log_folder] [inmem/tsi])
echo "Executing QueryingMixed.jar ..."
nohup java -jar standalone/QueryingMixed.jar $1 $2 $folder $3 > "logs/${folder}/extra/out__querying.txt" 2> "logs/${folder}/extra/err__querying.txt" &
#
# == Waiting 2 hours
echo "Sleeping 2 hours..."
sleep 30s
#
# == Stopping the processes
echo ""
echo "Stopping processes..."
kill $(ps | grep java | awk '{print $1;}')
#
# == Last fixes to output
echo "Fixing some things..."
rm "logs/${folder}/querying_${3}.xml.lck"
rm "logs/${folder}/ingestion_${3}.xml.lck"
echo "</logs>" >> "logs/${folder}/querying_${3}.xml"
echo "</logs>" >> "logs/${folder}/ingestion_${3}.xml"
#
# == Completed!
echo "Completed!"
