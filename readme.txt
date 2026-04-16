-----------------------------------------------------------
1. Project Title:
-----------------------------------------------------------
Real-Time Analysis of Air Quality Data for Urban Health Monitoring (SDG 3 – Good Health and Well-being and SDG 11 - Sustainable Cities and Communities)


-----------------------------------------------------------
2. Project Folder Structure: 
-----------------------------------------------------------
/Code/
│
├── Task1StreamRawData/
│   ├── consumer.py
│   ├── global_air_quality.csv 
│   ├── producer.py         
│
├── Task2ProcessData/
│   ├── preprocess/
│       ├── enrich.py
│       ├── fill_AQI.py
│       ├── fillers.py
│       ├── imputers.py
│       ├── rangeCheck.py
│       ├── removeDuplicate.py
│       ├── standardizer.py
│   ├── validate/
│       ├── error_labeler.py
│       ├── validation_config.py
│       ├── validation_predicates.py
│       ├── validator_pipeline.py
│       ├── validity_rules.py
│   ├── noise.csv
│   ├── runPipeline.py
│   ├── Task2Noise.ipynb
│
├── Task3Mongo/
│   ├── analytics_queries.py
│   ├── create_indexes.py
│   ├── pymongo_utils.py
│   ├── Task3.ipynb
│   ├── transform_and_load.py
│
├── Task4Neo4j/
│   ├── load_to_neo4j.py
│   ├── loader.py
│   ├── neo4j_queries.py
│   ├── neo4j_writer.py
│   ├── Task4.ipynb
│
├── Task5Kafka/
│   ├── main_pipeline.py
│   ├── spark_streaming_config.py
│   ├── stream_aggregator.py
│   ├── stream_reader.py
│   ├── stream_transformer.py
│   ├── stream_aggregator.py
│   ├── stream_writer.py
│   ├── task5_demo.ipynb
│
├── create_venv.sh
├── global_air_quality.csv
├── noise.csv
├── requirements.txt             
└── readme.txt


-----------------------------------------------------------
3. Links to Data Folder:
-----------------------------------------------------------
(Optional: include links if the size of the data files is too large. Ensure that you have shared the folder with your tutor)
https://drive.google.com/drive/folders/1-ZG7Th2DKLpxPokg9BBVuBql9ilm2JJm?usp=drive_link


    
-----------------------------------------------------------
4. Setup Instructions:
-----------------------------------------------------------
1. Install dependencies:
   $ pip install -r requirements.txt


2. Start Kafka server and necessary services (HDFS, YARN, Zookeeper, Kafka).


3. To run the demo:


   Task 1
   —-----
   wsl ~ 
   # starts WSL and opens it directly in your Linux home directory.
   start-dfs.sh      
   # start the Hadoop Distributed File System (HDFS) daemons.


   start-yarn.sh     
   # start the YARN (Yet Another Resource Negotiator) daemons, which handle
   cluster resource management and job scheduling.


   zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties & 
   # starts ZooKeeper in the background with Kafka’s default config.


   kafka-server-start.sh $KAFKA_HOME/config/server.properties &      
   # starts a Kafka broker after ZooKeeper is already running and makes it  
   run in the background.


   jps  
   # Check running Java processes which should displayed DataNode,  
   NameNode, QuorumPeerMain, Kafka.


   hdfs dfs -mkdir /user/student
   # Create HDFS directory /user/student.


   hdfs dfs -ls /user
   # List contents of /user in HDFS.


   hdfs dfs -chown student:hduser /user/student
   # Give ownership of /user/student to user student and group hduser.


   hdfs dfs -chmod 777 /
   # Allow everyone full read/write/execute on HDFS root /


   su - student
   # Switch to Linux user student.


   mkdir de-ass
   # Create local project folder de-ass.


   cp -r /mnt/c/de/* /home/student/de-ass
   # Copy all the content in de folder from Windows (/mnt/c/de/) into WSL 
   (/home/student/de-ass).


   cd de-ass
   # Enter the project directory de-ass.


   ./create_venv.sh
   # Run script to create a Python virtual environment.


   de-ass-venv
   # Enter de-ass-venv as the name of the virtual environment.


   /home/student/de-ass
   # /home/student/de-ass determine where the virtual environment is 
   installed.


   nano ~/.profile
   # Open profile config for editing environment variables.


   export SPARK_HOME=/home/hduser/spark
   export PATH=$PATH:$SPARK_HOME/bin
   export KAFKA_HOME=/home/hduser/kafka
   export PATH=$PATH:$KAFKA_HOME/bin
   # Add Spark and Kafka to the system path so their commands work anywhere


   source ~/.profile
   # Reload environment variables.


   cd ~
   # Go to the home directory.


kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic air_quality
   # Create Kafka topic air_quality with 1 partition and replication factor   
   1.


   cd de-ass
   # Enter the project directory de-ass.


   source de-ass-venv/bin/activate
   # Activate Python virtual environment.


   pip install kafka-python
   pip install lz4
   # Install Kafka client library along with the compression library for 
   producers.


   cd Task1StreamRawData
   # Move into Task1StreamRawData folder.


  spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1  \
  consumer.py \
  --bootstrap localhost:9092 \
  --topic air_quality \
  --hdfs_path hdfs://localhost:9000/air_quality_json_struct \
--checkpoint hdfs://localhost:9000/checkpoints/air_quality_json_struct_v2 \
  --raw_path hdfs://localhost:9000/global_air_quality/raw_csv \
  --checkpoint_raw hdfs://localhost:9000/checkpoints/air_quality_raw_v2 \
  --trigger "10 seconds"


    
   # Another Terminal
   wsl ~
   # starts WSL and opens it directly in your Linux home directory.


   su - student
   # Switch to Linux user student.
   
   cd de-ass
   # Enter the project directory de-ass.


   source de-ass-venv/bin/activate
   # Activate Python virtual environment.


   cd Task1StreamRawData
   # Move into Task1StreamRawData folder.


  python3 producer.py \
  --bootstrap localhost:9092 \
  --topic air_quality \
  --file ./global_air_quality.csv \
--hdfs-path hdfs://localhost:9000/global_air_quality/raw_copy/air_quality.jsonl \
  --has-header \
  --delay 0.05 \
  --compression lz4 \
  --log-every 10 \
  --show-metadata


   # Run Kafka producer:
* Reads global_air_quality.csv
* Sends rows to Kafka topic air_quality
* Also writes .jsonl copy to HDFS /raw_copy
* Waits 0.05s between messages
* Compresses messages with LZ4
* Logs every 10 rows, shows metadata


   # Verify
   # Look for files written
   hdfs dfs -ls /air_quality_json_struct
   # Check structured JSON output exists.


   hdfs dfs -ls /global_air_quality/raw_copy
   # Check if a raw JSON copy exists.


   hdfs dfs -cat /global_air_quality/raw_copy/air_quality.jsonl |head -n 10
   # Show first 10 JSON records.


   hdfs dfs -tail /global_air_quality/raw_copy/air_quality.jsonl
   # Show last records written.


   # Reset if need to rerun
   echo "Deleting old raw copy..."
   hdfs dfs -rm -r -f /global_air_quality/raw_copy
   # Delete old raw copy.


   echo "Deleting old raw_csv outputs..."
   hdfs dfs -rm -r -f /global_air_quality/raw_csv
   # Delete old raw CSV outputs.
 
   echo "Deleting old structured JSON outputs..."
   hdfs dfs -rm -r -f /air_quality_json_struct
   # Delete structured JSON outputs.


   echo "Deleting checkpoints..."
   hdfs dfs -rm -r -f /checkpoints/air_quality_raw_v2
   hdfs dfs -rm -r -f /checkpoints/air_quality_json_struct_v2
   # Delete old checkpoints.


   Task 2
   —-----
   # In virtual environment (de-ass-venv)
   cd ~
   # Go to the home directory.


   cd de-ass
   # Enter the project directory de-ass.


   cd Task2ProcessData
   # Move into Task2ProcessData folder.


   hdfs dfs -mkdir -p /user/student/air_quality_cleaned
   # Create HDFS directory /user/student/air_quality_cleaned with -p ensures parent directories are created if missing).


   spark-submit --master local[*] runPipeline.py
   # Run your Spark application (runPipeline.py) with Spark’s spark-submit tool.


   # Verify
   
   hdfs dfs -ls hdfs://localhost:9000/user/student/air_quality_cleaned
   # List contents of cleaned output directory.


hdfs dfs -ls hdfs://localhost:9000/user/student/air_quality_cleaned/valid_json
   # Check valid records output.


hdfs dfs -ls hdfs://localhost:9000/user/student/air_quality_cleaned/invalid_labeled_json
   # Check invalid records output.


hdfs dfs -cat hdfs://localhost:9000/user/student/air_quality_cleaned/valid_json/part-00000-*.json | head -20
   # Show first 20 valid records.


hdfs dfs -cat hdfs://localhost:9000/user/student/air_quality_cleaned/invalid_labeled_json/part-00000-*.json | head -20
   # Show first 20 invalid records.


   Task 3
   —-----
   cd de-ass
   source de-ass-venv/bin/activate
   #Activate virtual environment


   python -m pip install "pymongo[srv]==3.11"
   # installs the MongoDB driver pymongo version 3.11 with DNS SRV support
   in your virtual environment so you can connect to MongoDB clusters using    a connection string




   cd Task3Mongo
   # Move into Task3Mongo folder.


   python3 create_indexes.py
   # Run script to create an index on dedup field for improves query speed, 
   avoids duplicates).


   spark-submit transform_and_load.py \
  --in hdfs://localhost:9000/user/student/air_quality_cleaned/valid_json/part-00000-*.json \
  --mongo-uri "mongodb+srv://mongo:mongo12345@cluster0.kx5h57i.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" \
  --db aqi_demo \
  --coll aq_readings \
  --batch-size 1000


   # Run a PySpark application that transforms air quality data and loads 
   it into MongoDB.


   python3 analytics_queries.py
   # Run Python script with MongoDB queries.


   For one time cleanup only use when needed (To rerun)
   python3 - <<'PY'
   from pymongo_utils import PyMongoUtils
   pmu = PyMongoUtils()
   coll = pmu.get_collection("aqi_demo","aq_readings")
   res = coll.update_many({}, {"$unset": {"dq_flags": ""}})
   print("modified:", res.modified_count)
   PY
   # Connects to MongoDB, goes into the aqi_demo.aq_readings collection, 
   and removes the field dq_flags from all records, then prints how many 
   documents were modified.


   Task 4
   —-----
   cd de-ass
   source de-ass-venv/bin/activate
   
   pip install neo4j
   pip install tabulate
   # Installs the Python driver needed to talk to a Neo4j database from  
   Python scripts.


   cd Task4Neo4j
   # Move into Task4Neo4j folder.


   python load_to_neo4j.py
   # Load cleaned AQI data into Neo4j.


   python neo4j_queries.py
   # Run graph queries.
   
   CALL db.schema.visualization()
   # Show schema visualization in Neo4j Browser.


   Task 5
   —-----


# Terminal 1
wsl ~
# Starts WSL and opens it in your Linux home directory.


su - student
# Switch to the 'student' user.


cd de-ass
# Enter the project directory.


source de-ass-venv/bin/activate
# Activate the Python virtual environment.


pip install streamlit pandas plotly hdfs
# Install necessary Python libraries for the dashboard.


hdfs dfs -mkdir -p /user/student/dashboard_data
hdfs dfs -mkdir -p /user/student/checkpoints
# Create HDFS directories for Parquet output and checkpoints.


spark-submit --master local[4] \
  task5_main_pipeline.py \
  --hdfs_path hdfs://localhost:9000/user/student/air_quality_cleaned/valid_json \
  --parquet_path hdfs://localhost:9000/user/student/dashboard_data \
  --checkpoint_path hdfs://localhost:9000/user/student/checkpoints
# Run the main Spark Streaming application.
# This will start three streaming queries and show their output in this terminal.
# Leave this terminal running.


#OR in demo mode
spark-submit --master local[4] \
  --conf spark.sql.shuffle.partitions=5 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  main_pipeline.py \
  --hdfs-path hdfs://localhost:9000/user/student/air_quality_cleaned/valid_json \
  --checkpoint-base hdfs://localhost:9000/user/student/streaming \
  --duration 2 \
  --mode demo




# Terminal 2
wsl ~
# Starts WSL


su - student
# Switch to the 'student' user.


cd de-ass
# Enter the project directory.


source de-ass-venv/bin/activate
# Activate the Python virtual environment.


pip install jupyterlab
# Make sure Jupyter Lab is installed in your environment.


jupyter lab
# Starts the Jupyter Lab server. It will output one or more URLs in the terminal.


# Copy URL and paste it into the web browser. Jupyter Lab interface will be opened.


# Inside the Jupyter Lab file browser, navigate to and open the 
# 'task5_demo.ipynb' notebook.


# Run notebook
