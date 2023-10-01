Step #1:

Start zookeeper:
./bin/zookeeper-server-start.sh config/zookeeper.properties

Start server:
bin/kafka-server-start.sh config/server.properties

Create topic:
bin/kafka-topics.sh --create --topic stock-topic --bootstrap-server localhost:9092

Step #2:
	Check if the HBase Master and Region Server is running, by executing following command in terminal:
	sudo service --status-all

	If not Start HBase Master and Region Server, by executing following command in terminal:
	sudo service hbase-master start
	sudo service hbase-regionserver start

Step #3:
	Create External Table:
	CREATE EXTERNAL TABLE StockCount (Stock_type String, count INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES("SEPERATOR" = ",", 	"QUOTECHAR"="\"", "ESCAPECHAR"="\"") STORED AS TEXTFILE location '/user/cloudera/SparkTableCount' TBLPROPERTIES("skip.header.line.count"="1");


	CREATE EXTERNAL TABLE StockResult (key String, stock_type String, keyword String) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES("SEPERATOR" = 	",", "QUOTECHAR"="\"", "ESCAPECHAR"="\"") STORED AS TEXTFILE location '/user/cloudera/SparkTableResult' TBLPROPERTIES("skip.header.line.count"="1");
