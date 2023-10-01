package StockSparkSQL;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.AnalysisException;

public class App {

	public static void main(String[] args) throws AnalysisException,
			IOException {

		SparkConf conf = new SparkConf().setAppName("StockSparkSQL").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("SparkSQL2")
				.config(conf).getOrCreate();

		showStockAnalysis(sc, spark);
		spark.stop();
		sc.close();
	}

	private static void showStockAnalysis(JavaSparkContext sc,
			SparkSession spark) throws IOException {

		JavaRDD<Stock> stockRDD = sc.parallelize(new HBaseReader()
				.GetStockAnalysis());

		String schemaString = "key stock_analysis keyword";

		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName,
					DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<Row> rowRDD = stockRDD.map((Function<Stock, Row>) record -> {
			if (record.key != null) {
				return RowFactory.create(
						record.key,
						record.GetStatement().isEmpty() ? "NonListed" : record
								.GetStatement(), record.GetFoundKeywords());
			}
			return null;
		});

		Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
		dataFrame.createOrReplaceTempView("stocks");

		Dataset<Row> stockResult = spark
				.sql("SELECT key,stock_analysis as stock_type,keyword FROM stocks WHERE key != 'NULL'");
		stockResult.show(50);

		Dataset<Row> stockCount = spark
				.sql("SELECT stock_analysis as stock_type,count(*) as count FROM stocks group by stock_analysis");
		stockCount.show(50);

		stockCount.write().mode("append").option("header", "true")
				.csv("hdfs://localhost/user/cloudera/SparkTableResult");
		stockCount.write().mode("append").option("header", "true")
				.csv("hdfs://localhost/user/cloudera/SparkTableCount");

	}
}
