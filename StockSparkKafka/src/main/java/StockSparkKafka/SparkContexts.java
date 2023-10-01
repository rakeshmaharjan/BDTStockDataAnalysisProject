package StockSparkKafka;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkContexts {
	static int counter = 1;
	private JavaSparkContext sc;
	private HBaseManager db;
	private HashMap<String, String> mapKeywords;

	public SparkContexts() throws IOException {
		this.sc = new JavaSparkContext("local[*]", "SparkStock", new SparkConf());
		this.db = new HBaseManager();
		this.mapKeywords = this.db.GetKeywords();
	}

	public void Process(String key, List<String> l) throws IOException {
		JavaRDD<String> list = this.sc.parallelize(l)
				.flatMap(line -> Arrays.asList(line.toUpperCase()))
				.filter(line -> !line.isEmpty());

		System.out.println("list---> " + list.first());
		System.out.println("mapKeywords-----> " + this.mapKeywords);

		JavaPairRDD<String, Stock> stockResults = list.mapToPair(
				new PairHandler(key, this.mapKeywords)).sortByKey();

		System.out.println("key---->" + key);
		System.out.println("stockResults ---->" + stockResults.values());
		this.db.SaveStockAnalysis(key, stockResults.values());
	}

	static class PairHandler implements PairFunction<String, String, Stock>,
			Serializable {
		private String key;
		private HashMap<String, String> map;

		public PairHandler(String k, HashMap<String, String> m) {
			this.key = k;
			this.map = m;
		}

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Stock> call(String _line) throws Exception {
			// System.out.println("_line----> " + _line);
			String line = _line.toLowerCase();
			// System.out.println("line---> " + line);
			Stock stock = new Stock();

			for (String x : this.map.keySet()) {
				// System.out.println("this.map.get(x))----> " +
				for (String s : this.map.get(x).split(",")) {
					if (line.contains(s.toLowerCase())) {
						stock.keywordList.add(new Tuple2<String, String>(x, s));
					}
				}
			}

			// System.out.println("stock.keyword----> " + stock.keyword);
			if (stock.keywordList.isEmpty()) {
				stock.keywordList.add(new Tuple2<String, String>("NonListed",
						""));
			}
			return new Tuple2<String, Stock>(this.key, stock);
		}
	}
}
