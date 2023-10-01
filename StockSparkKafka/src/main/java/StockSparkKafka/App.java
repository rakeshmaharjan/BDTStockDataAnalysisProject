package StockSparkKafka;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
public class App {

	static String lastKey = "";
	static List<String> list = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
		
		SparkContexts spark = new SparkContexts();
		
		StockKafkaConsumer stockKafkaConsumer = new StockKafkaConsumer();
		stockKafkaConsumer.Wait(() -> {
			try {
				Thread.sleep(3000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;
		}, (String key, String val, boolean new_key) -> {
			if (lastKey.isEmpty())
				lastKey = key;
			if (new_key) {
				try {
					if (!list.isEmpty())
						spark.Process(lastKey, list);
				} catch (Exception e) {
					e.printStackTrace();
				}

				lastKey = key;
				list.clear();
			}
				list.add(key);
			});
	}
}