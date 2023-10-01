package main;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class StockKafkaProducer {

	public static Log logger = LogFactory.getLog(StockKafkaProducer.class);

	public static void main(String[] args) {
		try {
			// Create an HttpClient
			HttpClient httpClient = HttpClients.createDefault();

			// Define the URL to fetch JSON data
			String url = "https://raw.githubusercontent.com/riteshmaharjan1/stock-data/main/stockdata.json";

			// Create an HTTP GET request
			HttpGet httpGet = new HttpGet(url);

			// Execute the request and get the response
			String jsonResponse = EntityUtils.toString(httpClient.execute(
					httpGet).getEntity());
			
			// Parse the JSON using Gson's JsonParser
			JsonElement jsonElement = JsonParser.parseString(jsonResponse);
			if (jsonElement.isJsonObject()) {
				JsonObject jsonObject = jsonElement.getAsJsonObject();
				if (jsonObject.has("data")
						&& jsonObject.get("data").isJsonArray()) {
					JsonArray dataArray = jsonObject.getAsJsonArray("data");
					for (JsonElement stockElement : dataArray) {
						if (stockElement.isJsonObject()) {
							JsonObject stockObject = stockElement
									.getAsJsonObject();
							String index = stockObject.get("Index")
									.getAsString();
							String high = stockObject.get("High").getAsString();
							KafkaSender.sendStockData(index, high);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
