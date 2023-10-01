package StockSparkKafka;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;

public class HBaseManager {
	private Configuration hbaseConfig;
	private int rowkeyAnalysis = 0;
	private final String TABLE_NAME = "tbl_stock_analysis";
	int count = 0;

	public HBaseManager() throws IOException {
		this.hbaseConfig = HBaseConfiguration.create();
		this.DefaultValues();
		this.rowkeyAnalysis = this.GetMaxRownum();
	}

	private void DefaultValues() throws IOException {
		try (Connection connection = ConnectionFactory
				.createConnection(this.hbaseConfig);
				Admin admin = connection.getAdmin()) {

			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf("tbl_StockKeywords"));

			table.addFamily(new HColumnDescriptor("type_family")
					.setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor("keywords_family"));

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			if (!admin.tableExists(table.getTableName())) {
				admin.createTable(table);
				Table tbl = connection.getTable(TableName
						.valueOf("tbl_StockKeywords"));
				Put put1 = new Put(Bytes.toBytes("1"));
				put1.addColumn(Bytes.toBytes("type_family"),
						Bytes.toBytes("type"), Bytes.toBytes("HSI"));
				put1.addColumn(Bytes.toBytes("keywords_family"),
						Bytes.toBytes("keywords"), Bytes.toBytes("HSI"));
				tbl.put(put1);

				Put put2 = new Put(Bytes.toBytes("2"));
				put2.addColumn(Bytes.toBytes("type_family"),
						Bytes.toBytes("type"), Bytes.toBytes("NYA"));
				put2.addColumn(Bytes.toBytes("keywords_family"),
						Bytes.toBytes("keywords"), Bytes.toBytes("NYA"));
				tbl.put(put2);

				Put put3 = new Put(Bytes.toBytes("3"));
				put3.addColumn(Bytes.toBytes("type_family"),
						Bytes.toBytes("type"), Bytes.toBytes("IXIC"));
				put3.addColumn(Bytes.toBytes("keywords_family"),
						Bytes.toBytes("keywords"), Bytes.toBytes("IXIC"));
				tbl.put(put3);

				tbl.close();
			}
		}
	}

	public HashMap<String, String> GetKeywords() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();

		try (Connection connection = ConnectionFactory
				.createConnection(this.hbaseConfig)) {
			Table tbl = connection.getTable(TableName.valueOf("tbl_StockKeywords"));
			Scan scan = new Scan();
			scan.setCacheBlocks(false);
			scan.setCaching(10000);
			scan.setMaxVersions(10);
			ResultScanner scanner = tbl.getScanner(scan);
			for (Result result = scanner.next(); result != null; result = scanner
					.next()) {
				String type = "";
				String keywords = "";
				for (Cell cell : result.rawCells()) {
					String family = Bytes.toString(CellUtil.cloneFamily(cell));
					String column = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					if (family.equalsIgnoreCase("type_family")
							&& column.equalsIgnoreCase("type")) {
						type = Bytes.toString(CellUtil.cloneValue(cell));
					} else if (family.equalsIgnoreCase("keywords_family")
							&& column.equalsIgnoreCase("keywords")) {
						keywords = Bytes.toString(CellUtil.cloneValue(cell));
					}
				}

				if (!map.containsKey(type)) {
					map.put(type, keywords);
				} else {
					map.replace(type, map.get(type) + "," + keywords);
				}
			}
		}

		return map;
	}

	public void SaveStockAnalysis(String key, JavaRDD<Stock> rdd)
			throws IOException {

		try (Connection connection = ConnectionFactory
				.createConnection(this.hbaseConfig);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor("key_family")
					.setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor("stock_family"));
			
			if (!admin.tableExists(table.getTableName())) {
				admin.createTable(table);
			}

			Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));
			System.out.println("rdd----> " + rdd.collect());

			// int count=0;
			for (Stock tw : rdd.collect()) {
				Put put = new Put(Bytes.toBytes(String
						.valueOf(++this.rowkeyAnalysis)));
				put.addColumn(Bytes.toBytes("key_family"), Bytes.toBytes("key"),
						Bytes.toBytes(key));
//				put.addColumn(Bytes.toBytes("key_family"), Bytes.toBytes("user"),
//						Bytes.toBytes(tw.user));
				put.addColumn(Bytes.toBytes("stock_family"),
						Bytes.toBytes("stock_analysis"),
						Bytes.toBytes(tw.GetStatement()));
				put.addColumn(Bytes.toBytes("stock_family"),
						Bytes.toBytes("keyword"),
						Bytes.toBytes(tw.GetKeywordsFound()));
				tbl.put(put);
				count++;
			}
			tbl.close();

			System.out
					.println("tbl_stock_analysis written rows count: " + count);
		}
	}

	@SuppressWarnings({ "finally", "deprecation" })
	private int GetMaxRownum() {
		try {
			@SuppressWarnings("resource")
			Result result = new HTable(this.hbaseConfig, TABLE_NAME)
					.getRowOrBefore(Bytes.toBytes("9999"), Bytes.toBytes(""));
			return Integer.parseInt(Bytes.toString(result.getRow()));
		} catch (Exception ex) {
		} finally {
			return 0;
		}
	}

}
