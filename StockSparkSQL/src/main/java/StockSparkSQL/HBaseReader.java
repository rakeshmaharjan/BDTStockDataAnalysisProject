package StockSparkSQL;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseReader {
	private Configuration hbaseConfig;
	private final String TABLE_NAME = "tbl_stock_analysis";

	public HBaseReader() {
		this.hbaseConfig = HBaseConfiguration.create();
	}

	public List<Stock> GetStockAnalysis() throws IOException {
		List<Stock> stockList = new ArrayList<Stock>();

		try (Connection connection = ConnectionFactory
				.createConnection(this.hbaseConfig)) {
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			Scan scan = new Scan();
			scan.setCacheBlocks(false);
			scan.setCaching(10000);
			scan.setMaxVersions(10);
			ResultScanner scanner = table.getScanner(scan);
			for (Result result = scanner.next(); result != null; result = scanner
					.next()) {
				Stock stock = new Stock();
				for (Cell cell : result.rawCells()) {
					String family = Bytes.toString(CellUtil.cloneFamily(cell));
					String column = Bytes.toString(CellUtil
							.cloneQualifier(cell));
					if (family.equalsIgnoreCase("key_family")) {
						if (column.equalsIgnoreCase("key"))
							stock.key = Bytes.toString(CellUtil
									.cloneValue(cell));
					} 
					else if (family.equalsIgnoreCase("stock_family")) {
						if (column.equalsIgnoreCase("stock_analysis")) {
							stock.PutStatements(Bytes.toString(CellUtil
									.cloneValue(cell)));
						} else if (column.equalsIgnoreCase("keyword"))
							stock.PutKeywords(Bytes.toString(CellUtil
									.cloneValue(cell)));
					}
				}

				if (!stock.key.isEmpty())
					stockList.add(stock);
			}
		}

		return stockList;
	}
}
