package ub.bigdata.m2gl;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HBaseLink {

	public static class HBaseProg extends Configured implements Tool {
		
		protected static TableName TABLE_NAME = TableName.valueOf("PascalTestTiles");
		private static Connection connection;
		private static Table table;

		public static byte[] get(String row) {
			try {
				//table = connection.getTable(TABLE_NAME);
				Get g = new Get(row.getBytes());
				Result r;
				r = table.get(g);
				byte[] value = r.getValue("File".getBytes(), "Tile".getBytes());
				return value;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		public int run(String[] args) throws IOException {
			if (connection == null)
				connection = ConnectionFactory.createConnection(getConf());
			if (table == null)
				table = connection.getTable(TABLE_NAME);
			return 0;
		}

	}

}


