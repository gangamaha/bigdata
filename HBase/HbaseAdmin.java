import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;

public class HbaseAdmin {

	public HbaseAdmin(){

	}

	public void createTable(String tableName, String[] familys)
	throws Exception {
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
	}

	public void listTables () throws IOException{
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor[] tableDescriptor = admin.listTables();

	      // printing all the table names.
	      for (int i=0; i<tableDescriptor.length;i++ ){
	         System.out.println(tableDescriptor[i].getNameAsString());
	      }
	}

	public void deleteAllTables () throws IOException{
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor[] tableDescriptor = admin.listTables();
		String table_name;
	      // printing all the table names.
	      for (int i=0; i<tableDescriptor.length;i++ ){
	    	 table_name=tableDescriptor[i].getNameAsString();
	         System.out.println(table_name);
	         admin.disableTable(table_name);

	         // Deleting emp
	         admin.deleteTable(table_name);
	         System.out.println("Table " + table_name + " deleted");
	      }
	}
	
	public void tableExists(String tablename) throws IOException{
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		boolean result = admin.tableExists(tablename);
		System.out.println("Table " + tablename + " exists ? " + result);
	}

	@SuppressWarnings("deprecation")
	public void addData(String tablename, String familyname, int row, String name, int age) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTable hTable = new HTable(conf, tablename);
		Put p =new Put(Bytes.toBytes(row));
		p.add(null, Bytes.toBytes("name"),Bytes.toBytes(name));
		p.add(null, Bytes.toBytes("age"),Bytes.toBytes(age));
		hTable.put(p);
		System.out.println("Data Inserted");
		hTable.close();
	}

	public void addRecord(String tableName, String rowKey, String family, String qualifier, String string) throws Exception {
		try {
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(string));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getRecord(String tableName, String rowKey){
		try {
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf, tableName);
			Get g = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(g);
			//byte [] value = result.getValue(Bytes.toBytes("course"),Bytes.toBytes("science"));
			System.out.println("reading record " + result);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main (String[] args) throws Exception{


		HbaseAdmin admin = new HbaseAdmin();
		Random rn = new Random();


		String tablename = "scores_test2", student_name;
		int index, marks;
		String[] familys = {"course"};
		String[] subjects = { "science", "chemistry", "math", "art" };

		//Create a table
		admin.createTable(tablename, familys);

		long startTime = System.currentTimeMillis();

		// Add a record
		for (int i = 0; i < 10; i++) {
			index = rn.nextInt(1000)%4;	
			marks = rn.nextInt(1000)%100+1;
			student_name = "test"+rn.nextInt(100000);
			admin.addRecord(tablename, student_name, "course", subjects[index], String.valueOf(marks));
		}

		//other functions based on arguments.
		admin.addRecord(tablename, "rohit", "course", subjects[3], "25");
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("Write time in milli seconds is: "+elapsedTime);

		startTime = System.currentTimeMillis();
		admin.getRecord(tablename, "rohit");
		stopTime = System.currentTimeMillis();
		elapsedTime = stopTime - startTime;
		System.out.println("Read time is: "+elapsedTime);
		
		admin.listTables();
		
		//System.out.println("Deletig all tables..");
		//admin.deleteAllTables();

	}
}