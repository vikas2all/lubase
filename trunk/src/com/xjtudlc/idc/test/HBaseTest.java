package com.xjtudlc.idc.test;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.document.Document;

public class HBaseTest {
	
public Configuration config;
	
	public HBaseTest()
	{
		config = HBaseConfiguration.create();
		config.set("hbase.master", "xjtudlClient:60000");
		config.set("hbase.zookeeper.quorum", "xjtudlClient");
	}
	
	/*
	 * This test show me that when insert data into table if the rowKey is the same table will update, not insert.
	 */
	public void writeWithoutAutoFlush(String tableName) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
//		List<Put> list = new ArrayList<Put>();
//		table.setAutoFlush(false);
//		long startTime = System.currentTimeMillis();
//	    Put put = new Put(Bytes.toBytes(1));
//		put.add(Bytes.toBytes("document"), "test".getBytes(), Bytes.toBytes(1+""));
//		
//		list.add(put);
//		Put put1 = new Put(Bytes.toBytes(1));
//		put1.add(Bytes.toBytes("document"), "test".getBytes(), Bytes.toBytes(2+""));
//		list.add(put1);
//		//table.getWriteBuffer().addAll(list);
//		//System.out.println(list.size()+"####");
//		table.put(list);
//		table.flushCommits();
//		long endTime = System.currentTimeMillis();
//		System.out.println("Without AutoFlush Time: "+(endTime-startTime)+"ms");
//		list.clear();
		
		//ResultScanner scan = table.getScanner(family, qualifier)
		table.close();
		pool.close();
	}
	
	public void getRowOrBeforeTest() throws IOException
	{
		String tableName = "index";
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		
		Result r = table.getRowOrBefore(Bytes.toBytes("/0"), Bytes.toBytes("document"));
		System.out.println(r);
		
	}
	
	public void scaneByPrefixFilter(String tablename, String rowPrifix) {//change it to load forward.
		   try {
				HTablePool pool = new HTablePool(config,2000);
				HTable table = (HTable) pool.getTable(tablename);
		        Scan s = new Scan();
		        s.setCaching(10000);
		        s.setFilter(new PrefixFilter(rowPrifix.getBytes()));
		        s.addColumn("fm".getBytes(), Bytes.toBytes(0));
		        ResultScanner rs = table.getScanner(s);
		        for (Result r : rs) {
		          KeyValue[] kv = r.raw();
		          for (int i = 0; i < kv.length; i++) {
		              System.out.print(new String(kv[i].getRow()) + "  ");
		              System.out.println(Bytes.toInt(kv[i].getValue()));//Don't need to encode, it use the first 4 to convert to Int.
		          }
		       }
		   } catch (IOException e) {
		         e.printStackTrace();
		   }
		}
	
	public void getRegionInfo() throws IOException
	{
		String tableName = "experiment";
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		NavigableMap<HRegionInfo,ServerName> map = table.getRegionLocations();
		System.out.println(map.firstKey().getRegionNameAsString());
		map.firstKey().setSplit(true);
		System.out.println(map.firstKey().getRegionNameAsString());
		
//		table = (HTable) pool.getTable("index");
//		map = table.getRegionLocations();
//		System.out.println(map.firstKey().getRegionNameAsString());
	}
	
	/*
	 * Get docNum of table
	 */
	public int getDocNum() throws IOException
	{
		String tableName = "test";
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		Get get = new Get(Bytes.toBytes("docNum"));
		Result r = table.get(get);
		System.out.println(Bytes.toInt(r.getValue(Bytes.toBytes("fm"), Bytes.toBytes("total"))));
		return Bytes.toInt(r.getValue(Bytes.toBytes("fm"), Bytes.toBytes("total")));
	}
	
	/*
	 * 
	 */
	public void del() throws IOException
	{
		String tableName = "test";
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable)pool.getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes("/0/hello"));
		delete = delete.deleteColumn("fm.vc".getBytes(), "test".getBytes());
		table.delete(delete);
		
		table.close();
	}
	
	public void put() throws IOException
	{
		String tableName = "test";
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable)pool.getTable(tableName);
		Put put = new Put(Bytes.toBytes(1));
		put.add(Bytes.toBytes("document"), "test".getBytes(), Bytes.toBytes(1+""));
		table.put(put);
		table.close();
	}
	
	public void get() throws IOException{
		String tableName = "test";
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable)pool.getTable(tableName);
//		Get t = new Get();
//		t.addColumn("fm".getBytes(), Bytes.toBytes(0));
//		Result r = table.get(t);
//		for(KeyValue kv : r.raw())
//		{
//			System.out.println(Bytes.toString(kv.getQualifier())+Bytes.toString(kv.getValue()));
//		}
		Scan scan = new Scan();
		scan.addColumn("fm".getBytes(), Bytes.toBytes(0));
		ResultScanner rs = table.getScanner(scan);
		for(Result r : rs)
		{
			System.out.println("Row key: " + new String(r.getRow()));
			for(KeyValue kv : r.raw())
			{
				System.out.println("Family:" + new String(kv.getFamily())+" Column:"+new String(kv.getQualifier())+" Value:"+new String(kv.getValue())+" Timestamp:"+ kv.getTimestamp());
			}
		}
	}
	
	public static void main(String args[]) throws IOException
	{
//		HBaseTest test = new HBaseTest();
//		//test.writeWithoutAutoFlush("test");
//		//test.getRowOrBeforeTest();
//		//test.getRegionInfo();
//		long s1 = System.currentTimeMillis();
//		test.scaneByPrefixFilter("index", "/Content/");
//		long s2 = System.currentTimeMillis();
//		System.out.println((s2-s1)+"ms");
		System.out.println(Bytes.SIZEOF_CHAR);
	}

}
