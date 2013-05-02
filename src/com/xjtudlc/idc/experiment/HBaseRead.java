package com.xjtudlc.idc.experiment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRead {
	
public Configuration config;
	
	public HBaseRead()
	{
		config = HBaseConfiguration.create();
		config.set("hbase.master", "xjtudlClient:60000");
		config.set("hbase.zookeeper.quorum", "xjtudlClient");
	}
	
	public void createTable(String tableName)
	{
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if(admin.tableExists(tableName))
			{
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName+" is exist, delete...");
			}
			HTableDescriptor descriptor = new HTableDescriptor(tableName);
			descriptor.addFamily(new HColumnDescriptor("document"));
			//descriptor.addFamily(new HColumnDescriptor("score"));
			admin.createTable(descriptor);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void get(int id ,HTable table) throws IOException
	{
		
		//table.clearRegionCache();
		Get get = new Get(Bytes.toBytes(id));
		get.addFamily(Bytes.toBytes("fm"));
		
		Result r = table.get(get);
		//System.out.println(r.size());
		
	}
	
	public void scan(String tableName) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		
		Scan scan = new Scan();
		long startTime = System.currentTimeMillis();
		ResultScanner ss = table.getScanner(scan);
		ss.next();
		long endTime = System.currentTimeMillis();
		System.out.println((endTime-startTime)+"ms");
		
	}
	
	public static void main(String args[]) throws IOException
	{
		HBaseRead read = new HBaseRead();
		String tableName = "test";
		HTablePool pool = new HTablePool(read.config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		long startTime = System.currentTimeMillis();
		for(int i=0;i<1000;i++){
			//int tmp = i*10000;
			//read.get(1, "test");
			read.get(2, table);
			read.get(1, table);
			//read.scan("experiment");
		}
		long endTime = System.currentTimeMillis();
		System.out.println((endTime-startTime)+"ms");
		
	}

}
