package com.xjtudlc.idc.experiment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseWrite {
	
	public Configuration config;
	
	public HBaseWrite(){
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
			descriptor.addFamily(new HColumnDescriptor("fm"));
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
	
	public void put(String str,String tableName) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		table.setAutoFlush(false);
		List<Put> list = new ArrayList<Put>();
		long startTime = System.currentTimeMillis();
		for(int i=0;i<10000;i++){
			Put put = new Put(Bytes.toBytes(1));
			put.add(Bytes.toBytes("fm"), Bytes.toBytes(i), Bytes.toBytes(str+i));
			list.add(put);
			if(list.size()>100000){
				table.put(list);
				table.flushCommits();
				list.clear();
			}
		}
		table.put(list);
		table.flushCommits();
		long endTime = System.currentTimeMillis();
		System.out.println((endTime-startTime)+"ms");
		
	}
	
	public void delete(String tableName) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		table.setAutoFlush(false);
		for(int i=0;i<10000;i++){
			Delete delete = new Delete(Bytes.toBytes(i));
			table.delete(delete);
		}
	}
	
	public void put(String str, String tableName1,String tableName2) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table1 = (HTable) pool.getTable(tableName1);
		table1.setAutoFlush(false);
		HTable table2 = (HTable) pool.getTable(tableName1);
		table2.setAutoFlush(false);
		List<Put> list = new ArrayList<Put>();
		long startTime = System.currentTimeMillis();
		for(int i=0;i<50000;i++){
			Put put = new Put(Bytes.toBytes(1));
			put.add(Bytes.toBytes("fm"), Bytes.toBytes("value"), Bytes.toBytes(str+i));
			list.add(put);
			if(list.size()>100000){
				table1.put(list);
				table1.flushCommits();
				list.clear();
			}
		}
		table1.put(list);
		table1.flushCommits();
		list.clear();
		for(int i=0;i<50000;i++){
			Put put = new Put(Bytes.toBytes(1));
			put.add(Bytes.toBytes("fm"), Bytes.toBytes("value"), Bytes.toBytes(str+i));
			list.add(put);
			if(list.size()>100000){
				table2.put(list);
				table2.flushCommits();
				list.clear();
			}
		}
		table2.put(list);
		table2.flushCommits();
		list.clear();
		long endTime = System.currentTimeMillis();
		System.out.println((endTime-startTime)+"ms");
	}
	
	public void get(int id ,String tableName) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		Get get = new Get(Bytes.toBytes(id));
		//get.addColumn(Bytes.toBytes("document"), Bytes.toBytes(50000));
		long startTime = System.currentTimeMillis();
		Result r = table.get(get);
		System.out.println(r.size());
		long endTime = System.currentTimeMillis();
		System.out.println(id+": "+(endTime-startTime)+"ms");
	}
	
	public void update(String str,String tableName) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		table.setAutoFlush(false);
		List<Put> list = new ArrayList<Put>();
		long startTime = System.currentTimeMillis();
		Get get = new Get(Bytes.toBytes(1));
		Result r = table.get(get);
		
		for(int i=0;i<10000;i++){
			
			if(r.isEmpty()){
				Put put = new Put(Bytes.toBytes(1));
				put.add(Bytes.toBytes("fm"), Bytes.toBytes(i), Bytes.toBytes(str+i));
				list.add(put);
			}else{
				if((str+i).equals(Bytes.toString(r.getValue(Bytes.toBytes("fm"), Bytes.toBytes(i))))){
					//System.out.println("@@");
					continue;
				}else{
					Put put = new Put(Bytes.toBytes(1));
					put.add(Bytes.toBytes("fm"), Bytes.toBytes(i), Bytes.toBytes(str+i));
					list.add(put);
				}
			}
			
		}
		table.put(list);
		table.flushCommits();
		long endTime = System.currentTimeMillis();
		System.out.println((endTime-startTime)+"ms");
	}
	
	public static void main(String args[]) throws IOException
	{
		HBaseWrite hw = new HBaseWrite();
		String tableName1 = "write1";
		//String tableName2 = "write2";
		hw.createTable(tableName1);
		//hw.createTable(tableName2);
		String data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaassssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss";
		hw.put(data, tableName1);
		hw.put(data, tableName1);
		hw.update(data, tableName1);
		//hw.get(1, tableName1);
//		data = "ssssssssssaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaassssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss";
//		hw.put(data, tableName);
//		hw.delete(tableName);
	}

}
