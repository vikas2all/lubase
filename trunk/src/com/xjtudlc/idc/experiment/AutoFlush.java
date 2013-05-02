package com.xjtudlc.idc.experiment;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class AutoFlush {
	
	 Logger logger  =  Logger.getLogger(AutoFlush.class);
	
	public Configuration config;
	
	public AutoFlush()
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
	
    public void dropTable(String tablename) throws IOException{  
        HBaseAdmin admin = new HBaseAdmin(config);  
        admin.disableTable(tablename);  
        admin.deleteTable(tablename);  
        //System.out.println("drop table ok.");  
    }  
	
	public void writeAutoFlush(String tableName) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		long startTime = System.currentTimeMillis();
		for(int i=0;i<100000;i++){
			Put put = new Put(Bytes.toBytes(i));
			put.add(Bytes.toBytes("document"), "test".getBytes(), Bytes.toBytes(i+""));
			table.put(put);
		}
		long endTime = System.currentTimeMillis();
		//System.out.println("AutoFlush Time: "+(endTime-startTime)+"ms");
		logger.error(endTime-startTime);
		table.close();
		pool.close();
	}
	
	public void writeToLocalFS(String file) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(file)));
		for(int i=0;i<10000;i++){
			bw.write(i+" "+i);
			bw.newLine();
		}
		bw.flush();
		bw.close();
	}
	
	public void writeWithoutAutoFlush(String tableName) throws IOException
	{
		HTablePool pool = new HTablePool(config,2000);
		HTable table = (HTable) pool.getTable(tableName);
		List<Put> list = new ArrayList<Put>();
		table.setAutoFlush(false);
		long startTime = System.currentTimeMillis();
		for(int i=0;i<500000;i++){
			Put put = new Put(Bytes.toBytes(i));
			put.add(Bytes.toBytes("document"), Bytes.toBytes("test"), Bytes.toBytes(i+""));
			list.add(put);
			if(list.size()>10000){
				table.put(list);
				table.flushCommits();
				list.clear();
			}
		}
		//table.getWriteBuffer().addAll(list);
		//System.out.println(list.size()+"####");
		table.put(list);
		table.flushCommits();
		long endTime = System.currentTimeMillis();
		System.out.println("Without AutoFlush Time: "+(endTime-startTime)+"ms");
		logger.error(endTime-startTime);
		list.clear();
		table.close();
		pool.close();
	}
	
	public static void main(String args[]) throws IOException
	{
//		AutoFlush ex = new AutoFlush();
//		String tableName = "experiment";
//		ex.writeToLocalFS("E:\\Lubase\\"+tableName+"_10000");
//		for(int i=0;i<1;i++){
//			ex.createTable(tableName);
//			try {
//				//ex.writeAutoFlush(tableName);
//				ex.writeWithoutAutoFlush(tableName);
//				//ex.dropTable(tableName);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		int N = 2;
		int L1 = 2;
		byte[] rowKey = new byte[L1];

        byte[] value = null;

        Put put = new Put(rowKey);

        for (int i = 0; i < N; i++) {

                 put.add(Bytes.toBytes("c"), Bytes.toBytes("cs"), value);

        }

        System.out.println("Put Size:"+ (put.heapSize()) + " bytes");
	}

}
