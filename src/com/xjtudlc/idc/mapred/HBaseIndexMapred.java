package com.xjtudlc.idc.mapred;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class HBaseIndexMapred {
	
	public static final byte[] FAMILY = Bytes.toBytes("fm");
	public static final String tableName = "index";
	
	public static Job configureJob(Configuration conf, String [] args)
	  throws IOException, URISyntaxException {
	    String inputPath = args[0];
	    int rnum = Integer.parseInt(args[1]);
	    Job job = new Job(conf,"index");
	    job.setJarByClass(HBaseIndexMapred.class);
	    job.setMapperClass(HBaseIndexMapper.class);
	    job.setNumReduceTasks(rnum);
	    job.setReducerClass(HBaseIndexReducer.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.setInputPaths(job, inputPath);
	    job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
//	    DistributedCache.addCacheFile(new Path(args[2]).toUri(),job.getConfiguration());//Attention!! Must use job.getConfiguration not Configuration conf.
//	    DistributedCache.addCacheFile(new Path(args[3]).toUri(),job.getConfiguration());
	    return job;
	  }
	
	public static void createTable(String tableName, Configuration config)
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
			descriptor.addFamily(new HColumnDescriptor(FAMILY));
			//descriptor.addFamily(new HColumnDescriptor("score"));
			admin.createTable(descriptor);
			admin.close();
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
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = HBaseConfiguration.create();
	    conf.set("hbase.master", "hadoop5:60000");
		conf.set("hbase.zookeeper.quorum", "hadoop6,hadoop7,hadoop8");
		
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if(otherArgs.length < 2) {
	      System.err.println("Only " + otherArgs.length + " arguments supplied, required: 3");
	      System.err.println("Usage: HBaseIndexMapred <input_dir> <reduce_num>");
	      System.exit(-1);
	    }
	    createTable(tableName,conf);
	    long start = System.currentTimeMillis();
	    Job job = configureJob(conf, otherArgs);
	    TableMapReduceUtil.initTableReducerJob(tableName, HBaseIndexReducer.class, job);
	    boolean flag = job.waitForCompletion(true);
	    long end = System.currentTimeMillis();
	    System.out.println("*********"+(end-start)+"**********");
	    System.exit(flag ? 0 : 1);
     }

}
