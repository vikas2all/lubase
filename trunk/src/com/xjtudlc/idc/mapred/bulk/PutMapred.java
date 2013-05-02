package com.xjtudlc.idc.mapred.bulk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PutMapred {
	public static Job configureJob(Configuration conf, String [] args)
	  throws IOException {
	    String inputPath = args[0];

	    //conf.set("index.familyname", "attributes");
     
	    Job job = new Job(conf,"index");
	    job.setJarByClass(PutMapred.class);
	    job.setMapperClass(PutMapper.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.setInputPaths(job, inputPath);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(MultiTableOutputFormat.class);
	   
	    return job;
	  }
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = HBaseConfiguration.create();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if(otherArgs.length < 1) {
	      System.err.println("Only " + otherArgs.length + " arguments supplied, required: 1");
	      System.err.println("Usage: PutMapred <input_dir>");
	      System.exit(-1);
	    }
	    long start = System.currentTimeMillis();
	    Job job = configureJob(conf, otherArgs);
	    boolean flag = job.waitForCompletion(true);
	    long end = System.currentTimeMillis();
	    System.out.println("*********"+(end-start)+"**********");
	    System.exit(flag ? 0 : 1);
	  }
	
	

}
