package com.xjtudlc.idc.mapred.bulk;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.mortbay.log.Log;

public class PutMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
	
	public static final byte[] INDEX_COLUMN = Bytes.toBytes("document");
	public static final String tableName = "index";
	public static final byte[] INDEX_q = Bytes.toBytes("test");
	
	public void map(LongWritable key, Text value, Context context){
		String content = value.toString();
		String ss[] = content.split(" ");
		String fileName = ss[0];
		content = ss[1];
		Put put = new Put(fileName.getBytes());
		put.add(INDEX_COLUMN, INDEX_q, content.getBytes());
		try {
			context.write( new ImmutableBytesWritable(Bytes.toBytes(tableName)), put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
