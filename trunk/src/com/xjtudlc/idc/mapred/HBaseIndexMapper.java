package com.xjtudlc.idc.mapred;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HBaseIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	public Set<String> vocabulary  = new HashSet<String>();
	public static int documentId = -1;
	Text filename = new Text();
	Text content = new Text();
	
	/**
	 * Map function.
	 */
	public void map(LongWritable key, Text value, Context context){
		String str = value.toString();
		int t = str.indexOf("@@",2);
		if(t<0){
			filename.set(value);
			content.set(value);
		}else{
			String fileName = str.substring(0,t);
			str = str.substring(t+2,str.length());
			documentId ++;
			filename.set(fileName+"@@"+documentId);
			content.set(str);
		}
		try {
			context.write(filename, content);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		InputSplit split = context.getInputSplit();
//		String fileName = ((FileSplit)split).getPath().getName();
	}

}
