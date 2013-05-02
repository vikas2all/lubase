package com.xjtudlc.idc.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
	static class SequenceFileMapper extends MapReduceBase implements Mapper<NullWritable, BytesWritable, Text, Text>  {
	private JobConf conf;
	Text fileName = new Text();
	Text Content  = new Text();
	
	public void configure(JobConf conf) {
	this.conf = conf;
	}
	public void map(NullWritable key, BytesWritable value, OutputCollector<Text,Text> output, Reporter reporter)throws IOException {
	String filename = conf.get("map.input.file");
	filename = filename.substring(filename.lastIndexOf("/")+1,filename.length()-4);
	StringBuffer name = new StringBuffer(filename);
//	byte content[] = value.getBytes();
//       // ByteToCharConverter convert = ByteToCharConverter.getDefault();  
//    String con = Bytes.toString(content);
    fileName.set(name.append("@@").toString());
    String cc = new String(value.getBytes()).replaceAll("\n", " ");
    System.out.println(cc+"###"+cc.length());
    StringBuffer str = new StringBuffer();
    for(int i=0;i<cc.length();i++){
    	if((cc.charAt(i))!='\0'){
    		str.append(cc.charAt(i)+"");
    	}
    }
    Content.set(str.toString());
   // System.out.println(new String(value.getBytes()).replaceAll("\n", " ")+"@@@@@");
    //System.out.println(Content.getLength()+"%%%%"+new String(Content.getBytes())+"@@@@@"+new String(value.getBytes()).replaceAll("\n", " ")+"####"+new String(Content.getBytes()).length());
	output.collect(fileName, Content);
	}
	}


	@Override
	public int run(String[] args) throws IOException {
    JobConf conf = new JobConf(getConf(), SmallFilesToSequenceFileConverter.class);
    if (args.length < 2) {
        System.out.println("SmallFilesToSequenceFileConverter <input-dir> <output-dir>  [mappers] [reducers]");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    int mappers = (args.length > 2) ? Integer.parseInt(args[2]) : 10;
    int reducers = (args.length > 3) ? Integer.parseInt(args[3]) : 1;
    
    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, outputPath);
	conf.setInputFormat(WholeFileInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapperClass(SequenceFileMapper.class);
	conf.setReducerClass(IdentityReducer.class);
	conf.setNumMapTasks(mappers);
    conf.setNumReduceTasks(reducers);
	JobClient.runJob(conf);
	return 0;
	}
	public static void main(String[] args) throws Exception {
	int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
	System.exit(exitCode);
	}
	}
