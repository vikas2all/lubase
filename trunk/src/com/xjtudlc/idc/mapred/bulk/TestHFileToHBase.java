package com.xjtudlc.idc.mapred.bulk;

import java.io.IOException;  
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;  
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;  
import org.apache.hadoop.hbase.util.Bytes;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.xjtudlc.idc.index.HBaseIndexWriter;
import com.xjtudlc.idc.index.analyzer.BaseAnalyzer;
  
public class TestHFileToHBase {
	public static int documentId = -1;
	public static final byte[] FAMILY = Bytes.toBytes("fm");
  
    public static  class TestHFileToHBaseMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue> {  
  
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
            String[] values = value.toString().split("@@", 2);  
//            byte[] row = Bytes.toBytes(values[0]);  
//            ImmutableBytesWritable k = new ImmutableBytesWritable(row);  
//            KeyValue kvProtocol = new KeyValue(row, "fm".getBytes(), "qulifier".getBytes(), values[1].getBytes());  
//            context.write(k, kvProtocol);  
            String filename = values[0];
            String content = values[1];
            Map<String, List<Integer>> termVector = new HashMap<String, List<Integer>>();
    	    Map<String, byte[]> fieldsToStore = new HashMap<String, byte[]>();
    		Document doc = new Document();
    		doc.add(new Field("FileName", filename, Field.Store.YES,Field.Index.NOT_ANALYZED));
    		doc.add(new Field("Content", content, Field.Store.YES,Field.Index.ANALYZED));
    		HBaseIndexWriter writer = new HBaseIndexWriter();
    		writer.add(doc, new BaseAnalyzer(), termVector, fieldsToStore);
    		/**
			 * Store Field
			 */
			documentId ++;
			byte[] row = Bytes.toBytes(documentId);
			for(Map.Entry<String, byte[]> entry : fieldsToStore.entrySet()){
				//System.out.println(entry.getKey());
//				put.add(FAMILY,Bytes.toBytes(entry.getKey()),entry.getValue());
				KeyValue kv = new KeyValue(row, FAMILY,Bytes.toBytes(entry.getKey()),entry.getValue());
				context.write( new ImmutableBytesWritable(row), kv);
			}
			
			/**
			 * Store Term Position
			 */
			byte[] docSet = null;
			final Map<String,Map<Integer,List<Integer>>> termPosition = new HashMap<String,Map<Integer,List<Integer>>>();
			addTermPosition(documentId,termVector,termPosition);
			for(Map.Entry<String, Map<Integer,List<Integer>>> entry : termPosition.entrySet()){
//				put = new Put(entry.getKey().getBytes());
				row = Bytes.toBytes(entry.getKey());
				for(Map.Entry<Integer, List<Integer>> entry2 : entry.getValue().entrySet()){
					//System.out.println(entry.getValue().size()+"###");
					List<Integer> list = entry2.getValue();
					byte[] out = new byte[list.size() * Bytes.SIZEOF_INT];
				    for (int i = 0; i < list.size(); ++i) {
				       Bytes.putInt(out, i * Bytes.SIZEOF_INT, list.get(i).intValue());
				    }
				    docSet = Bytes.add(Bytes.toBytes(list.size()), out);
//					put.add(FAMILY, Bytes.toBytes(entry2.getKey()), docSet);
				    KeyValue kv = new KeyValue(row,FAMILY, Bytes.toBytes(entry2.getKey()), docSet);
				    context.write( new ImmutableBytesWritable(row), kv);
				}
				
			}
        }  
  
    }  
    
    /**
	 * TermVector to TermPosition
	 */
	static void addTermPosition(int docId,Map<String,List<Integer>> termVector,Map<String,Map<Integer,List<Integer>>> termPosition)
	{
		for(Map.Entry<String, List<Integer>> entry : termVector.entrySet()){
			//this.termPosition.put(entry.getKey(), value)
			Map<Integer,List<Integer>> existingFrequencies = termPosition.get(entry.getKey());
			if(existingFrequencies == null){
				existingFrequencies = new HashMap<Integer,List<Integer>>();
				existingFrequencies.put(docId, entry.getValue());
				termPosition.put(entry.getKey(), existingFrequencies);
			}else{
				existingFrequencies.put(docId, entry.getValue());
			}
		}
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
			//region Ô¤·ÖÇø
			/**
			 * @link http://hbase.apache.org/book.html#precreate.regions
			 */
			byte[] startkey = Bytes.toBytes(0);
			byte[] endkey = Bytes.toBytes("docNum");
			int regionNum = 2;
			HTableDescriptor descriptor = new HTableDescriptor(tableName);
			descriptor.addFamily(new HColumnDescriptor("fm".getBytes()));
			//descriptor.addFamily(new HColumnDescriptor("score"));
			//admin.createTable(descriptor);
			admin.createTable(descriptor, startkey, endkey, regionNum);
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
  
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {  
        Configuration conf = HBaseConfiguration.create();  
        conf.set("hbase.master", "hadoop5:60000");
		conf.set("hbase.zookeeper.quorum", "hadoop6,hadoop7,hadoop8");
		createTable("song",conf);
        Job job = new Job(conf, "TestHFileToHBase");  
        job.setJarByClass(TestHFileToHBase.class);  
  
        job.setOutputKeyClass(ImmutableBytesWritable.class);  
        job.setOutputValueClass(KeyValue.class);  
  
        job.setMapperClass(TestHFileToHBaseMapper.class);  
        job.setReducerClass(KeyValueSortReducer.class);  
//      job.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.HFileOutputFormat.class);  
        job.setOutputFormatClass(HFileOutputFormat.class);  
        
        // job.setPartitionerClass(org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner.class);  
  
        // HBaseAdmin admin = new HBaseAdmin(conf);  
       
         HTable table = new HTable(conf, "song");  
  
         HFileOutputFormat.configureIncrementalLoad(job, table);  
         //job.setNumReduceTasks(4); 
  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
  
}  
