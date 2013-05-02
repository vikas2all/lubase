package com.xjtudlc.idc.mapred;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.xjtudlc.idc.index.HBaseIndexWriter;
import com.xjtudlc.idc.index.analyzer.BaseAnalyzer;
import com.xjtudlc.idc.index.analyzer.PorterStemAnalyzer;
import com.xjtudlc.idc.index.analyzer.StopWordAnalyzer;
import com.xjtudlc.idc.index.analyzer.VocabularyAnalyzer;

public class HBaseIndexReducer  extends TableReducer<Text,Text,ImmutableBytesWritable> {
	private static final Logger log = Logger.getLogger(HBaseIndexMapper.class);
	public static final byte[] FAMILY = Bytes.toBytes("fm");
	public static final String tableName = "index";
	//public Analyzer analyzer = null;
//	public Set<String> vocabulary = new HashSet<String>();
//	public Set<String> stopWords = new HashSet<String>();
	
	/**
	 * Init vocabulary from cache.
	 */
//	public void configure(Configuration conf){
//		try {
//			Path[] paths = DistributedCache.getLocalCacheFiles(conf);
//			if(paths!=null&&paths.length>0){
//				int i = 0;
//				for(Path path : paths){
//					init(path,i);
//					i++;
//				}
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
//	public void init(Path path, int i){
//		try {
//			BufferedReader br = new BufferedReader(new FileReader(path.toString()));
//			String line = "";
//			while((line = br.readLine())!=null){
//				if(i==0)
//				    vocabulary.add(line);
//				else 
//					stopWords.add(line);
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * TermVector to TermPosition
	 */
	void addTermPosition(int docId,Map<String,List<Integer>> termVector,Map<String,Map<Integer,List<Integer>>> termPosition)
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
	
	/**
	 * Reduce Function
	 */
	public void reduce(Text key,Iterable<Text> value,Context context){
		String fileName = key.toString();//fileName
        String str = value.iterator().next().toString();//content
        int t = fileName.indexOf("@@");
        //configure(context.getConfiguration());//cache vocabulary&stop words
        //analyzer = new VocabularyAnalyzer(new PorterStemAnalyzer(new StopWordAnalyzer(new BaseAnalyzer(),stopWords)),vocabulary);
        /**
         * if t<0, there is no @@ in Text key. Store docNum&maxId.
         * @author song
         */
        if(t<0){
        	try {
        		Put put = new Put(Bytes.toBytes("docNum"));
            	put.add(FAMILY, Bytes.toBytes("total"), Bytes.toBytes(Integer.parseInt(str)));
				context.write( new ImmutableBytesWritable(Bytes.toBytes(tableName)), put);
				put = new Put(Bytes.toBytes("maxId"));
				put.add(FAMILY, Bytes.toBytes("id"), Bytes.toBytes(Integer.parseInt(str)-1));
				context.write( new ImmutableBytesWritable(Bytes.toBytes(tableName)), put);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }else{
        	int documentId = Integer.parseInt(fileName.substring(t+2,fileName.length()));
            fileName = fileName.substring(0,t);
            Map<String, List<Integer>> termVector = new HashMap<String, List<Integer>>();
    	    Map<String, byte[]> fieldsToStore = new HashMap<String, byte[]>();
    		Document doc = new Document();
    		doc.add(new Field("FileName", fileName, Field.Store.YES,Field.Index.NOT_ANALYZED));
    		doc.add(new Field("Content", str, Field.Store.YES,Field.Index.ANALYZED));
    		log.error(fileName);
    		HBaseIndexWriter writer = new HBaseIndexWriter();
    		try {
    			writer.add(doc, new BaseAnalyzer(), termVector, fieldsToStore);
    			/**
    			 * Store Field
    			 */
    			//documentId ++;
    			Put put = new Put(Bytes.toBytes(documentId));
    			for(Map.Entry<String, byte[]> entry : fieldsToStore.entrySet()){
    				//System.out.println(entry.getKey());
    				put.add(FAMILY,Bytes.toBytes(entry.getKey()),entry.getValue());
    			}
    			context.write( new ImmutableBytesWritable(Bytes.toBytes(tableName)), put);
    			/**
    			 * Store Term Position
    			 */
    			byte[] docSet = null;
    			final Map<String,Map<Integer,List<Integer>>> termPosition = new HashMap<String,Map<Integer,List<Integer>>>();
    			addTermPosition(documentId,termVector,termPosition);
    			for(Map.Entry<String, Map<Integer,List<Integer>>> entry : termPosition.entrySet()){
    				put = new Put(entry.getKey().getBytes());
    				for(Map.Entry<Integer, List<Integer>> entry2 : entry.getValue().entrySet()){
    					//System.out.println(entry.getValue().size()+"###");
    					List<Integer> list = entry2.getValue();
    					byte[] out = new byte[list.size() * Bytes.SIZEOF_INT];
    				    for (int i = 0; i < list.size(); ++i) {
    				       Bytes.putInt(out, i * Bytes.SIZEOF_INT, list.get(i).intValue());
    				    }
    				    docSet = Bytes.add(Bytes.toBytes(list.size()), out);
    					put.add(FAMILY, Bytes.toBytes(entry2.getKey()), docSet);
    				}
    				context.write( new ImmutableBytesWritable(Bytes.toBytes(tableName)), put);
    			}
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		} catch (InterruptedException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
        }
        
	}

}
