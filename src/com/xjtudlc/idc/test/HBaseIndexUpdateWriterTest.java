package com.xjtudlc.idc.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StaleReaderException;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;

import com.xjtudlc.idc.cluster.util.AnalyzerUtil;
import com.xjtudlc.idc.index.HBaseIndexReader;
import com.xjtudlc.idc.index.HBaseIndexStore;
import com.xjtudlc.idc.index.HBaseIndexWriter;
import com.xjtudlc.idc.index.analyzer.BaseAnalyzer;
import com.xjtudlc.idc.index.analyzer.PorterStemAnalyzer;
import com.xjtudlc.idc.index.analyzer.StopWordAnalyzer;
import com.xjtudlc.idc.index.analyzer.VocabularyAnalyzer;
import com.xjtudlc.idc.index.update.HBaseIndexUpdateStore;
import com.xjtudlc.idc.index.update.HBaseIndexUpdateWriter;
import com.xjtudlc.idc.util.LubaseUtil;

public class HBaseIndexUpdateWriterTest {
	
public void create(HTablePool pool,String tableName,Configuration config) throws IOException{
		
	    HBaseIndexUpdateStore store1 = new HBaseIndexUpdateStore(pool,tableName,config);
		//write
	    HBaseIndexUpdateWriter writer = new HBaseIndexUpdateWriter(store1);  
	    System.out.println("***********");
		Document doc = new Document();
	    doc.add(new Field("title", "one", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    doc.add(new Field("content", "hello java hello java hello", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    writer.addDocument(doc, new BaseAnalyzer());
	    writer.close();
	    store1.close();
	}
	//update..
public void update(HTablePool pool,String tableName,Configuration config) throws IOException{
	HBaseIndexUpdateStore store1 = new HBaseIndexUpdateStore(pool,tableName,config);
	//write
    HBaseIndexUpdateWriter writer = new HBaseIndexUpdateWriter(store1);//0 for load forward.
    writer.load("/content/","title","one");
    System.out.println("***********");
	Document doc = new Document();
    doc.add(new Field("title", "one", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
    doc.add(new Field("content", "hello hello fire", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
    writer.updateDocument(doc, new BaseAnalyzer());
    writer.close();
}
	
	
	public static void main(String args[]) throws IOException
	{
		HBaseIndexUpdateWriterTest test = new HBaseIndexUpdateWriterTest();
		String tableName = "test2";
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "xjtudlClient:60000");
		config.set("hbase.zookeeper.quorum", "xjtudlClient");
		
		//HBaseIndexStore.createTable(tableName, config);
		
		HTablePool pool = new HTablePool(config,2000);
		
		//HBaseIndexStore.createTable(tableName, config);
		
		//test.create(pool,tableName,config);
		//test.update(pool,tableName,config);
		//test.delete(pool,tableName,config);
		//test.update(tableName);
		HBaseIndexUpdateStore store = new HBaseIndexUpdateStore(pool,tableName,config);
		
		Set<String> vocabulary = AnalyzerUtil.loadVocabulary();
		Set<String> stopWords = AnalyzerUtil.loadStopWord();
		File dir = new File("E:\\Lubase\\sfile\\100\\");
		HBaseIndexUpdateWriter writer = new HBaseIndexUpdateWriter(store); 
		
		//write
//		long startTime1 = System.currentTimeMillis();
//		if(dir.isDirectory())
//		{
//			File f[] = dir.listFiles();
//			for(int i=0;i<f.length;i++)
//			{
//				String fileName = f[i].getName();
//				Document doc = new Document();
//				Field f0 = new Field("FileName",fileName,Field.Store.YES, Field.Index.NOT_ANALYZED);
//				doc.add(f0);
//				FileInputStream is = new FileInputStream(f[i]);
//				Reader reader = new BufferedReader(new InputStreamReader(is));
//				Field f1 = new Field("Content",LubaseUtil.ReaderToString(reader),Field.Store.YES, Field.Index.ANALYZED);
//				doc.add(f1);
//				writer.addDocument(doc, new VocabularyAnalyzer(new PorterStemAnalyzer(new StopWordAnalyzer(new BaseAnalyzer(),stopWords)),vocabulary));
//			}
//		}
//		long s1 = System.currentTimeMillis();
//		writer.commit();
//		long endTime1 = System.currentTimeMillis();
//		System.out.println("commit time:"+(endTime1-s1)+"ms");
//	    System.out.println((endTime1-startTime1)+"ms");
		
		
		
		////update
		long startTime = System.currentTimeMillis();
		if(dir.isDirectory())
		{
			File f[] = dir.listFiles();
			for(int i=0;i<f.length;i++)
			{
				String fileName = f[i].getName();
				Document doc = new Document();
				Field f0 = new Field("FileName",fileName,Field.Store.YES, Field.Index.NOT_ANALYZED);
				doc.add(f0);
				FileInputStream is = new FileInputStream(f[i]);
				Reader reader = new BufferedReader(new InputStreamReader(is));
				Field f1 = new Field("Content",LubaseUtil.ReaderToString(reader),Field.Store.YES, Field.Index.ANALYZED);
				doc.add(f1);
				writer.load("Content", "FileName", fileName);//Must load first!!
				writer.updateDocument(doc,  new VocabularyAnalyzer(new PorterStemAnalyzer(new StopWordAnalyzer(new BaseAnalyzer(),stopWords)),vocabulary));
			}
		}
		
		store.close();
		writer.close();
		long endTime = System.currentTimeMillis();
	    System.out.println((endTime-startTime)+"ms");

	    
	}
	

}
