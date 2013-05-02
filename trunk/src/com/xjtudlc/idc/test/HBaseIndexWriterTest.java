package com.xjtudlc.idc.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import com.xjtudlc.idc.util.LubaseUtil;

public class HBaseIndexWriterTest {
	
	public void create(HTablePool pool,String tableName,Configuration config) throws IOException{
		
		HBaseIndexStore store = new HBaseIndexStore(pool,tableName,config);
		//write
		HBaseIndexWriter writer = new HBaseIndexWriter(store); 
		Document doc = new Document();
	    doc.add(new Field("title", "one", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    doc.add(new Field("content", "hello java hello world hello", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    writer.addDocument(doc, new BaseAnalyzer());
	    writer.close();
	    store.close();
	}
	
	public void update(HTablePool pool,String tableName,Configuration config) throws IOException{
		
		HBaseIndexStore store = new HBaseIndexStore(pool,tableName,config);
		//write
		HBaseIndexWriter writer = new HBaseIndexWriter(store); 
		Document doc = new Document();
	    doc.add(new Field("title", "ten", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    doc.add(new Field("content", "hello china", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    Term term = new Term("title","two");
	    writer.updateDocument(term,doc, new BaseAnalyzer());
	   // writer.close();
	    store.close();
	}
	
	public void delete(HTablePool pool,String tableName,Configuration config) throws StaleReaderException, CorruptIndexException, LockObtainFailedException, IOException{
		IndexReader reader = new HBaseIndexReader(pool,tableName,config);
		reader.deleteDocuments(new Term("title","one"));
	}
	
	public void create2(File dir,HBaseIndexWriter writer) throws Exception{
		long startTime1 = System.currentTimeMillis();
		if(dir.isDirectory())
		{
			File f[] = dir.listFiles();
			for(int i=0;i<f.length;i++)
			{
				String fileName = f[i].getName();
				//long s1 = System.currentTimeMillis();
				Document doc = new Document();
				Field f0 = new Field("FileName",fileName,Field.Store.YES, Field.Index.NOT_ANALYZED);
				doc.add(f0);
				FileInputStream is = new FileInputStream(f[i]);
				Reader reader = new BufferedReader(new InputStreamReader(is));
				Field f1 = new Field("Content",LubaseUtil.ReaderToString(reader),Field.Store.YES,Field.Index.ANALYZED);
				doc.add(f1);
				//long s2 = System.currentTimeMillis();
				//System.out.println(s2-s1);
				writer.addDocument(doc, new BaseAnalyzer());
			}
		}
		long s1 = System.currentTimeMillis();
		writer.commit();
		long endTime1 = System.currentTimeMillis();
		System.out.println("commit time:"+(endTime1-s1)+"ms");
	    System.out.println((endTime1-startTime1)+"ms");
	}
	
	/**
	 * Lucene更新方案实例
	 * 
	 * @param dir
	 * @param writer
	 * @param vocabulary
	 * @param stopWords
	 * @throws Exception
	 */
	public void update2(File dir,HBaseIndexWriter writer,Set<String> vocabulary,Set<String> stopWords ) throws Exception{
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
				Term term = new Term("FileName",fileName);
				//writer.addDocument(doc, new VocabularyAnalyzer(new PorterStemAnalyzer(new StopWordAnalyzer(new BaseAnalyzer(),stopWords)),vocabulary));
				writer.updateDocument(term,doc,  new VocabularyAnalyzer(new PorterStemAnalyzer(new StopWordAnalyzer(new BaseAnalyzer(),stopWords)),vocabulary));
			}
		}
		writer.close();
		long endTime = System.currentTimeMillis();
	    System.out.println((endTime-startTime)+"ms");
	}
	
	public static void main(String args[]) throws IOException
	{
		HBaseIndexWriterTest test = new HBaseIndexWriterTest();
		String tableName = "t1";
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
		HBaseIndexStore store = new HBaseIndexStore(pool,tableName,config);
		//write
//		Set<String> vocabulary = AnalyzerUtil.loadVocabulary();
//		Set<String> stopWords = AnalyzerUtil.loadStopWord();
		File dir = new File("E:\\Lubase\\sfile\\test\\");
		HBaseIndexWriter writer = new HBaseIndexWriter(store); 
		//test.create(pool,tableName,config);
		try {
			test.create2(dir, writer);
			//test.update2(dir, writer, vocabulary, stopWords);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//update
		
	}
	
}
