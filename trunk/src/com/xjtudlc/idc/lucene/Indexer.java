package com.xjtudlc.idc.lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

import com.xjtudlc.idc.index.analyzer.BaseAnalyzer;
import com.xjtudlc.idc.util.LubaseUtil;

public class Indexer {
	public static void main(String args[]) throws IOException{
		String indexDir = "E:\\Lubase\\lucene_index";
		//将要搜索TXT文件的地方
		String dateDir = "E:\\Lubase\\sfile\\10";
		IndexWriter indexWriter = null;
		//创建Directory对象 ，FSDirectory代表待索引的文件存在磁盘上
		//Directory dir = new SimpleFSDirectory(new File(indexDir)); 
		indexWriter = new IndexWriter(FSDirectory.open(new File(indexDir)),new BaseAnalyzer(),true,IndexWriter.MaxFieldLength.UNLIMITED); 
		long start = System.currentTimeMillis();
		File[] files = new File(dateDir).listFiles();
		for (int i = 0; i < files.length; i++) {
			long s1 = System.currentTimeMillis();
			Document doc = new Document();
			//创建Field对象，并放入doc对象中
			FileInputStream is = new FileInputStream(files[i]);
			Reader reader = new BufferedReader(new InputStreamReader(is));
			doc.add(new Field("contents", LubaseUtil.ReaderToString(reader),Field.Store.YES,Field.Index.ANALYZED));
			doc.add(new Field("filename", files[i].getName(),Field.Store.YES, Field.Index.NOT_ANALYZED));
			//doc.add(new Field("indexDate",DateTools.dateToString(new Date(), DateTools.Resolution.DAY),Field.Store.YES,Field.Index.NOT_ANALYZED));
			//写入IndexWriter
			long s2 = System.currentTimeMillis();
			System.out.println(s2-s1);
			indexWriter.addDocument(doc);
		}
		long s = System.currentTimeMillis();
		System.out.println(s-start);
		indexWriter.close(); 
		long end = System.currentTimeMillis();
		System.out.println("time :"+(end-start));
	}

}
