package com.xjtudlc.idc.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.xjtudlc.idc.index.analyzer.BaseAnalyzer;

public class Searcher {
	
	public void search() throws CorruptIndexException, IOException, ParseException{
		String indexPath = "E:\\500M_index";
		String keyword = "book";
		    IndexReader reader = IndexReader.open(FSDirectory.open(new File(indexPath)));
	        IndexSearcher searcher = new IndexSearcher(reader);
	        
	        long s = System.currentTimeMillis();
	        /**ËÑË÷Ìõ¼þ*/
	        QueryParser parser = new QueryParser(Version.LUCENE_30, "contents", new BaseAnalyzer());
//	        QueryParser parser = new MultiFieldQueryParser(VERSION_36, fields, new BaseAnalyzer());
	        Query query = parser.parse(keyword);
	        System.out.println("Searching for: " + query.toString());
	        
	        TopDocs results = searcher.search(query, 50);
//	        TopDocs results = searcher.search(query, 5, sort);
	        ScoreDoc[] hits = results.scoreDocs;
	        System.out.println("results.totalHits:"+ results.totalHits);
	       // System.out.println("hits.length:"+ hits.length);
	        for(int i=0;i<hits.length;i++){
	        	ScoreDoc scoreDoc = hits[i];
		        float score = scoreDoc.score;
		        int docID = scoreDoc.doc;
		        /*
		         * IndexSearcher call Function of document in HBaseIndexReader
		         */
		        Document document = searcher.doc(docID);//
		        System.out.println("docId: "+docID +" score: "+score+" document:"+document.get("filename"));
	        }
	        long e = System.currentTimeMillis();
	        System.out.println((e-s)+"ms");
	        searcher.close();
	        
	}
	public static void main(String args[]){
		Searcher s = new Searcher();
		try {
			s.search();
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
