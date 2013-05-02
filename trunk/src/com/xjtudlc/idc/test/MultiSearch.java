package com.xjtudlc.idc.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.xjtudlc.idc.index.HBaseIndexReader;

public class MultiSearch {
	
	public void search() throws IOException, ParseException{
		String indexName = "weibotest1";
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "xjtudlClient:60000");
		config.set("hbase.zookeeper.quorum", "xjtudlClient");
		HTablePool pool = new HTablePool(config,2000);
		
		IndexReader reader = new HBaseIndexReader(pool, indexName,config);
		IndexSearcher searcher = new IndexSearcher(reader);
	    //searcher.setSimilarity(new Similarity());
	    
	    String[] queryString = {"日本","中国"};
	    String[] fields = {"text","text"}; 
	    IKAnalyzer  analyzer = new IKAnalyzer();
	    BooleanClause.Occur[]   flags   =   {   //检索条件SHOULD
	    		BooleanClause.Occur.MUST,
                BooleanClause.Occur.MUST};
	    //BooleanClause.Occur[]   flags   =   { BooleanClause.Occur.MUST};
	    Query   query = MultiFieldQueryParser.parse(Version.LUCENE_30, queryString, fields, flags, analyzer);
		System.out.println(query);
		ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
		System.out.println(searcher.search(query, 1000).totalHits+"&&&&&&");
		for (int i = 0; i < hits.length; i++) {
			System.out.println(hits[i].doc+"@@@@@");
		    Document hitDoc = searcher.doc(hits[i].doc);
		    System.out.println(hitDoc.get("text"));
		    System.out.print("\n");

		}
		searcher.close();
	}
	
	public static void main(String args[]) throws IOException, ParseException{
		MultiSearch search = new MultiSearch();
		search.search();
	}

}
