package com.xjtudlc.idc.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.wltea.analyzer.lucene.IKQueryParser;

import com.xjtudlc.idc.index.HBaseIndexReader;

public class SearchWeiboTest {
	
	public static void main(String args[]){
		String indexName = "yulin";
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "xjtudlClient:60000");
		config.set("hbase.zookeeper.quorum", "xjtudlClient");
		HTablePool pool = new HTablePool(config,2000);
		
		IndexReader reader = new HBaseIndexReader(pool, indexName,config);
	    IndexSearcher searcher = new IndexSearcher(reader);
	    
	    //QueryParser queryParser = new QueryParser(Version.LUCENE_30,"Content",analyzer);
	    try {
			Query query =  IKQueryParser.parse("content","堵车");
			TopDocs topDocs = searcher.search(query,100);
		    Integer count = topDocs.totalHits;
		    ScoreDoc[] scoreDocs = topDocs.scoreDocs;
		    for(int i = 0;i<scoreDocs.length;i++){
		        ScoreDoc scoreDoc = scoreDocs[i];
		        float score = scoreDoc.score;
		        int docID = scoreDoc.doc;
		        /*
		         * IndexSearcher call Function of document in HBaseIndexReader
		         */
		        Document document = searcher.doc(docID);//
		        System.out.println("docId: "+docID +" score: "+score+" title:"+document.get("title"));
		        String content = document.get("url");
		        System.out.println(content);
		    }
		    System.out.println("总共获取了" + count + "条记录");
		    searcher.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
