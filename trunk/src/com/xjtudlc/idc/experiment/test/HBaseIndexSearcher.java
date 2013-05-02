package com.xjtudlc.idc.experiment.test;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import org.apache.lucene.util.Version;

import com.xjtudlc.idc.index.HBaseIndexReader;
import com.xjtudlc.idc.index.analyzer.BaseAnalyzer;


/**
 * It is too slow to search!
 * @author song
 *
 */
public class HBaseIndexSearcher {
	
	public static void main(String args[]) throws IOException, ParseException
	{
		String indexName = "test1";
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "hbase0:60000");
		config.set("hbase.zookeeper.quorum", "hbase1,hbase2,hbase3");
		HTablePool pool = new HTablePool(config,2000);
		
		IndexReader reader = new HBaseIndexReader(pool, indexName,config);
	    IndexSearcher searcher = new IndexSearcher(reader);
	    
	    Analyzer analyzer = new BaseAnalyzer();
	    QueryParser queryParser = new QueryParser(Version.LUCENE_30,"Content",analyzer);
	    Query query = queryParser.parse("see");
	    
	  //高亮显示
//		SimpleHTMLFormatter sHtmlF = new SimpleHTMLFormatter("<b><font color='#CC0000'>", "</font></b>");
//        Highlighter highlighter = new Highlighter(sHtmlF,new QueryScorer(query));
//      // 设置显示字数默认为100
//        highlighter.setTextFragmenter(new SimpleFragmenter(100));
        
	    //System.out.println(query);
	    TopDocs topDocs = searcher.search(query,50);
	    Integer count = topDocs.totalHits;
	    ScoreDoc[] scoreDocs = topDocs.scoreDocs;
	    for(int i = 0;i<scoreDocs.length;i++){
	        ScoreDoc scoreDoc = scoreDocs[i];
	        float score = scoreDoc.score;
	        int docID = scoreDoc.doc;
//	        /*
//	         * IndexSearcher call Function of document in HBaseIndexReader
//	         */
	        Document document = searcher.doc(docID);//
	        System.out.println("docId: "+docID +" score: "+score+" document:"+document.get("FileName"));
//	        String content = document.get("Content");
//	        TokenStream tokenStream = analyzer.tokenStream("", new StringReader(content));
//	        try {
//				String tt = highlighter.getBestFragment(tokenStream, content);
//				System.out.println(tt);
//			} catch (InvalidTokenOffsetsException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
	    }
	    System.out.println("总共获取了" + count + "条记录");
	    searcher.close();
	}

}
