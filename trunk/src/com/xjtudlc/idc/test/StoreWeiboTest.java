package com.xjtudlc.idc.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;
import org.wltea.analyzer.lucene.IKQueryParser;

import com.xjtudlc.idc.cluster.util.AnalyzerUtil;
import com.xjtudlc.idc.index.HBaseIndexReader;
import com.xjtudlc.idc.index.HBaseIndexStore;
import com.xjtudlc.idc.index.HBaseIndexWriter;
import com.xjtudlc.idc.index.analyzer.BaseAnalyzer;
import com.xjtudlc.idc.util.ConnectDB;
public class StoreWeiboTest {
	public static final Logger log = Logger.getLogger(StoreWeiboTest.class);
	
public void create(HTablePool pool,String tableName,Configuration config,Analyzer analyzer) throws IOException{
		
		HBaseIndexStore store = new HBaseIndexStore(pool,tableName,config);
		//write
		HBaseIndexWriter writer = new HBaseIndexWriter(store); 
		Document doc = new Document();
	    doc.add(new Field("title", "微博", Field.Store.YES,Field.Index.NOT_ANALYZED));
	    doc.add(new Field("content", "转发微博", Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    writer.addDocument(doc, analyzer);
	    writer.close();
	    store.close();
	}
public void search(){
	String indexName = "weibotest";
	Configuration config = HBaseConfiguration.create();
	config.set("hbase.master", "xjtudlClient:60000");
	config.set("hbase.zookeeper.quorum", "xjtudlClient");
	HTablePool pool = new HTablePool(config,2000);
	
	IndexReader reader = new HBaseIndexReader(pool, indexName,config);
    IndexSearcher searcher = new IndexSearcher(reader);
    
    //QueryParser queryParser = new QueryParser(Version.LUCENE_30,"Content",analyzer);
    try {
		Query query =  IKQueryParser.parse("content","微博");
		System.out.println(query.toString());
		TopDocs topDocs = searcher.search(query,20);
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
	        System.out.println("docId: "+docID +" score: "+score+" document:"+document.get("title"));
	        String content = document.get("content");
	        System.out.println(content);
	    }
	    System.out.println("总共获取了" + count + "条记录");
	    searcher.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

public void fromMysql(HTablePool pool,String tableName,Configuration config,Analyzer analyzer) throws IOException{
	HBaseIndexStore store = new HBaseIndexStore(pool,tableName,config);
	//write
	HBaseIndexWriter writer = new HBaseIndexWriter(store); 
	Connection con = ConnectDB.getConnection();
	//ArrayList<WeiboBean> list = new ArrayList<WeiboBean>();
	String sql ="select * from post";
	try {
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()){
			//System.out.println(rs.getString("text"));
			Document doc = new Document();
		    doc.add(new Field("url", rs.getString("url"), Field.Store.YES,Field.Index.NOT_ANALYZED));
		    doc.add(new Field("title", rs.getString("title"), Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
		    doc.add(new Field("content", rs.getString("content"), Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
		    doc.add(new Field("post_time", rs.getString("post_time"), Field.Store.YES,Field.Index.NOT_ANALYZED));
		    writer.addDocument(doc, new IKAnalyzer());
		}
		rs.close();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	writer.close();
}

public void create2(HTablePool pool,String tableName,Configuration config,Analyzer analyzer) throws IOException{
	HBaseIndexStore store = new HBaseIndexStore(pool,tableName,config);
	//write
	String text = "日本共同社6月14日报道，由中国大陆、台湾地区以及中国香港等地民间团体组成的“世界华人保钓联盟”14日在香港向媒体透露，已有保钓船从中国大陆出发，如果顺利，将在1-2日内到达钓鱼岛。";
	String text1 = "//@王强_99:崔永元：日本人也太矫情了，丢个自行车也要发个微薄，看看咱们中国人多淡定：出去一趟房子没了；去公安局一趟命没了，去山西一趟人没了，去河南一趟肾没了，去重庆一趟自由没了，去广西一趟钱没了，去广东一趟包没了，去北京一趟变精神病了，都觉得正常。";
	HBaseIndexWriter writer = new HBaseIndexWriter(store); 

	for(int i=0;i<10000;i++){
		Document doc = new Document();
	    doc.add(new Field("user", "1", Field.Store.YES,Field.Index.NOT_ANALYZED));
	    doc.add(new Field("text", text1, Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    doc.add(new Field("time", "1", Field.Store.YES,Field.Index.NOT_ANALYZED));
	    writer.addDocument(doc, new IKAnalyzer());
	    
	}
		
	    Document doc1 = new Document();
	    doc1.add(new Field("user", "1", Field.Store.YES,Field.Index.NOT_ANALYZED));
	    doc1.add(new Field("text", text, Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
	    doc1.add(new Field("time", "1", Field.Store.YES,Field.Index.NOT_ANALYZED));
	    writer.addDocument(doc1, new IKAnalyzer());

	writer.close();
}

public void scan(HTablePool pool,String tableName,Configuration config) throws IOException{
	HTable table = (HTable) pool.getTable(tableName);
	Scan scan = new Scan();
	scan.setCaching(10000);//improve almost 5 times!!!!!
	scan.addColumn("fm".getBytes(), Bytes.toBytes(9505));
	ResultScanner rs = table.getScanner(scan);
	BufferedWriter bw = new BufferedWriter(new FileWriter(new File("E:\\song.log")));
	for(Result r : rs)
	{ 
		String text = Bytes.toString(r.getRow());
		System.out.println(text);
		bw.write(text+"###"+Bytes.toInt(r.getValue("fm".getBytes(), Bytes.toBytes(9505))));
		bw.newLine();
		//log.error(Bytes.toString(r.getRow()));
		//document.add(new Field(str[0],str[1],Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
		
	}
	bw.flush();
	bw.close();
}

public static void main(String args[]) throws IOException{
	StoreWeiboTest t = new StoreWeiboTest();
	String tableName = "yulin";
	Configuration config = HBaseConfiguration.create();
	config.set("hbase.master", "xjtudlClient:60000");
	config.set("hbase.zookeeper.quorum", "xjtudlClient");
	
	HBaseIndexStore.createTable(tableName, config);
	
	HTablePool pool = new HTablePool(config,2000);
	Analyzer analyzer = new IKAnalyzer();
	//t.create2(pool, tableName, config, analyzer);
	//t.scan(pool, tableName, config);
//	t.search();
	t.fromMysql(pool, tableName, config, analyzer);
	//t.create2(list, pool, tableName, config, analyzer);
}

}
