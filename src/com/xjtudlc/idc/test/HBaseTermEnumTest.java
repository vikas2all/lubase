package com.xjtudlc.idc.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.index.Term;

import com.xjtudlc.idc.index.HBaseIndexReader;
import com.xjtudlc.idc.index.HBaseTermEnum;

public class HBaseTermEnumTest {
	
	public static void main(String args[]) throws IOException
	{
		String indexName = "index";
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "xjtudlClient:60000");
		config.set("hbase.zookeeper.quorum", "xjtudlClient");
		
		HTablePool pool = new HTablePool(config,2000);
		
		HBaseIndexReader reader = new HBaseIndexReader(pool,indexName,config);
		HBaseTermEnum termEnum = new HBaseTermEnum(reader);
		//System.out.println(termEnum.docFreq());
		termEnum.next();
		System.out.println(termEnum.docFreq());
		Term term = new Term("world","some");
		termEnum.skipTo(term);
		termEnum.next();
		termEnum.next();
//		termEnum.next();
//		termEnum.next();
//		termEnum.next();
	}

}
