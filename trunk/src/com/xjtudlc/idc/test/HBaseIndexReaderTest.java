package com.xjtudlc.idc.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.StaleReaderException;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;

import com.xjtudlc.idc.index.HBaseIndexReader;

public class HBaseIndexReaderTest {
	
	public static void main(String args[]){
        String tableName = "test";
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "xjtudlClient:60000");
		config.set("hbase.zookeeper.quorum", "xjtudlClient");
		
		HTablePool pool = new HTablePool(config,2000);
		HBaseIndexReader reader = new HBaseIndexReader(pool,tableName,config);
		//Term term = new Term("title","two");
		try {
			HashMap<String,Integer> map=reader.loadForward("/title/one","content");
			for(Entry<String,Integer> entry:map.entrySet()){
				System.out.println(entry.getKey()+"----"+entry.getValue());
			}
		} catch (StaleReaderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
