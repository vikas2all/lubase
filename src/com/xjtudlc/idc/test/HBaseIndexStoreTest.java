package com.xjtudlc.idc.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import com.xjtudlc.idc.index.DocumentIndexContext;
import com.xjtudlc.idc.index.HBaseIndexStore;

public class HBaseIndexStoreTest {
	
	public static void main(String args[]) throws IOException
	{
		String indexName = "test";
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "xjtudlClient:60000");
		config.set("hbase.zookeeper.quorum", "xjtudlClient");
		HBaseIndexStore.createTable(indexName, config);
		HTablePool pool = new HTablePool(config,2000);
		HBaseIndexStore store = new HBaseIndexStore(pool,indexName,config);
		Map<String,List<Integer>> termPositionVctor = new HashMap<String,List<Integer>>();
		for(int i=1;i<4;i++){
			List<Integer> positionList = new ArrayList<Integer>();
			positionList.add(i);
			positionList.add(i+1);
			//System.out.println(positionList.size()+"@@@@");
			termPositionVctor.put(i+"", positionList);
		}
		Map<String,byte[]> field = new HashMap<String,byte[]>();
		field.put("id", "123".getBytes());
		DocumentIndexContext context = new DocumentIndexContext(termPositionVctor,field);
		store.indexDocument(context);
		store.close();
	}

}
