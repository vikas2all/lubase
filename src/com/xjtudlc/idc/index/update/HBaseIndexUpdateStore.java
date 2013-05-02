package com.xjtudlc.idc.index.update;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StaleReaderException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.LockObtainFailedException;

import com.xjtudlc.idc.index.AbstractHbaseIndexStore;
import com.xjtudlc.idc.index.DocumentIndexContext;
import com.xjtudlc.idc.index.HBaseFlushConfig;
import com.xjtudlc.idc.index.HBaseIndexReader;
import com.xjtudlc.idc.util.LubaseConstants;

public class HBaseIndexUpdateStore extends AbstractHbaseIndexStore implements LubaseConstants, HBaseFlushConfig {
	
	private int documentId = -1;
	private final HTable table;
	public Configuration config;
	private final HTablePool pool;
	public final String name;
	
	private final Map<String,Map<Integer,List<Integer>>> termPosition = new HashMap<String,Map<Integer,List<Integer>>>();
	
	public HBaseIndexUpdateStore(final HTablePool tablePool, final String indexName, Configuration config){
		this.pool = tablePool;
		this.name = indexName;
		this.table = (HTable) tablePool.getTable(indexName);
		this.table.setAutoFlush(false);
		this.config = config;
	}

	@Override
	public void commit() throws IOException {
		// TODO Auto-generated method stub
        this.doCommit();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		commit();
	    this.table.close();
	 //   this.pool.close();
	}

	/**
	 * There must use synchronized.
	 * TODO indexDocument(DocumentIndexContext context, Int documentId);
	 */
	@Override
	public synchronized void indexDocument(DocumentIndexContext documentIndexContext) throws IOException {
		// TODO Auto-generated method stub
		++this.documentId;
		this.storeDocsNum();
		this.addTermPosition(documentId, documentIndexContext.termPositionVctor);
		if(!documentIndexContext.storeFiled.isEmpty())
			this.storeField(documentId, documentIndexContext.storeFiled);
		if(this.documentId == maxCommitDocs){
			this.doCommit();
		}
		//return this.documentId;
	}
	
	void addTermPosition(int docId,Map<String,List<Integer>> termPosition)
	{
		for(Map.Entry<String, List<Integer>> entry : termPosition.entrySet()){
			//this.termPosition.put(entry.getKey(), value)
			Map<Integer,List<Integer>> existingFrequencies = this.termPosition.get(entry.getKey());
			if(existingFrequencies == null){
				existingFrequencies = new HashMap<Integer,List<Integer>>();
				existingFrequencies.put(docId, entry.getValue());
				this.termPosition.put(entry.getKey(), existingFrequencies);
			}else{
				existingFrequencies.put(docId, entry.getValue());
			}
		}
	}
	
	//TODO Use List to put?
	void storeField(int docId, Map<String,byte[]> storeField) throws IOException
	{
		//List<Put> puts = new ArrayList<Put>();
		//System.out.println(docId+"@@@@");
		Put put = new Put(Bytes.toBytes(docId));
		for(Map.Entry<String, byte[]> entry : storeField.entrySet()){
			//System.out.println(entry.getKey());
			put.add(FAMILY,Bytes.toBytes(entry.getKey()),entry.getValue());
			
		}
		this.table.put(put);
		this.table.flushCommits();
	}
	
	void doCommit() throws IOException
	{
		this.doTermFrenquenceCommit();
		this.doForwardCommit();
		if(this.documentId!=-1)
			this.storeMaxId(this.documentId);
	}
	
	void doTermFrenquenceCommit() throws IOException
	{
		List<Put> puts = new ArrayList<Put>();
		byte[] docSet = null;
		for(Map.Entry<String, Map<Integer,List<Integer>>> entry : this.termPosition.entrySet()){
			Put put = new Put(entry.getKey().getBytes());
			put.setWriteToWAL(false);
			for(Map.Entry<Integer, List<Integer>> entry2 : entry.getValue().entrySet()){
				//System.out.println(entry.getValue().size()+"###");
				List<Integer> list = entry2.getValue();
				byte[] out = new byte[list.size() * Bytes.SIZEOF_INT];
			    for (int i = 0; i < list.size(); ++i) {
			       Bytes.putInt(out, i * Bytes.SIZEOF_INT, list.get(i).intValue());
			    }
			    docSet = Bytes.add(Bytes.toBytes(list.size()), out);
				put.add(FAMILY, Bytes.toBytes(entry2.getKey()), docSet);
			}
			puts.add(put);
			if(puts.size()==FLUSH){
				this.table.put(puts);
				this.table.flushCommits();
				puts.clear();
			}
		}
		this.table.put(puts);
		this.table.flushCommits();
		puts.clear();
	}
	
	/**
	 *  /0/content  column=fm.vc:hello, timestamp=1353898800899, value=\x00\x00\x00\x03
	 *  Build Forward of Doc. 
	 */
	void doForwardCommit() throws IOException
	{
		List<Put> puts = new ArrayList<Put>();
		byte[] docSet = null;
		for(Map.Entry<String, Map<Integer,List<Integer>>> entry:this.termPosition.entrySet()){
			for(Map.Entry<Integer, List<Integer>> entry2:entry.getValue().entrySet()){
				String str[] = entry.getKey().split("/");
			    Put put = new Put(Bytes.toBytes("/"+entry2.getKey()+"/"+str[1]));
				put.setWriteToWAL(false);
				List<Integer> list = entry2.getValue();
			    docSet = Bytes.toBytes(list.size());
				put.add(FAMILY_VECTOR, Bytes.toBytes(str[2]),docSet);
				puts.add(put);
				if(puts.size()==FLUSH){
					this.table.put(puts);
					this.table.flushCommits();
					puts.clear();
				}
			}
		}
		this.table.put(puts);
		this.table.flushCommits();
		puts.clear();
	}
	
	/**
	 * Attention !! Make +1 Atomicity.
	 * TODO Can it run fast?
	 */
	synchronized void storeDocsNum() throws IOException
	{
		int num = 0;
		Get get = new Get(DOCNUM);
		Result r = this.table.get(get);
		if(r.getValue(FAMILY, DOCNUM_QUALIFIER)!=null)
			num = Bytes.toInt(r.getValue(FAMILY, DOCNUM_QUALIFIER));
		
		Put put = new Put(DOCNUM);
		put.add(FAMILY,DOCNUM_QUALIFIER,Bytes.toBytes(num+1));//I made a mistake here, and it waste me a hole afternoon.
		this.table.put(put);
		this.table.flushCommits();
	}
	
	void storeMaxId(int docId) throws IOException
	{
		Put put = new Put(MAXID);
		put.add(FAMILY,MAXID_QUALIFIER,Bytes.toBytes(docId));
		this.table.put(put);
		this.table.flushCommits();
	}
	
	public static void createTable(String tableName, Configuration config)
	{
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if(admin.tableExists(tableName))
			{
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName+" is exist, delete...");
			}
			HTableDescriptor descriptor = new HTableDescriptor(tableName);
			descriptor.addFamily(new HColumnDescriptor(fm));
			descriptor.addFamily(new HColumnDescriptor(fmvc));
			//descriptor.addFamily(new HColumnDescriptor("score"));
			admin.createTable(descriptor);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * TODO It is not good to use HBaseIndexReader here.
	 * @throws IOException 
	 */
	public synchronized int delDocument(Term term) throws IOException{
		IndexReader reader = new HBaseIndexReader(this.pool,this.name,this.config);
		TermDocs docs = reader.termDocs(term);
		if (docs == null) return 0;
	    int n = 0;
	    /**
		 * @return The first one's id
		 */
	    if(docs.next())
			n = docs.doc();
		try {
			reader.deleteDocuments(term);//调用doDelete
			
			//reader.close();
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
		return n;
	}
	
	/**
	 * Load forward data.
	 * @param fileName --> /title/filename
	 * @param field --> 
	 * @return HashMap<String, Integer> Term frequency.
	 * @throws IOException
	 * 论文的方案，需要构建前置表支持。
	 */
	public HashMap<String,Integer> loadForward(String fileName,String field) throws IOException{
		HashMap<String,Integer> map = new HashMap<String,Integer>();
		Get g = new Get(fileName.getBytes());
		int id = -1;
		Result r = this.table.get(g);
		/**
		 *  /FileName/40.pdf.txt column=fm:\x00\x00\x00\x09
		 */
		if(r!=null){
			//NavigableMap<byte[], byte[]> m = r.getFamilyMap(FAMILY);
			//id = Bytes.toInt(m.firstKey());
			KeyValue kv[] = r.raw();
			id = Bytes.toInt(kv[0].getQualifier());
			map.put(fileName, id);
		}
		if(id!=-1){
			g = new Get(Bytes.toBytes("/"+id+"/"+field));
			r = this.table.get(g);
			/**
			 * If you don't assign Family to a Get, then it will get all versions of this rowKey.
			 */
			for(KeyValue kv : r.raw()){
				map.put("/"+field+"/"+Bytes.toString(kv.getQualifier()), Bytes.toInt(kv.getValue()));
			}
		}	
		return map;
	}
	
	public void showMap1(Map<String,Integer> map)
	{
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			System.out.print(entry.getKey()+": ");
			System.out.print(entry.getValue());
			System.out.println("");
		}
	}
	/**
	 * 我的方案
	 * @param fileName
	 * @param rowPrifix
	 * @return
	 * @throws IOException
	 */
	public HashMap<String,Integer> loadForward2(String fileName,String rowPrifix) throws IOException{
		HashMap<String,Integer> map = new HashMap<String,Integer>();
		Get g = new Get(fileName.getBytes());
		int id = -1;
		Result r = this.table.get(g);
		/**
		 * Get Document id by file name.
		 *  /FileName/40.pdf.txt column=fm:\x00\x00\x00\x09
		 */
		if(r!=null){
			//NavigableMap<byte[], byte[]> m = r.getFamilyMap(FAMILY);
			//id = Bytes.toInt(m.firstKey());
			KeyValue kv[] = r.raw();
			id = Bytes.toInt(kv[0].getQualifier());
			map.put(fileName, id);
		}
		if(id!=-1){
			Scan s = new Scan();
	        s.setCaching(10000);
	        s.setFilter(new PrefixFilter(rowPrifix.getBytes()));//前缀过滤器->content
	        s.addColumn(FAMILY, Bytes.toBytes(id));
	        ResultScanner rs = table.getScanner(s);
	        for (Result r1 : rs) {
	          KeyValue[] kv = r1.raw();
	          for (int i = 0; i < kv.length; i++) {
	              map.put(Bytes.toString(kv[i].getRow()), Bytes.toInt(kv[i].getValue()));//Don't need to encode, it use the first 4 to convert to Int.
	          }
	       }
		}
		return map;
	}
	
	/**
	 * It's for update with specified id. Perfect!
	 */
	public synchronized void indexDocument(DocumentIndexContext documentIndexContext,int docId,Map<String,Integer> map) throws IOException {
		// TODO Auto-generated method stub
		//this.storeDocsNum();
		if(map.size()>0)
			delForward(map,docId);
		if(documentIndexContext.termPositionVctor.size()>1)
			this.addTermPosition(docId, documentIndexContext.termPositionVctor);
		if(!documentIndexContext.storeFiled.isEmpty())
			this.storeField(docId, documentIndexContext.storeFiled);
		this.doCommit();
	}
	
	/**
	 * Delete term which is in old forward but not in new forward.
	 * @throws IOException 
	 */
	public void delForward(Map<String,Integer> map, int docId) throws IOException{
		if(map.size()>0){
			for(Entry<String,Integer> entry:map.entrySet()){
				String str[] = entry.getKey().split("/");
				Delete delete = new Delete(entry.getKey().getBytes());
				delete = delete.deleteColumns(FAMILY, Bytes.toBytes(docId));
				this.table.delete(delete);
				delete = new Delete(Bytes.toBytes("/"+docId+"/"+str[1]));
				delete = delete.deleteColumns(FAMILY_VECTOR, Bytes.toBytes(str[2]));
				this.table.delete(delete);
			}
			this.table.flushCommits();
		}
	}
	
	/**
	 * TODO There must be wrong, when Multithreaded.  Not been used.
	 * It is so bad that i could not use it!!!!
	 * Well, if i use it, it is too difficult to control it when multithead. 
	 * @return
	 */
	public synchronized  int maxId() {
		// TODO Auto-generated method stub
		int num = -1;
		Get get = new Get(MAXID);
		try {
			Result r = table.get(get);
			if(r!=null)
				num = Bytes.toInt(r.getValue(FAMILY, MAXID_QUALIFIER));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return num;
	}
	
	/**
	 * Not used.
	 * @return
	 */
	public synchronized int numDocs() {
		// TODO Auto-generated method stub
		int num = 0;
		Get get = new Get(DOCNUM);
		try {
			Result r = this.table.get(get);
			if(r.getValue(FAMILY, DOCNUM_QUALIFIER)!=null)
				num = Bytes.toInt(r.getValue(FAMILY, DOCNUM_QUALIFIER));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return num;
	}

	@Override
	public void indexDocument(DocumentIndexContext documentIndexContext,
			int docId) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
