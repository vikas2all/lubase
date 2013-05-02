package com.xjtudlc.idc.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StaleReaderException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.LockObtainFailedException;

import com.xjtudlc.idc.util.LubaseConstants;

public class HBaseIndexStore extends AbstractHbaseIndexStore implements LubaseConstants, HBaseFlushConfig {
	
	private int documentId = -1;
	private int commit = 0;
	private final HTable table;
	public Configuration config;
	private final HTablePool pool;
	private final String name;
	private int docNum = 0;
	
	private final Map<String,Map<Integer,List<Integer>>> termPosition = new HashMap<String,Map<Integer,List<Integer>>>();
	private final Map<String,Map<Integer,byte[]>> storeContent = new HashMap<String,Map<Integer,byte[]>>();
	
	public HBaseIndexStore(final HTablePool tablePool, final String indexName, Configuration config){
		this.pool = tablePool;
		this.name = indexName;
		this.table = (HTable) tablePool.getTable(indexName);
		this.table.setAutoFlush(false);
		this.config = config;
	}
	
	/**
	 * For Incremental index. Not been used.
	 * @param tablePool
	 * @param indexName
	 * @param config
	 * @param flag
	 */
	public HBaseIndexStore(final HTablePool tablePool, final String indexName, Configuration config,boolean flag){
		this.pool = tablePool;
		this.name = indexName;
		this.table = (HTable) tablePool.getTable(indexName);
		this.table.setAutoFlush(false);
		this.config = config;
		if(flag==true)this.documentId = maxId();
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
		//this.storeDocsNum();
		/**
		 * TODO there is something wrong when Incremental!!!!!!!
		 */
		++this.docNum;
		++this.commit;
		this.addTermPosition(documentId, documentIndexContext.termPositionVctor);
		if(!documentIndexContext.storeFiled.isEmpty())
			this.addStoreField(documentId, documentIndexContext.storeFiled);//it is too slow over here.
		System.out.println("add over"+this.documentId);
		if(this.commit == maxCommitDocs){
			System.out.println("commiting..............");
			this.commit = 0;
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
		termPosition.clear();
		termPosition.entrySet().clear();
	}
	
	void addStoreField(int docId,Map<String,byte[]> storeField){
		for(Map.Entry<String, byte[]> entry: storeField.entrySet()){
			Map<Integer,byte[]> existField = this.storeContent.get(entry.getKey());
			if(existField == null){
				existField = new HashMap<Integer,byte[]>();
				existField.put(docId, entry.getValue());
				this.storeContent.put(entry.getKey(), existField);
			}else{
				existField.put(docId, entry.getValue());
			}
		}
		//this.storeContent.
		storeField.clear();
		storeField.entrySet().clear();
	}
	
	//TODO Use List to put?
	void storeField() throws IOException
	{
		List<Put> puts = new ArrayList<Put>();
		for(Map.Entry<String, Map<Integer,byte[]>> entry : this.storeContent.entrySet()){
			for(Map.Entry<Integer, byte[]> entry2 : entry.getValue().entrySet()){
				//System.out.println(entry.getValue().size()+"###");
				Put put = new Put(Bytes.toBytes(entry2.getKey()));
				//put.
				put.setWriteToWAL(false);
				byte[] docSet = entry2.getValue();
				put.add(FAMILY, Bytes.toBytes(entry.getKey()), docSet);
				puts.add(put);
			}
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
	
	void doCommit() throws IOException
	{
		if(this.documentId!=-1)
			this.storeMaxId(this.documentId);
		this.storeDocsNum2();
		this.doTermFrenquenceCommit();
		this.storeField();
		//this.doTermPositionCommit();
		this.termPosition.entrySet().clear();
		this.storeContent.entrySet().clear();
		this.termPosition.clear();
		this.storeContent.clear();
		System.gc();
	}
	
	void doTermFrenquenceCommit() throws IOException
	{
		List<Put> puts = new ArrayList<Put>();
		byte[] docSet = null;
		for(Map.Entry<String, Map<Integer,List<Integer>>> entry : this.termPosition.entrySet()){
			Put put = new Put(Bytes.toBytes(entry.getKey()));
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
	 * it is no use!!
	 * commit TermVector to Hbase using byte[] instead of String 
	 * TODO Vint? UInt32?
	 */
	void doTermPositionCommit() throws IOException
	{
		List<Put> puts = new ArrayList<Put>();
		byte[] docSet = null;
		for(Map.Entry<String, Map<Integer,List<Integer>>> entry:this.termPosition.entrySet()){
			for(Map.Entry<Integer, List<Integer>> entry2:entry.getValue().entrySet()){
				String str[] = entry.getKey().split("/");
			    Put put = new Put(Bytes.toBytes("/"+entry2.getKey()+"/"+str[2]));
				put.setWriteToWAL(false);
				List<Integer> list = entry2.getValue();
				byte[] out = new byte[list.size() * Bytes.SIZEOF_INT];
			    for (int i = 0; i < list.size(); ++i) {
			       Bytes.putInt(out, i * Bytes.SIZEOF_INT, list.get(i).intValue());
			    }
			    docSet = Bytes.add(Bytes.toBytes(list.size()), out);
			        
				put.add(FAMILY_VECTOR, Bytes.toBytes(str[1]),docSet);
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
	 * This is the old version 12/11/12
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
	
	synchronized void storeDocsNum2() throws IOException
	{
		Put put = new Put(DOCNUM);
		put.add(FAMILY,DOCNUM_QUALIFIER,Bytes.toBytes(this.docNum));
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
			//descriptor.addFamily(new HColumnDescriptor(fmvc));
			//descriptor.addFamily(new HColumnDescriptor("score"));
			admin.createTable(descriptor);
			admin.close();
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
			reader.deleteDocuments(term);
			
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
	 * It's for update with specified id.
	 */
	public synchronized void indexDocument(DocumentIndexContext documentIndexContext,int docId) throws IOException {
		// TODO Auto-generated method stub
		this.storeDocsNum();
		this.addTermPosition(docId, documentIndexContext.termPositionVctor);
		if(!documentIndexContext.storeFiled.isEmpty())
			this.addStoreField(docId, documentIndexContext.storeFiled);
		this.doCommit();
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

}
