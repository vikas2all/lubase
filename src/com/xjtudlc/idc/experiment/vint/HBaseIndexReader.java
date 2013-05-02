package com.xjtudlc.idc.experiment.vint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.DefaultSimilarity;

import com.xjtudlc.idc.util.LubaseConstants;
import com.xjtudlc.idc.util.LubaseUtil;


public class HBaseIndexReader extends IndexReader implements LubaseConstants {
	
	private final HTable table;
	private Configuration config;
	
	static final byte DEFAULT_NORM = DefaultSimilarity.encodeNorm(1.0f);
	
	public HBaseIndexReader(HTablePool tablePool, String indexName, Configuration config) {
		this.table = (HTable) tablePool.getTable(indexName);
		this.table.setAutoFlush(false);
		this.config = config;
	}

	@Override
	protected void doClose() throws IOException {
		// TODO Auto-generated method stub
		this.table.close();

	}

	@Override
	protected void doCommit(Map<String, String> arg0) throws IOException {
		// TODO Auto-generated method stub
		this.table.flushCommits();

	}
	

	@Override
	protected void doDelete(int docId) throws CorruptIndexException, IOException {
		// TODO Auto-generated method stub
		delFromHBase(docId);
		Document doc = getTermByDocId(docId);
		List<Fieldable> list = doc.getFields();
		delFromHBase(list,docId);
		updateDocNum(this.table);
		this.table.flushCommits();
	}
	
	protected void delFromHBase(List<Fieldable> list,int docId) throws IOException{
		List<Delete> dels = new ArrayList<Delete>();
		for(int i=0;i<list.size();i++){
		  Delete delete = new Delete(("/"+list.get(i).name()+"/"+list.get(i).stringValue()).getBytes());
		  delete.setWriteToWAL(false);
		  delete = delete.deleteColumns(FAMILY, Bytes.toBytes(docId));//fuck, there must be deleteColumns, and i don't know why.
		  dels.add(delete);
		  if(dels.size()>=20000){
			  table.delete(dels);
			  table.flushCommits();
			  dels.clear();
		  }
		}
		table.delete(dels);
		table.flushCommits();
		dels.clear();
	}
	
	protected void delFromHBase( int rowKey) throws IOException{
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
	}
	
	protected synchronized void updateDocNum(HTable table) throws IOException{
		int num = numDocs();
		Put p = new Put(DOCNUM);
		p.add(FAMILY, DOCNUM_QUALIFIER, Bytes.toBytes(num-1));
		table.put(p);//TODO AutoFlush is ok??
	}

	@Override
	protected synchronized void doSetNorm(int arg0, String arg1, byte arg2)
			throws CorruptIndexException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void doUndeleteAll() throws CorruptIndexException, IOException {
		// TODO Auto-generated method stub

	}

	/**
	 * @see org.apache.lucene.index.IndexReader#docFreq(org.apache.lucene.index.Term)
	 * 
	 * Returns the number of documents containing the term t.
	 * @author song 11/1/12
	 */
	@Override
	public int docFreq(Term term) throws IOException {
		// TODO Auto-generated method stub
		String rowKey = "/"+term.field()+"/"+term.text();
		Get get = new Get(Bytes.toBytes(rowKey));
		Result r = this.table.get(get);
        if(r==null)
        	return 0;
        else
        	return r.size();
	}

	/**
	 * Get Document(full) from docId.
	 * @author song 11/1/12
	 */
	@Override
	public Document document(int docId, FieldSelector field)
			throws CorruptIndexException, IOException {
		// TODO How to use FieldSelector?
		Document document = new Document();
		Get t = new Get(Bytes.toBytes(docId));
		Result r = this.table.get(t);
		/**
		 * If you don't assign Family to a Get, then it will get all versions of this rowKey.
		 */
		for(KeyValue kv : r.raw()){
			document.add(new Field(Bytes.toString(kv.getQualifier()),Bytes.toString(kv.getValue()),Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
		}
		return document;
	}
	
	/**
	 * Get Term(field and text) from document by docId, and show it in a document.
	 * @author song
	 * @return Document
	 * @throws IOException 
	 */
	public Document getTermByDocId(int docId) throws IOException{
		Document document = new Document();
		Scan scan = new Scan();
		scan.setCaching(10000);//improve almost 5 times!!!!!
		scan.addColumn(FAMILY, Bytes.toBytes(docId));
		ResultScanner rs = this.table.getScanner(scan);
		for(Result r : rs)
		{
			String str[] = LubaseUtil.splitRow(new String(r.getRow()));
			document.add(new Field(str[0],str[1],Field.Store.YES,Field.Index.ANALYZED_NO_NORMS));
		}
		return document;
	}
	
	/**
	 * Load forward data.
	 * @param fileName --> /title/filename
	 * @param field --> 
	 * @return HashMap<String, Integer> Term frequency.
	 * @throws IOException
	 */
	public HashMap<String,Integer> loadForward(String fileName,String field) throws IOException{
		HashMap<String,Integer> map = new HashMap<String,Integer>();
		Get g = new Get(fileName.getBytes());
		int id = -1;
		Result r = this.table.get(g);
		if(r!=null){
			NavigableMap<byte[], byte[]> m = r.getFamilyMap(FAMILY);
			id = Bytes.toInt(m.firstKey());
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

	@Override
	public Collection<String> getFieldNames(FieldOption arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TermFreqVector getTermFreqVector(int arg0, String arg1)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void getTermFreqVector(int arg0, TermVectorMapper arg1)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getTermFreqVector(int arg0, String arg1, TermVectorMapper arg2)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public TermFreqVector[] getTermFreqVectors(int arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasDeletions() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDeleted(int arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int maxDoc() {
		// TODO Auto-generated method stub
		return this.numDocs();
	}

	@Override
	public byte[] norms(String arg0) throws IOException {
		// TODO Auto-generated method stub
		byte[] result = new byte[this.maxDoc()];
	    Arrays.fill(result, DEFAULT_NORM);
	    return result;
	}

	@Override
	public void norms(String arg0, byte[] arg1, int arg2) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int numDocs() {
		// TODO Auto-generated method stub
		int num = 0;
		Get get = new Get(DOCNUM);
		try {
			Result r = this.table.get(get);
			num = Bytes.toInt(r.getValue(FAMILY, DOCNUM_QUALIFIER));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return num;
	}

	@Override
	public TermDocs termDocs() throws IOException {
		// TODO Auto-generated method stub
		return  new HBaseTermPositions(this);
	}

	@Override
	public TermPositions termPositions() throws IOException {
		// TODO Auto-generated method stub
		return new HBaseTermPositions(this);
	}

	@Override
	public TermEnum terms() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TermEnum terms(Term arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public HTable getTable() {
		return table;
	}
	

}
