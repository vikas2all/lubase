package com.xjtudlc.idc.experiment.vint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermPositions;

import com.xjtudlc.idc.util.LubaseConstants;
import com.xjtudlc.idc.util.LubaseUtil;

public class HBaseTermPositions implements TermPositions, LubaseConstants {
	
	private final HTable table;
	//private final HTablePool pool;
	
	private List<byte[]> documents;

	private int currentIndex;

	private byte[] currentRow;
	
	private HashMap<Integer,byte[]> termPositions_q;

	private int[] currentTermPositions;

	private int currentTermPositionIndex;
	
	public HBaseTermPositions(final HBaseIndexReader reader)
	{
		//this.pool = reader.getTablePool();
		this.table = reader.getTable();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(this.documents==null)return;
		this.documents.clear();
	    this.currentIndex = 0;
	    //this.pool.putTable(table);

	}

	@Override
	public int doc() {
		// TODO Auto-generated method stub
		return Bytes.toInt(this.documents.get(this.currentIndex));
	}

	/**
	 * Get term frequency.
	 */
	@Override
	public int freq() {
		// TODO Auto-generated method stub
		return this.currentTermPositions.length;
	}

	/**
	 * Moves to next document.
	 */
	@Override
	public boolean next() throws IOException {
		// TODO Auto-generated method stub
		if(this.documents==null)return false;
	    if (currentIndex < (this.documents.size() - 1)) {
		   this.currentIndex++;
		   resetTermPositions();
		   return true;
	    } else {
		   return false;
	    }
	}
	
	/**
	 * Have been changed to the new one
	 * @author song 11/1/12
	 * @throws IOException
	 */
	void resetTermPositions() throws IOException
	{
		//Get get = new Get(this.currentRow);
		//System.out.println(this.currentRow);
		//Result r = table.get(get);
		//byte[] vector = r.getValue(FAMILY, this.documents.get(this.currentIndex));
		byte[] vector = this.termPositions_q.get(Bytes.toInt(this.documents.get(this.currentIndex)));
		this.currentTermPositionIndex = 0;
		this.currentTermPositions = LubaseUtil.encode(vector);
	}

	@Override
	public int read(int[] docs, int[] freqs) throws IOException {
	    int count = 0;
		for (int i = 0; i < docs.length; ++i) {
		   if (next()) {
		       docs[i] = this.doc();
		       freqs[i] = this.freq();
		       ++count;
		   } else {
		       break;
		   }
		   }
		return count;
	}

	/**
	 * TODO Have got from HBase, whether it can be reduced?
	 * @see org.apache.lucene.index.IndexReader#docFreq
	 * @author song
	 */
	@Override
	public void seek(Term term) throws IOException {
		final String rowKey = "/"+term.field()+"/"+term.text();
		//System.out.println(Bytes.toBytes(rowKey));
		this.currentRow = Bytes.toBytes(rowKey);
		Get get = new Get(this.currentRow);
		Result r = this.table.get(get);
		if(!r.isEmpty())
		{
//			NavigableMap<byte[], byte[]> map = r.getFamilyMap(FAMILY);
			this.documents = new ArrayList<byte[]>();
			this.termPositions_q = new HashMap<Integer,byte[]>();
			System.out.println(r.size()+"@@##$$");
			for(KeyValue kv : r.raw()){
				if(kv.getValue()!=null){
					this.documents.add(kv.getQualifier());
					this.termPositions_q.put(Bytes.toInt(kv.getQualifier()), kv.getValue());
				}
			}
			this.currentIndex = -1;
		}
		else{
			System.out.println("........");
			this.documents = null;
			this.currentIndex = -1;
		}
	}

	@Override
	public void seek(TermEnum termEnum) throws IOException {
		// TODO Auto-generated method stub
		seek(termEnum.term());

	}

	@Override
	public boolean skipTo(int arg0) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte[] getPayload(byte[] arg0, int arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPayloadLength() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isPayloadAvailable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int nextPosition() throws IOException {
		// TODO Auto-generated method stub
		return this.currentTermPositions[this.currentTermPositionIndex++];
	}

}
