package com.xjtudlc.idc.experiment.vint;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import com.xjtudlc.idc.util.LubaseConstants;

public class HBaseTermEnum extends TermEnum implements LubaseConstants {

	private final HTable table;

	private ResultScanner resultScanner;

	private Term currentTerm;

	//private final HTablePool pool;
	
	public HBaseTermEnum(final HBaseIndexReader reader) throws IOException
	{
		// this.pool = reader.getTablePool();
		 this.table = reader.getTable();
		 this.resultScanner = this.table.getScanner(Bytes.toBytes("document"));
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		 this.resultScanner.close();
	}

	@Override
	public int docFreq() {
		// TODO Get the number of doc? Why can i use sequenceId?
		// Returns the docFreq of the current Term in the enumeration. 
		Scan scan = new Scan(Bytes.toBytes("/" + this.currentTerm.field() + "/"+ this.currentTerm.text()));
		ResultScanner scanner = null;
	    try {
	      scanner = this.table.getScanner(scan);
	      final Result result = scanner.next();

	      if (result == null) {
	        return 0;
	      }
	      NavigableMap<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes("document"));
	      return map.size();
	    } catch (Exception ex) {
	      return 0;
	    } finally {
	      scanner.close();
	    }
	}

	/*
	 * I am sure this function doesn't work, but this class is not used.
	 * @see org.apache.lucene.index.TermEnum#next()
	 */
	@Override
	public boolean next() throws IOException {
		// TODO Auto-generated method stub
		try {
		      Result result = resultScanner.next();
		      if (result != null) {
		        String fieldTerm = Bytes.toString(result.getRow());
		        String[] fieldTerms = fieldTerm.split("/");
//		        for(int i=0;i<fieldTerms.length;i++){
//		        	System.out.print(fieldTerms[i]+"%");
//		        }
		        if(fieldTerms.length>3)
		        	this.currentTerm = new Term(fieldTerms[2], fieldTerms[3]);
		        else
		        	this.currentTerm = new Term(fieldTerms[1], fieldTerms[2]);
		        
		        System.out.println(this.currentTerm.field()+"****"+this.currentTerm.text());
		        return true;
		      } else {
		        return false;
		      }
		    } catch (Exception ex) {
		      return false;
		    }
	}

	@Override
	public Term term() {
		// TODO Auto-generated method stub
		return this.currentTerm;
	}
	
	public void skipTo(Term t) throws IOException {
	    if (this.resultScanner != null) {
	      this.resultScanner.close();
	    }
	    Scan scan = new Scan();
	    scan.addFamily(Bytes.toBytes("document"));
	    scan.setStartRow(Bytes.toBytes("/" + t.field() + "/" + t.text()));
	    this.resultScanner = this.table.getScanner(scan);
	  }

}
