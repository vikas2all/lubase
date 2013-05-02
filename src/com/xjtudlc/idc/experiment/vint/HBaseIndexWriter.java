package com.xjtudlc.idc.experiment.vint;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;

import com.xjtudlc.idc.util.LubaseUtil;

public class HBaseIndexWriter {
	
	private final AbstractHbaseIndexStore indexStore;
	
	static final List<Integer> EMPTY_TERM_POSITIONS = Arrays.asList(new Integer[] { 0 });
	
	public HBaseIndexWriter(HBaseIndexStore indexStore)
	{
		this.indexStore = indexStore;
	}
	
	/**
	 * This is for MapReduce.
	 * @author song 11/5/12
	 */
	public HBaseIndexWriter(){
		indexStore = null;
	}
	
	public void addDocument(Document doc, Analyzer analyzer) throws IOException
	{
		Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
	    Map<String, byte[]> fieldsToStore = new HashMap<String, byte[]>();
	    add(doc,analyzer,termPositions,fieldsToStore);
	    indexStore.indexDocument(new DocumentIndexContext(termPositions,fieldsToStore));
	    //termPositions.clear();
	    //fieldsToStore.clear();
	}
	
	public void addDocumentById(Document doc,Analyzer analyzer,int docId) throws IOException{
		Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
	    Map<String, byte[]> fieldsToStore = new HashMap<String, byte[]>();
	    add(doc,analyzer,termPositions,fieldsToStore);
	    indexStore.indexDocument(new DocumentIndexContext(termPositions,fieldsToStore),docId);
	    termPositions.clear();
	    fieldsToStore.clear();
	}
	
	/**
	 * Base operation of dealing with document.
	 * @param doc
	 * @param analyzer
	 * @param termPositions
	 * @param fieldsToStore
	 * @throws IOException
	 */
	public void add(Document doc, Analyzer analyzer,Map<String,List<Integer>> termPositions,Map<String ,byte[]> fieldsToStore) throws IOException{
		int position = 0;
		for (Fieldable field : doc.getFields()) {
	    	Reader read = field.readerValue();
	        // Indexed field
	        if (field.isIndexed() && field.isTokenized()) {
	          TokenStream tokens = field.tokenStreamValue();
	          if (tokens == null) {
	        	  if(field.stringValue()==null){
	        		  tokens = analyzer.tokenStream(field.name(), read);
	        		  //Store reader of field. I don't understand why doesn't this works;
    	  	          //fieldsToStore.put(field.name(), Bytes.toBytes(LubaseUtil.ReaderToString(read)));
	        	  }else
	        		  tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
	            //field.
	          }
	          tokens.addAttribute(TermAttribute.class);
	          tokens.addAttribute(PositionIncrementAttribute.class);
	          // collect term frequencies per doc
	          if (position > 0) {
	            position += analyzer.getPositionIncrementGap(field.name());
	          }
	          // Build the termPositions vector for all terms
	          while (tokens.incrementToken()) {
	            String term = tokens.getAttribute(TermAttribute.class).term();
	            /**
	             * This is important! Otherwise the position is for the whole 
	             * document, not for fields.
	             */
	            term = "/"+field.name()+"/"+term;
	            List<Integer> pvec = termPositions.get(term);
	            if (pvec == null) {
	              pvec = new ArrayList<Integer>();
	              termPositions.put(term, pvec);
	            }
	            position += (tokens.getAttribute(PositionIncrementAttribute.class).getPositionIncrement() - 1);
	            pvec.add(++position);
	          }
	          tokens.close();
	        }
	        // Untokenized fields go in without a termPosition
	        if (field.isIndexed() && !field.isTokenized()) {
	        	String term = "";
	        	if(field.stringValue()==null){
	        	   term = "/"+field.name()+"/"+ LubaseUtil.ReaderToString(read);
	        	}else
	               term = "/"+field.name()+"/"+ field.stringValue();
	            termPositions.put(term, EMPTY_TERM_POSITIONS);
	        }
	        // Stores each field as a column under this doc key
	        if (field.isStored()) {
	            byte[] value = field.isBinary() ? field.getBinaryValue() : Bytes.toBytes(field.stringValue());
	            fieldsToStore.put(field.name(), value);
	        }
	      }
	}
	
	public void updateDocument(Term term,Document doc, Analyzer analyzer) throws IOException{
		int id = indexStore.delDocument(term);
		addDocumentById(doc,analyzer,id);
	}
	
	public void updateDocumentLocal(Term term,Document doc,Analyzer analyzer){
		/**
		 * I want to update document by id, not delete it first.
		 * But, i don't know how to do it now !
		 * @author song 11/2/12
		 */
	}

	public void commit() throws IOException {
	    this.indexStore.commit();
	}

	public void close() throws IOException {
	    this.indexStore.close();
	}
	
	public void showMap(Map<String,List<Integer>> map)
	{
		for(Map.Entry<String, List<Integer>> entry : map.entrySet()){
			System.out.print(entry.getKey()+": ");
			for(int i=0;i<entry.getValue().size();i++){
				System.out.print(entry.getValue().get(i)+" ");
			}
			System.out.println("");
		}
	}


}
