package com.xjtudlc.idc.index.update;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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

import com.xjtudlc.idc.index.AbstractHbaseIndexStore;
import com.xjtudlc.idc.index.DocumentIndexContext;
import com.xjtudlc.idc.util.LubaseUtil;

public class HBaseIndexUpdateWriter {
	
	private final HBaseIndexUpdateStore indexStore;
	
	static final List<Integer> EMPTY_TERM_POSITIONS = Arrays.asList(new Integer[] { 0 });
	
	public HashMap<String,Integer> forward = new HashMap<String,Integer>();
	
	public int id = -1;
	
	/**
	 * 
	 * @param indexStore
	 * @param field --> field content.
	 * @param title The name of file.
	 */
	public HBaseIndexUpdateWriter(HBaseIndexUpdateStore indexStore)
	{
		this.indexStore = indexStore;
		
	}
	
	/**
	 * Load forward, if you want to update table, must call it first.
	 * @param field
	 * @param fieldTitle
	 * @param title
	 */
	public void load(String field,String fieldTitle,String title){
		String name = "/"+fieldTitle+"/"+title;
		try {
			//this.forward = this.indexStore.loadForward(name, field);
			this.forward = this.indexStore.loadForward(name, field);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		id = this.forward.get(name);
		this.forward.put(name, 1);
		//showMap1(this.forward);
	}
	
	/**
	 * This is for MapReduce.
	 * @author song 11/5/12
	 */
	public HBaseIndexUpdateWriter(){
		indexStore = null;
	}
	
	
	public void addDocument(Document doc, Analyzer analyzer) throws IOException
	{
		
		Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
	    Map<String, byte[]> fieldsToStore = new HashMap<String, byte[]>();
	    update(doc,analyzer,termPositions,fieldsToStore,0);
	    indexStore.indexDocument(new DocumentIndexContext(termPositions,fieldsToStore));
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
	public void update(Document doc, Analyzer analyzer,Map<String,List<Integer>> termPositions,Map<String ,byte[]> fieldsToStore,int tmp) throws IOException{
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
		/**
         * TODO Compare termPosition to map get from loadForward.
         * Attention!! You can't remove element when use iterator.
         */
        if(tmp >0){
        	Iterator<String> it = termPositions.keySet().iterator();
            while(it.hasNext()){
            	String key = it.next();
            	 if(forward.containsKey(key)){
    	            	if(forward.get(key)==termPositions.get(key).size()){
    	            		forward.remove(key);
    	            		it.remove();//Important!!!!!
    	            		termPositions.remove(key);
    	            		//continue;
    	            	}
    	            	else{
    	            		forward.remove(key);
    	            	}
    	            		
    	            }
            }
        }
	}
	
	public void updateDocument(Document doc, Analyzer analyzer) throws IOException{
		/**
		 * TODO something about update.
		 */
		Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
	    Map<String, byte[]> fieldsToStore = new HashMap<String, byte[]>();
	    update(doc, analyzer, termPositions, fieldsToStore,1);
	    indexStore.indexDocument(new DocumentIndexContext(termPositions,fieldsToStore),id,this.forward);
	    termPositions.clear();
	    fieldsToStore.clear();
	}
	
	public void updateDocumentLocal(Term term,Document doc,Analyzer analyzer){
		/**
		 * I want to update document by id, not delete it first.
		 * But, i don't know how to do it now !
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
	
	public void showMap1(Map<String,Integer> map)
	{
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			System.out.print(entry.getKey()+": ");
			System.out.print(entry.getValue());
			System.out.println("");
		}
	}


}
