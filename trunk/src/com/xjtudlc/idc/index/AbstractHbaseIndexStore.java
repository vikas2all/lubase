package com.xjtudlc.idc.index;

import java.io.IOException;

import org.apache.lucene.index.Term;

public abstract class AbstractHbaseIndexStore {
	
	public abstract void commit() throws IOException;
	
	public abstract void close() throws IOException;
	
	public abstract void indexDocument(final DocumentIndexContext documentIndexContext) throws IOException;
	
	public abstract void indexDocument(final DocumentIndexContext documentIndexContext,final int docId) throws IOException;
	
	public abstract int delDocument(Term term) throws IOException;

}
