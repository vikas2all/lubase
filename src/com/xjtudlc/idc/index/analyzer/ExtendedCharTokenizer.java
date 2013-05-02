package com.xjtudlc.idc.index.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.CharTokenizer;

public final class ExtendedCharTokenizer extends CharTokenizer {

	 public ExtendedCharTokenizer( Reader in )
	 {
	        super( in );
	 }
	 
	 @Override
	 protected char normalize( char c )
	 {
	     return Character.toLowerCase( c );
	 }
	@Override
	protected boolean isTokenChar(char c) {
		// TODO Auto-generated method stub
		if(c>='a'&&c<='z' || c>='A'&&c<='Z')
			return true;
		else return false;
		//return Character.isLetter( c );
	}

}
