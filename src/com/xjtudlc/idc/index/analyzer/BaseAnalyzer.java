package com.xjtudlc.idc.index.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;

/**
 * �����������������������ơ�Ӣ����ĸ���ơ�Сдת����
 * @author song
 */
public class BaseAnalyzer extends Analyzer {

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		// TODO Auto-generated method stub
		return new LengthFilter( // filter words those are too short (length<=1)
                new StandardFilter( new ExtendedCharTokenizer( reader ) )// filter non-letter token stream and transform it into lowercase
                , 3, Integer.MAX_VALUE ); // LengthFilter
	}

}
