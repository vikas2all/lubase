package com.xjtudlc.idc.index.analyzer;

import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

/*
 * 增加术语表filter，去掉不属于术语表中的词
 */

public class VocabularyAnalyzer extends Analyzer {
	
	Analyzer analyzer;
	Set<String> vocabulary;
	
	public VocabularyAnalyzer(Analyzer analyzer,Set<String> vocabulary)
	{
		this.analyzer = analyzer;
		this.vocabulary = vocabulary;
	}

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		// TODO Auto-generated method stub
		return new VocabularyFilter(this.analyzer.tokenStream(fieldName, reader),vocabulary);
	}

}
