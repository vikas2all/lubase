package com.xjtudlc.idc.index.analyzer;

import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

import com.xjtudlc.idc.cluster.util.AnalyzerUtil;

public class StopWordAnalyzer extends Analyzer {
	
	Analyzer analyzer;
	Set<String> stopWords;
	
	public StopWordAnalyzer(Analyzer base, Set<String> stopWords)
	{
		this.analyzer = base;
		this.stopWords = stopWords;
	}

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		// TODO Auto-generated method stub
		return new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(Version.LUCENE_30), //stop word
						this.analyzer.tokenStream(fieldName, reader)
				, this.stopWords, false);
	}

}
