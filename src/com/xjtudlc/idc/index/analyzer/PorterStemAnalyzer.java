package com.xjtudlc.idc.index.analyzer;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

/**
 * ��չ��������ȥͣ�ʣ�steming������ʱʹ�ã�����ʱ��ò�Ҫʹ��Steming��
 * @author song
 */
public class PorterStemAnalyzer extends Analyzer {
	
	Analyzer base;//use BaseAnalyzer
	
	public PorterStemAnalyzer(Analyzer base)
	{
		this.base = base;
	}

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		// TODO stemming
		return new PorterStemFilter(//steming
				this.base.tokenStream(fieldName, reader));
	}

}
