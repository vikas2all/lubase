package com.xjtudlc.idc.index.analyzer;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class VocabularyFilter extends TokenFilter {
	
	private TermAttribute termAttr;
	private Set<String> vocabulary;

	protected VocabularyFilter(TokenStream input,Set<String> vocabulary) {
		super(input);
		termAttr = addAttribute( TermAttribute.class );
		this.vocabulary = vocabulary;
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean incrementToken() throws IOException {
		// TODO Auto-generated method stub
		while(input.incrementToken())
		{
			String term = termAttr.term();
			if(vocabulary.contains(term))
			{
				return true;
			}
		}
		return false;
	}

}
