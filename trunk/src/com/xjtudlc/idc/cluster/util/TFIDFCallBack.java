package com.xjtudlc.idc.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.xjtudlc.idc.cluster.TFIDFInformation;
import com.xjtudlc.idc.index.analyzer.BaseAnalyzer;
import com.xjtudlc.idc.index.analyzer.PorterStemAnalyzer;
import com.xjtudlc.idc.index.analyzer.StopWordAnalyzer;
import com.xjtudlc.idc.index.analyzer.VocabularyAnalyzer;
import com.xjtudlc.idc.util.CallBack;
import com.xjtudlc.idc.util.LubaseUtil;

public class TFIDFCallBack implements CallBack<String> {
	
	public static final Logger log = Logger.getLogger(TFIDFCallBack.class);
	
	TFIDFInformation tFIDFInformation;
	
	public Set<String> stopWords = AnalyzerUtil.loadStopWord();
	
	public TFIDFCallBack(TFIDFInformation tFIDFInformation)
	{
		this.tFIDFInformation = tFIDFInformation;
	}

	@Override
	public boolean execute(String file) {
		// TODO Auto-generated method stub
		File f = new File(file);
		String fileName = f.getName();
		Document doc = new Document();
		FileInputStream is;
		try {
			is = new FileInputStream(f);
			Reader reader = new BufferedReader(new InputStreamReader(is));
			Field f1 = new Field("Content",LubaseUtil.ReaderToString(reader),Field.Store.YES, Field.Index.ANALYZED);
			doc.add(f1);
			tFIDFInformation.createTermFreq(doc, new VocabularyAnalyzer(new PorterStemAnalyzer(new StopWordAnalyzer(new BaseAnalyzer(),stopWords)),AnalyzerUtil.loadVocabulary()), fileName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
			e.printStackTrace();
		}
		
		
		return false;
	}

}
