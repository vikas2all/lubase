package com.xjtudlc.idc.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.xjtudlc.idc.config.Config;

public class AnalyzerUtil {
	
	public static final Logger log = Logger.getLogger(AnalyzerUtil.class);
	
	public static Set<String> loadVocabulary()
	{
		Set<String> vocabulary = new HashSet<String>();
		String path = Config.getConfig("path.file.vocabulary");
		//log.info(path);
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(path)));
			String line = "";
			while((line = br.readLine())!=null){
				vocabulary.add(line);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
		}
		return vocabulary;
	}
	
	public static Set<String> loadStopWord()
	{
		Set<String> vocabulary = new HashSet<String>();
		String path = Config.getConfig("path.file.stopword");
		//log.info(path);
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(path)));
			String line = "";
			while((line = br.readLine())!=null){
				vocabulary.add(line);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
		}
		return vocabulary;
	}
	
	public static void main(String args[])
	{
		Set<String> set = loadVocabulary();
		//for(String str : set){
			System.out.println(set.size());
		//}
	}

}
