package com.xjtudlc.idc.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;

import com.xjtudlc.idc.util.LubaseUtil;

public class TFIDFInformation{
	
	//<fileName,<term,frequent>>
	public static Map<String,Map<String,Integer>> tf = new HashMap<String,Map<String,Integer>>();
	public static Map<String,Map<String,Double>> tf_idf = new HashMap<String,Map<String,Double>>();
	public static Map<String,Integer> idf = new HashMap<String,Integer>();
	//DecimalFormat df = new DecimalFormat("0.000000");
	
	/*
	 * TODO 递归调用获得文档集中所有文档词频
	 */
	public  void createTermFreq(Document doc, Analyzer analyzer, String fileName) throws IOException
	{
		//tf.put(fileName, null);
		Map<String,Integer> termf = new HashMap<String,Integer>();
		
		Fieldable field = doc.getField("Content");//
		TokenStream tokens = field.tokenStreamValue();
		
		Reader read = field.readerValue();
		if (tokens == null) {
      	  if(field.stringValue()==null){
    		  tokens = analyzer.tokenStream(field.name(), read);
    	  }else
    		  //System.out.println(field.stringValue()+"###");
    		  tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
        //field.
        }
		tokens.addAttribute(TermAttribute.class);
        while(tokens.incrementToken()){
        	String term = tokens.getAttribute(TermAttribute.class).term();
        	if(termf.get(term)==null){
        		termf.put(term, 1);
        	}else{
        		
        		termf.put(term, termf.get(term)+1);
        	}
        	//termf.put(term, termf.get(term)+1);
        }
        tokens.close();
        tf.put(fileName, termf);
		//return tf;
	}
	
//	public void showTF()
//	{
//		for(Map.Entry<String, Map<String,Double>> entry : tf_idf.entrySet()){
//			System.out.print(entry.getKey()+" ");
//			for(Map.Entry<String, Double> entry2 : entry.getValue().entrySet()){
//				System.out.println(entry2.getKey()+" "+entry2.getValue());
//			}
//		}
//	}
	
	public void showTFIDF()
	{
		for(Map.Entry<String, Map<String,Double>> entry : tf_idf.entrySet()){
			System.out.print(entry.getKey()+" ");
			for(Map.Entry<String, Double> entry2 : entry.getValue().entrySet()){
				System.out.println(entry2.getKey()+" "+entry2.getValue());
			}
		}
	}
	
	public void showIDF()
	{
		for(Map.Entry<String, Integer> entry : idf.entrySet()){
			System.out.println(entry.getKey()+" "+entry.getValue());
		}
	}
	
	/*
	 *TODO 下面一个函数计算，文档集的tf。 
	 */
	
	//<filename, <term, tf>>
	public void getTFInformation()
	{
		for(Map.Entry<String, Map<String,Integer>> entry : tf.entrySet()){
			String fileName = entry.getKey();
			Map<String,Double> map = new HashMap<String,Double>();
			int total = 0;
			for(Map.Entry<String, Integer> entry2 : entry.getValue().entrySet()){
				total = total + entry2.getValue();
			}
			//System.out.println(total+"###");
			for(Map.Entry<String, Integer> entry2 : entry.getValue().entrySet()){
				map.put(entry2.getKey(), (double)entry2.getValue()/total);
			}
			tf_idf.put(fileName, map);
		}
		//showTFIDF();
	}
	/*
	 * 计算每个term存在文档的个数
	 */
	public void getDocNumOfTerm()
	{
		for(Map.Entry<String, Map<String,Integer>> entry : tf.entrySet()){
			for(Map.Entry<String, Integer> entry2 : entry.getValue().entrySet()){
				if(idf.get(entry2.getKey())==null){
					idf.put(entry2.getKey(), 1);
				}else{
					idf.put(entry2.getKey(), idf.get(entry2.getKey())+1);
				}
			}
		}
	}
	
	/*
	 * TODO 得到文档集的tfidf值
	 */
	public void updateIDFInformation()
	{
		int num = tf.size();//the number of files
		//int contain = 0;
		for(Map.Entry<String, Map<String,Double>> entry : tf_idf.entrySet()){
			Map<String,Double> map = entry.getValue();
			for(Map.Entry<String, Double> entry2 : entry.getValue().entrySet()){
				System.out.println(num+"##"+Math.log((double)(num)/(double)(idf.get(entry2.getKey())))+"###");
				BigDecimal   b   =   new   BigDecimal(entry2.getValue()*Math.log((double)(num)/(double)(idf.get(entry2.getKey())+1)));  
				double tmp = b.setScale(6,   BigDecimal.ROUND_HALF_UP).doubleValue(); 
				map.put(entry2.getKey(), tmp);
			}
		}
	}
	
	/*
	 * TODO 选择文档集中最大的weight的词
	 */
	public List<String> getMaxWeightTermList(int N)
	{
		List<String> list = new ArrayList<String>();
		Map<String,Double> map = new HashMap<String,Double>();
		for(Map.Entry<String, Integer> entry : idf.entrySet()){
			String term = entry.getKey();
			Double tmp = 0.0;
			for(Map.Entry<String, Map<String,Double>> entry2:tf_idf.entrySet()){
				Map<String,Double> map2 = entry2.getValue();
				if(map2.containsKey(term)){
					tmp = tmp + map2.get(term);
				}
			}
			map.put(term, tmp);
		}
		List<Entry<String,Double>> list2 = LubaseUtil.sortMapByValue(map);
		for(int i=0;i<N;i++){
			list.add(list2.get(i).getKey());
		}
		return list;
	}
	
	/*
	 * TODO 存储文档集tfidf到文件
	 */
	
	public void storeToFile(List<String> list, String path) throws IOException
	{
		//FSUtil.store(path, list);
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
		bw.write(" ");
		for(int i=0;i<list.size();i++){
			bw.write(list.get(i)+" ");
		}
		bw.newLine();
		bw.flush();
		for(Map.Entry<String, Map<String,Double>> entry : tf_idf.entrySet()){
			bw.write(entry.getKey()+" ");
			Map<String,Double> map = entry.getValue();
			for(int i=0;i<list.size();i++){
				if(map.containsKey(list.get(i))){
					bw.write(map.get(list.get(i))+" ");
				}else{
					bw.write("0 ");
				}
			}
			bw.newLine();
			bw.flush();
		}
		bw.close();
	}
	
	public void process()
	{
		getTFInformation();
		getDocNumOfTerm();
		updateIDFInformation();
	}
}