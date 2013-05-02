package com.xjtudlc.idc.predo.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class C_WikiDumpStatistic {
	
	/**
	 * 根据file.py得到的结果文件对WikiDump进行统计
	 */
	public Map<Integer,Integer> map = new HashMap<Integer,Integer>();
	double total = 0;
	
	public void wikiDump(String path){
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(path)));
			String line = "";
			while((line = br.readLine())!=null){
				System.out.println(line);
				String str[] = line.split(" ");
				if(str.length==2){
					statistic(Integer.parseInt(str[1]));
				}
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void statistic(int tmp){
		total += tmp;
		int size = tmp/1024;
		if(size>=100)size=100;
		if(map.containsKey(size)){
			map.put(size, map.get(size)+1);
		}else{
			map.put(size, 1);
		}
	}
	
	public void saveResult(String path){
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
			for(Map.Entry<Integer, Integer> entry:map.entrySet()){
				String line = entry.getKey()+" "+entry.getValue();
				bw.write(line);
				bw.newLine();
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]){
		C_WikiDumpStatistic w = new C_WikiDumpStatistic();
		w.wikiDump("E:\\Lubase\\result.log");
		w.saveResult("E:\\Lubase\\wikistatistic.result");
		System.out.println(w.total+"####");
		//w.wikiDump("E:\\dafei.log");
	}

}
