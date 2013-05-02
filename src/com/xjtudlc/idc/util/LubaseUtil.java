package com.xjtudlc.idc.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


public class LubaseUtil {
	
	private static final Logger log = Logger.getLogger(LubaseUtil.class);
	
	public static String ReaderToString(Reader reader) throws IOException
	{
		BufferedReader br = new BufferedReader(reader);
	    StringBuffer buffer = new StringBuffer();
		String line = "";
		while ((line = br.readLine())!= null){
			//System.out.println(line);
		    buffer.append(line+" ");
		}
		return buffer.toString();
	}
	
	public static int[] encode(byte[] out)
	{
		int len = Bytes.toInt(out,0);
		int a[] = new int[len];
		for(int i=0;i<len;i++){
			a[i] = Bytes.toInt(out,(i+1)*Bytes.SIZEOF_INT);
		}
		return a;
	}
	
	public static void byteToString()
	{
		String str = "2222";
		//byte b[] = {\x00\x00\x08\xAE};
		//Bytes.toLong(b);
		byte b[] = Bytes.toBytes(str);
		for(int i=0;i<4;i++){
			System.out.println(b[i]);
		}
	}
	
	public static List<Entry<String, Double>> sortMapByValue(  
            Map<String, Double> keywordMap) {  
        List<Entry<String, Double>> arrayList = new ArrayList<Entry<String, Double>>(  
                keywordMap.entrySet());  
        Collections.sort(arrayList, new Comparator<Entry<String, Double>>() {  
            public int compare(Entry<String, Double> e1,  
                    Entry<String, Double> e2) {  
                return (e2.getValue()).compareTo(e1.getValue());  
            }  
        });  
       
        return arrayList;  
    }  
	
	public static String[] splitRow(String row){
		String str[] = row.split("/");
		return new String[]{str[1],str[2]};
	}
	
	public static void main(String args[]) throws IOException
	{
//		List<Integer> list = new ArrayList<Integer>();
//		list.add(22);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		list.add(2);
//		list.add(3);
//		byte[] out = new byte[(list.size()+1) * Bytes.SIZEOF_INT];
//        for (int i = 0; i < list.size(); ++i) {
//            Bytes.putInt(out, (i ) * Bytes.SIZEOF_INT, list.get(i).intValue());
//        }
//        System.out.println(Bytes.toInt(out));
//		int a[] = encode(out);
//	    for(int i=0;i<a.length;i++){
//	    	log.error(a[i]);
//	    }
		String str = "hello world java test nice hello java wo";
		System.out.println(str.indexOf("@@"));
		//System.out.println(str.substring(0,str.indexOf("@@")));
 	}

}
