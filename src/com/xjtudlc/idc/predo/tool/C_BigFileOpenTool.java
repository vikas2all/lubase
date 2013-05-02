package com.xjtudlc.idc.predo.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class C_BigFileOpenTool {
	
	public static void main(String args[]) throws IOException{
		String path = "F:\\weibo\\twitter\\tweets200911.txt";
		BufferedReader br = new BufferedReader(new FileReader(new File(path)));
		String line = "";
		while((line=br.readLine())!=null){
			System.out.println(line);
		}
	}

}
