package com.xjtudlc.idc.predo.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import com.xjtudlc.idc.util.CallBack;

public class MergeFileCallBack implements CallBack<File> {

	@Override
	public boolean execute(File f) {
		// TODO Auto-generated method stub
		try {
			String line = "";
			StringBuffer content = new StringBuffer();
			BufferedReader br = new BufferedReader(new FileReader(f));
			while((line = br.readLine())!=null){
				line = line.replaceAll("\n", " ");
				content.append(line);
			}
			br.close();
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("E:\\merge.txt")));
			bw.write(content.toString());
			bw.newLine();
			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean execute(File f, BufferedWriter bw) {
		// TODO Auto-generated method stub
		try {
			String line = "";
			StringBuffer content = new StringBuffer();
			String fileName = f.getName();
			content.append(fileName+"@@");
			BufferedReader br = new BufferedReader(new FileReader(f));
			while((line = br.readLine())!=null){
				content.append(line+" ");
			}
			br.close();
			bw.write(content.toString());
			bw.newLine();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
