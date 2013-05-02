package com.xjtudlc.idc.predo.tool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.xjtudlc.idc.config.Config;
import com.xjtudlc.idc.util.FSUtil;

public class C_MergeFilesToOne {
	
	public static void main(String args[]) throws IOException{
		FSUtil util = new FSUtil();
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("E:\\merge.txt")));
		int tmp = util.list(new File("E:\\wikidump\\data"), new MergeFileCallBack(),bw);
		//bw.write(tmp+"");
		bw.flush();
		bw.close();
	}

}
