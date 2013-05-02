package com.xjtudlc.idc.cluster.tool;

import java.io.File;

import com.xjtudlc.idc.cluster.util.CSV2ArffCallBack;
import com.xjtudlc.idc.config.Config;
import com.xjtudlc.idc.util.FSUtil;

public class C1_ConvertCSV2Arff {
	
	public static void main(String args[])
	{
		FSUtil util = new FSUtil();
		util.list(new File(Config.getConfig("path.file.csv")), new CSV2ArffCallBack());
	}

}
