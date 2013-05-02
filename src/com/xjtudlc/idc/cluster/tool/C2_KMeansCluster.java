package com.xjtudlc.idc.cluster.tool;

import com.xjtudlc.idc.cluster.util.KMeansCallBack;
import com.xjtudlc.idc.config.Config;
import com.xjtudlc.idc.util.FSUtil;

public class C2_KMeansCluster {
	
	public static void main(String args[])
	{
		FSUtil util = new FSUtil();
		util.list(Config.getConfig("path.file.arff"), new KMeansCallBack());
	}

}
