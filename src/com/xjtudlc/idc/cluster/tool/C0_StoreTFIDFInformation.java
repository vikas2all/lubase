package com.xjtudlc.idc.cluster.tool;

import java.io.IOException;

import com.xjtudlc.idc.config.Config;
import com.xjtudlc.idc.util.FSUtil;
import com.xjtudlc.idc.cluster.TFIDFInformation;
import com.xjtudlc.idc.cluster.util.TFIDFCallBack;

public class C0_StoreTFIDFInformation {
	
	public static void main(String args[])
	{
		FSUtil util = new FSUtil();
		TFIDFInformation tf = new TFIDFInformation();
		util.list(Config.getConfig("path.file.tfidf"), new TFIDFCallBack(tf));
		tf.process();
		//tf.showTFIDF();
		try {
			tf.storeToFile(tf.getMaxWeightTermList(30), Config.getConfig("path.file.storeTF"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
