package com.xjtudlc.idc.predo.tool;

import com.xjtudlc.idc.config.Config;
import com.xjtudlc.idc.util.FSUtil;

public class DescriptionToDB {
	
	public static void main(String args[])
	{
		FSUtil util = new FSUtil();
		util.list(Config.getConfig("path.file.description"), new DescriptionCallBack());
	}

}
