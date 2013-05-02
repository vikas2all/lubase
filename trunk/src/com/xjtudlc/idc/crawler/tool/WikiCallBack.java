package com.xjtudlc.idc.crawler.tool;

import com.xjtudlc.idc.crawler.WikiParser;
import com.xjtudlc.idc.util.CallBack;

public class WikiCallBack implements CallBack<String> {

	@Override
	public boolean execute(String t) {
		// TODO Auto-generated method stub
		try {
			WikiParser.wikiParser(t);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		return true;
	}

}
