package com.xjtudlc.idc.crawler;

import org.apache.log4j.Logger;

import com.xjtudlc.idc.crawler.tool.WikiCallBack;
import com.xjtudlc.idc.crawler.util.WikiUtil;

public class WikiCrawler {
	
	private static final Logger log = Logger.getLogger(WikiCrawler.class);
	
	public static void main(String args[])
	{
		if(args.length<1){
			//http://en.wikipedia.org/wiki/Category:Areas_of_computer_science
			System.out.println("we need at least one arg!!");
		}else{
			WikiUtil util = new WikiUtil();
			try {
				util.list(args[0], new WikiCallBack());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				log.error(e.getMessage());
			}
		}
	}

}
