package com.xjtudlc.idc.crawler;

import org.apache.log4j.Logger;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.HasAttributeFilter;
import org.htmlparser.filters.NotFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;

import com.xjtudlc.idc.config.Config;
import com.xjtudlc.idc.crawler.dao.IWikiCategoryDao;
import com.xjtudlc.idc.crawler.dao.WikiCategoryDaoImpl;
import com.xjtudlc.idc.util.FSUtil;

public class WikiParser {
	
	private static final Logger log = Logger.getLogger(WikiParser.class);
	
	public static void wikiParser(String url) throws Exception
	{
		StringBuffer sb = new StringBuffer();
		//String category = "";
		NodeList nodelist = null;
		Parser parser = new Parser(url);
		parser.setEncoding("UTF-8");
		NodeFilter textFilter = new AndFilter(new TagNameFilter("div"), new HasAttributeFilter("id", "mw-content-text"));
		NodeFilter notTable = new NotFilter(new AndFilter(new TagNameFilter("table"), new HasAttributeFilter("class", "navbox")));
		nodelist = parser.parse(textFilter);
		if(nodelist.size()==1){
			Node node = nodelist.elementAt(0);
			nodelist = node.getChildren().extractAllNodesThatMatch(notTable);
			for(int i=0;i<nodelist.size();i++){
				sb.append(nodelist.elementAt(i).toPlainTextString().trim());
			}
		}else{
			log.error("There are some mistakes in wiki parser!");
		}
//		parser = new Parser(url);
//		nodelist = parser.parse(new AndFilter(new TagNameFilter("div"), new HasAttributeFilter("id", "mw-normal-catlinks")));
//		if(nodelist.size()==1){
//			Node node = nodelist.elementAt(0);
//			category = node.toPlainTextString();
//		}else{
//			log.error("There are some mistakes in wiki parser!");
//		}
		String title = url.substring(url.lastIndexOf("/"),url.length())+".txt";
		FSUtil.store(Config.getConfig("path.file.wiki")+url.substring(url.lastIndexOf("/"),url.length())+".txt", sb.toString());
		//log.debug(url.substring(url.lastIndexOf("/"),url.length())+".txt is download.");
		//IWikiCategoryDao idao = new WikiCategoryDaoImpl();
		//idao.insertCategory(title, category);
		System.out.println(title + " is done.");

	}
	
	public static void main(String args[])
	{
		try {
			wikiParser("http://en.wikipedia.org/wiki/Kinetic_heap");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("@@@");
	}

}
