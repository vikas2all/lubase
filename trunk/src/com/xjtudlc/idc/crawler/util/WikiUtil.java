package com.xjtudlc.idc.crawler.util;

import java.util.ArrayList;



import org.apache.log4j.Logger;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.HasAttributeFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;

import com.xjtudlc.idc.config.Config;
import com.xjtudlc.idc.crawler.dao.IWikiCategoryDao;
import com.xjtudlc.idc.crawler.dao.WikiCategoryDaoImpl;
import com.xjtudlc.idc.util.CallBack;

public class WikiUtil {
	
	private static final Logger log = Logger.getLogger(WikiUtil.class);
	
	int ceng = 0;//控制爬取链接层数，否则将出现无法控制
	int threshold = Integer.parseInt(Config.getConfig("threshold.crawler.layer"));
	
	public IWikiCategoryDao dao = new WikiCategoryDaoImpl();
	
	public ArrayList<WikiPage> listPages(String url) throws Exception
	{
		log.error("***"+url+"***");
		ArrayList<WikiPage> list = new ArrayList<WikiPage>();
		//Subcategories
		NodeList nodelist = null;
		Parser parser = new Parser(url);
		parser.setEncoding("UTF-8");
		NodeFilter textFilter = new AndFilter(new TagNameFilter("div"), new HasAttributeFilter("id", "mw-subcategories"));
		NodeFilter textFilter2 = new AndFilter(new TagNameFilter("div"), new HasAttributeFilter("id", "mw-pages"));
		nodelist = parser.parse(textFilter);
		if(nodelist.size()==1){
			Node node = nodelist.elementAt(0);
			parser = new Parser(node.toHtml());//重新parse，找到最里面的a标签
			parser.setEncoding("UTF-8");
			nodelist = parser.parse(new TagNameFilter("a"));
			for(int i=0;i<nodelist.size();i++){
				WikiPage page = new WikiPage();
				String tmp = nodelist.elementAt(i).getText();
				page.setUrl("http://en.wikipedia.org"+tmp.substring(tmp.indexOf("/"),tmp.lastIndexOf("\"")));
				page.setType("category");
				list.add(page);
			}
		}
		//pages
		parser = new Parser(url);
		nodelist = parser.parse(textFilter2);
		System.out.println(nodelist.size()+"@@@");
		if(nodelist.size()==1){
			Node node = nodelist.elementAt(0);
			//node = node.getChildren().elementAt(2);
			parser = new Parser(node.toHtml());//重新parse，找到最里面的a标签
			parser.setEncoding("UTF-8");
			nodelist = parser.parse(new TagNameFilter("a"));
			for(int i=2;i<nodelist.size();i++){
				WikiPage page = new WikiPage();
				String tmp = nodelist.elementAt(i).getText();
				page.setUrl("http://en.wikipedia.org"+tmp.substring(tmp.indexOf("/"),tmp.lastIndexOf("\" title")));
				//System.out.println(tmp.substring(tmp.indexOf("/"),tmp.lastIndexOf("\" title")));
				page.setType("pages");
				list.add(page);
			}
		}
		return list;
	}
	
	public boolean isCategories(String url)
	{
		if(url.startsWith("http://en.wikipedia.org/wiki/Category:"))
			return true;
		else return false;
	}
	
	public void list(String url, CallBack<String> call) throws Exception
	{
		ceng ++;
		if(ceng>threshold){
			ceng --;
			return ;
		}
		log.error("****ceng: "+ceng+"***");
		if(dao.selectCategory(url)==1){
			ceng --;
			return ;
		}
		//System.out.println("*********");
		if(isCategories(url)){
			ArrayList<WikiPage> list = listPages(url);
			dao.insertCategory(url);
			for(WikiPage pages : list){
				if(pages.getType().equals("pages")){
					dao.insertCategory(pages.getUrl(), url);
					call.execute(pages.getUrl());
				}else{
					list(pages.getUrl(),call);
				}
			}
		}else if(!url.startsWith("http://en.wikipedia.org/wiki/Template:")){
			dao.insertCategory(url, null);
			call.execute(url);
		}
		ceng --;
	}
	
	public static void main(String args[])
	{
		WikiUtil wu = new WikiUtil();
		try {
			System.out.println(wu.isCategories("http://en.wikipedia.org/wiki/Algorithms_and_data_structures"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
