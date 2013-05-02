package com.xjtudlc.idc.crawler.dao;

public interface IWikiCategoryDao {
	
	public int insertCategory(String title, String category);
	
	public int insertCategory(String category);
	
	public int selectCategory(String category);

}
