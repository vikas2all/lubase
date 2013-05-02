package com.xjtudlc.idc.predo.tool;

import java.io.IOException;
import java.util.ArrayList;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public class XMLParser {
	
	 public void parser(String fileName, ContentHandler xml)   
	    {          
	        try  
	        {   
	            //通过指定解析器的名称来动态加载解析器   
	            XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");   
	            //处理内容前要注册内容管理器   
	            parser.setContentHandler(xml);   
	            //开始解析文档   
	            parser.parse(fileName);   
	        }   
	        catch (IOException e)   
	        {   
	            e.printStackTrace();   
	        }   
	        catch (SAXException e)   
	        {   
	            e.printStackTrace();   
	        }   
	    }   
	 
	 public static void main(String args[])
	 {
		 XMLParser parser = new XMLParser();
		 XMLContentHandler xml = new XMLContentHandler();
		 parser.parser("E:\\Lubase\\description\\0\\3.xml",xml);
		 ArrayList<String> tagList = xml.list;
		 System.out.println(tagList.size()+tagList.get(0));
	 }

}
