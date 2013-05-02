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
	            //ͨ��ָ������������������̬���ؽ�����   
	            XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");   
	            //��������ǰҪע�����ݹ�����   
	            parser.setContentHandler(xml);   
	            //��ʼ�����ĵ�   
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
