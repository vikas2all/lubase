package com.xjtudlc.idc.config;

import java.util.Properties;

import org.apache.log4j.Logger;

public class Config {
	
	private static final Logger log = Logger.getLogger(Config.class);
	
	public static String getConfig(String conf)
	{
		java.net.URL url = Thread.currentThread().getContextClassLoader().getResource("config.properties");
		Properties p = new Properties();
		//log.error(p.toString());
		try {
			p.load(url.openStream());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error(e.getMessage());
			//e.printStackTrace();
		}
		return p.getProperty(conf);
	}
	
	public static void main(String args[])
	{
		//Config conf = new Config();
		System.out.println(Config.getConfig("path.file.csv"));
	}

}
