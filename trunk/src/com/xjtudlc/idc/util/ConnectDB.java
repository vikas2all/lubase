package com.xjtudlc.idc.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;


public class ConnectDB {
	private String driver = "";
	private String dbURL = "";
	private String user = "";
	private String password = "";
	private static ConnectDB factory = null;

	private ConnectDB() throws Exception {
		java.net.URL url = Thread.currentThread().getContextClassLoader().getResource("mysql.properties");
		Properties p = new Properties();
		try{
			p.load(url.openStream());
		}catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String dbusername = p.getProperty("dbUserName");
		String dbPassword = p.getProperty("dbPassword");
		//System.out.println(dbusername+"@@@@@@@@"+dbPassword);
		driver = "com.mysql.jdbc.Driver";
		dbURL = p.getProperty("connectionURL");
		user = dbusername;
		password = dbPassword;
	}

	public static Connection getConnection() {
		Connection conn = null;
		if (factory == null) {
			try {
				factory = new ConnectDB();
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
		try {
			Class.forName(factory.driver);
			conn = DriverManager.getConnection(factory.dbURL, factory.user,factory.password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
	


}
