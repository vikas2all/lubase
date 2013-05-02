package com.xjtudlc.idc.crawler.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.xjtudlc.idc.util.ConnectDB;

public class WikiCategoryDaoImpl implements IWikiCategoryDao {

	@Override
	public int insertCategory(String title, String category) {
		// TODO Auto-generated method stub
		Connection con = ConnectDB.getConnection();
		int tmp = 0;
		String sql = "insert into wikipage(title,category) values (?,?)";
		try {
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, title);
			ps.setString(2, category);
			tmp = ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tmp;

	}

	@Override
	public int insertCategory(String category) {
		// TODO Auto-generated method stub
		Connection con = ConnectDB.getConnection();
		int tmp = 0;
		String sql = "insert into wikicategory(category) values (?)";
		try {
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, category);
			tmp = ps.executeUpdate();
			ps.close();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tmp;
	}

	@Override
	public int selectCategory(String category) {
		// TODO Auto-generated method stub
		Connection con = ConnectDB.getConnection();
		int tmp = 0;
		String sql = "select * from wikicategory where category=?";
		try {
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, category);
			ResultSet rs = ps.executeQuery();
			if(rs.next())tmp = 1;
			ps.close();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tmp;
	}

}
