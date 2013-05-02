package com.xjtudlc.idc.predo.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.xjtudlc.idc.util.ConnectDB;

public class DescriptionDaoImpl implements IDescriptionDao {

	@Override
	public int insertDescription(String fileName, String classify) {
		// TODO Auto-generated method stub
		Connection con = ConnectDB.getConnection();
		int tmp = 0;
		String sql = "insert into description(name,classify) values (?,?)";
		try {
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, fileName);
			ps.setString(2, classify);
			tmp = ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tmp;
	}

}
