package com.xjtudlc.idc.predo.tool;

import java.io.File;
import java.util.ArrayList;

import com.xjtudlc.idc.predo.dao.DescriptionDaoImpl;
import com.xjtudlc.idc.predo.dao.IDescriptionDao;
import com.xjtudlc.idc.util.CallBack;

public class DescriptionCallBack implements CallBack<String> {

	@Override
	public boolean execute(String t) {
		// TODO Auto-generated method stub
		boolean tmp = false;
		File f = new File(t);
		String fileName = f.getName();
		IDescriptionDao dao = new DescriptionDaoImpl();
		XMLParser parser = new XMLParser();
		XMLContentHandler handler = new XMLContentHandler();
		parser.parser(t, handler);
		ArrayList<String> tagList = handler.list;
		for(int i=0;i<tagList.size();i++){
			if(dao.insertDescription(fileName, tagList.get(i))==1)
				tmp = true;
		}
		return tmp;
	}

}
