package com.xjtudlc.idc.predo;

import java.io.File;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.xjtudlc.idc.predo.DocFactory;
import com.xjtudlc.idc.predo.IDocFactory;

public class DocFactoryTest {
	
	Logger logger  =  Logger.getLogger(DocFactoryTest.class);
	
	ArrayList<File> list = new ArrayList<File>();
	
	public void getPdfFile(File path)
	{
		if(path.isDirectory()){
			File[] files = path.listFiles();
			for(File f : files){
				getPdfFile(f);
			}
		}else{
			list.add(path);
		}
	}
	
	public static void main(String args[])
	{
		IDocFactory factory = DocFactory.getDocFactory();
		DocFactoryTest test = new DocFactoryTest();
		test.getPdfFile(new File("E://Lubase//paper//"));
		for(int i=0;i<test.list.size();i++){
			System.out.println(test.list.get(i).getAbsolutePath()+"@@"+test.list.get(i).getName());
			try {
				factory.getTxtFile(test.list.get(i).getAbsolutePath(), "E://Lubase//txt11//"+test.list.get(i).getName()+".txt");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				test.logger.error("Error: "+test.list.get(i).getName());
				continue;
			}
		}
	}

}
