package com.xjtudlc.idc.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.apache.log4j.Logger;

import com.xjtudlc.idc.predo.tool.MergeFileCallBack;

public class FSUtil {
	
	public static final Logger log = Logger.getLogger(FSUtil.class);
	
	public void list(File f, CallBack<File> call)
	{
		if(f.isDirectory()){
			for(File fs : f.listFiles()){
				if(fs.isFile()){
					call.execute(fs);
				}else{
					list(fs,call);
				}
			}
		}else{
			call.execute(f);
		}
	}
	
	/**
	 * Niubility!!
	 * @param File f
	 * @param MergeFileCallBack call
	 * @param BufferWriter bw
	 * @return int tmp
	 * @author song 
	 */
	public int list(File f, MergeFileCallBack call, BufferedWriter bw)
	{
		int tmp = 0;
		if(f.isDirectory()){
			for(File fs : f.listFiles()){
				if(fs.isFile()){
					call.execute(fs,bw);
					tmp ++;
				}else{
					tmp = tmp + list(fs,call,bw);
				}
			}
		}else{
			call.execute(f,bw);
			tmp ++;
		}
		return tmp;
	}
	
	public void list(String f,CallBack<String> call)
	{
		File path = new File(f);
		if(path.isDirectory()){
			for(File fs : path.listFiles()){
				if(fs.isFile()){
					call.execute(fs.getAbsolutePath());
				}else{
					list(fs.getAbsolutePath(),call);
				}
			}
		}else{
			call.execute(f);
		}
	}
	
	public static void store(String path , List<String> list)
	{
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
			//bw.write("\t");
			for(int i=0;i<list.size();i++){
				bw.write(list.get(i)+" ");
			}
			 bw.newLine();
             bw.flush();
             bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error(e.getMessage());
		}
	}
	
	public static void store(String path, String content)
	{
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
			//bw.write("\t");
			 bw.write(content);
			 bw.newLine();
             bw.flush();
             bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			log.error(e.getMessage());
		}
	}

}
