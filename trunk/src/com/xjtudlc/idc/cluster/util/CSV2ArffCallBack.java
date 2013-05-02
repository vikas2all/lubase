package com.xjtudlc.idc.cluster.util;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.xjtudlc.idc.util.CallBack;

public class CSV2ArffCallBack implements CallBack<File> {
	
	private static final Logger log = Logger.getLogger(CSV2ArffCallBack.class);
	private static final Pattern fullNamePattern = Pattern.compile("csv$");
	CSV2ArffConvert convert = new CSV2ArffConvert();

	@Override
	public boolean execute(File t) {
		// TODO Auto-generated method stub
		Matcher m = fullNamePattern.matcher( t.getAbsolutePath() );
        if (!m.find())
        {   //·ÇcsvÎÄ¼þ
        	log.error("not csv!");
            return false;
        }
        String arff = m.replaceFirst( "arff" );
        try {
			convert.convert(t, new File(arff));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.log(Level.INFO, t.getAbsoluteFile(), e);
			//e.printStackTrace();
		}
		return true;
	}

}
