package com.xjtudlc.idc.predo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

public class PDFToDoc implements IDocFactory {

	@Override
	public void getTxtFile(String pdfFile, String txtFile) throws IOException {
		// TODO Auto-generated method stub
		PDDocument pd = null;
		String encoding = "UTF-8";
		  // 开始提取页数
	    int startPage = 1;
		  // 结束提取页数
		int endPage = Integer.MAX_VALUE;
		Writer output = null;
		pd = PDDocument.load(pdfFile);
		PDFTextStripper stripper=new PDFTextStripper();
		output = new OutputStreamWriter(new FileOutputStream(txtFile), encoding);
		stripper.setStartPage(startPage);
		stripper.setEndPage(endPage);
		stripper.writeText(pd, output);
	    try {
			if (output != null) {
			   output.close();
			}
			if (pd!=null){  
			   pd.close();
			}
			}catch (IOException e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		}        

	}

}
