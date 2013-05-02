package com.xjtudlc.idc.predo;

public class DocFactory {
	
	public static IDocFactory getDocFactory()
	{
		IDocFactory docFactory = new PDFToDoc();
		return docFactory;
	}

}
