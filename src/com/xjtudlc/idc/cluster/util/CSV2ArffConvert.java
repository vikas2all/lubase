package com.xjtudlc.idc.cluster.util;

import java.io.File;
import java.io.IOException;

import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;

public class CSV2ArffConvert {
	
	/*
	 * convert csv to arff.
	 */
	
	public void convert(File csv, File arff) throws IOException
	{
		 CSVLoader loader = new CSVLoader();
		 loader.setSource(csv);
		 Instances data = loader.getDataSet();
		 
		 // save ARFF
		 ArffSaver saver = new ArffSaver();
		 saver.setInstances(data);
		 saver.setFile(arff);
		 // saver.setDestination(f);
		 saver.writeBatch();
	}
	
	public static void main(String args[]) throws IOException
	{
		String csv = "E://Lubase//csv//tfidf_2222_30.csv";
		String arff = "E://1.arff";
		File f = new File(arff);
		CSVLoader loader = new CSVLoader();
		loader.setSource(new File(csv));
		Instances data = loader.getDataSet();
		 
	    // save ARFF
		ArffSaver saver = new ArffSaver();
		saver.setInstances(data);
		saver.setFile(f);
		// saver.setDestination(f);
		saver.writeBatch();
	}

}
