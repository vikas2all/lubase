package com.xjtudlc.idc.cluster.util;

import java.io.File;
import java.io.IOException;

import weka.clusterers.SimpleKMeans;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

import com.xjtudlc.idc.util.CallBack;

public class KMeansCallBack implements CallBack<String> {

	@Override
	public boolean execute(String t) {
		// TODO Auto-generated method stub
		File file = new File(t);
		ArffLoader loader = new ArffLoader();
        try {
			loader.setFile(file);
			Instances  ins = loader.getDataSet();
			SimpleKMeans km = new SimpleKMeans();
			km.setNumClusters(2);
			km.buildClusterer(ins);
			Instances temp = km.getClusterCentroids();
			int a[] = km.getClusterSizes();
			for(int i=0;i<a.length;i++){
				System.out.println(a[i]);
			}
			//System.out.println(temp);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
