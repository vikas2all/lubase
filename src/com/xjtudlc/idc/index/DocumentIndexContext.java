package com.xjtudlc.idc.index;

import java.util.List;
import java.util.Map;

public class DocumentIndexContext {
	
	public final Map<String,List<Integer>> termPositionVctor;//<term, <list of term position>> 
	
	public final Map<String,byte[]> storeFiled;//<filedName, content>
	
	public DocumentIndexContext(Map<String,List<Integer>> termPositionVctor,Map<String,byte[]> storeFiled){
		this.termPositionVctor = termPositionVctor;
		this.storeFiled = storeFiled;
	}

}
