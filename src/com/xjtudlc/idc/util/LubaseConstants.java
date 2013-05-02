package com.xjtudlc.idc.util;

import org.apache.hadoop.hbase.util.Bytes;

public interface LubaseConstants {
	
	static final String fm = "fm";
	static final String fmvc = "fm.vc";
	static final byte[] FAMILY = Bytes.toBytes(fm);
	static final byte[] FAMILY_VECTOR = Bytes.toBytes(fmvc);
	static final byte[] DOCNUM = Bytes.toBytes("docNum");
	static final byte[] DOCNUM_QUALIFIER = Bytes.toBytes("total");
	static final byte[] MAXID = Bytes.toBytes("maxId");
	static final byte[] MAXID_QUALIFIER = Bytes.toBytes("id");

}
