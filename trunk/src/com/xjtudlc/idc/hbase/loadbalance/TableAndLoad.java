package com.xjtudlc.idc.hbase.loadbalance;

import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;

public class TableAndLoad {
	private String TableName;
	private List<HRegionInfo> tableHri;
	
	public TableAndLoad(String TableName,List<HRegionInfo> tableHri){
		this.TableName = TableName;
		this.tableHri = tableHri;
	}
	
	public String getTableName() {
		return TableName;
	}
	public void setTableName(String tableName) {
		TableName = tableName;
	}
	public List<HRegionInfo> getTableHri() {
		return tableHri;
	}
	public void setTableHri(List<HRegionInfo> tableHri) {
		this.tableHri = tableHri;
	}
	
	public boolean equals(Object o)
	{
		if(this==o)return true;
		if(this.getClass() == o.getClass()){
			TableAndLoad t = (TableAndLoad)o;
			if(this.getTableName().equals(t.getTableName()))
				return true;
		}
		return false;
	}

}
