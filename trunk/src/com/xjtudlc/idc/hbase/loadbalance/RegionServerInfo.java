package com.xjtudlc.idc.hbase.loadbalance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;

/**
 * RegionServer状态函数测试类
 * @author song
 *
 */
public class RegionServerInfo {
	
	public Map<ServerName, List<HRegionInfo>> getRegionInfo() throws IOException{
		Map<ServerName,List<HRegionInfo>> map = new HashMap<ServerName,List<HRegionInfo>>();
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "hbase0:60000");
		config.set("hbase.zookeeper.quorum", "hbase1,hbase2,hbase3");
		HBaseAdmin admin = new HBaseAdmin(config);
		MetaScanner scan = new MetaScanner();
		HTableDescriptor descriptor[] = admin.listTables();
		for(int i=0;i<descriptor.length;i++){
			NavigableMap<HRegionInfo,ServerName> nMap = scan.allTableRegions(config, descriptor[i].getName(), false);//
			showNavigableMap(nMap);
			//将NavigableMap<HRegionInfo,ServerName>转换成Map<ServerName, List<HRegionInfo>>
			for(Entry<HRegionInfo,ServerName> entry:nMap.entrySet()){
				if(map!=null){
					int tmp = 0;
					for(Entry<ServerName,List<HRegionInfo>> entry2:map.entrySet()){
						if(entry2.getKey().equals(entry.getValue())&&entry2.getKey().compareTo(entry.getValue())==0){//两个ServerName相等
							List<HRegionInfo> lr = entry2.getValue();
							if(lr==null){
								lr = new ArrayList<HRegionInfo>();
								lr.add(entry.getKey());
							}else{
								lr.add(entry.getKey());
							}
							tmp = 1;
							break;
						}
					}
					if(tmp==0){
						ArrayList<HRegionInfo> alr = new ArrayList<HRegionInfo>();
						alr.add(entry.getKey());
						map.put(entry.getValue(), alr);
					}
				}else{
					ArrayList<HRegionInfo> infoList = new ArrayList<HRegionInfo>();
					infoList.add(entry.getKey());
					map.put(entry.getValue(), infoList);
				}
			}
		}
		
		return map;
	}
	
	/**
	 * 将Map<ServerName, List<HRegionInfo>>转换为Map<TableName,Map<ServerName,List<HRegionInfo>>>
	 */
	public Map<String,List<Map<ServerName,List<HRegionInfo>>>> tableInfo(Map<ServerName, List<HRegionInfo>> map){
		Map<String,List<Map<ServerName,List<HRegionInfo>>>> tableInfo = new HashMap<String,List<Map<ServerName,List<HRegionInfo>>>>();
		for(Entry<ServerName,List<HRegionInfo>> entry:map.entrySet()){
			for(HRegionInfo hri:entry.getValue()){
				String tableName = hri.getTableNameAsString();
				if(tableInfo!=null){
					if(tableInfo.containsKey(tableName)){
						List<Map<ServerName,List<HRegionInfo>>> ltmap = tableInfo.get(tableName);
						//因为map中的key是对象，所以应该不能用containsKey，而应该进行遍历比较
						int tmp = 0;
						for(int i=0;i<ltmap.size();i++){
							Map<ServerName,List<HRegionInfo>> tmap = ltmap.get(i);
							for(Entry<ServerName,List<HRegionInfo>> entry2:tmap.entrySet()){
								if(entry2.getKey().equals(entry.getKey())&&entry2.getKey().compareTo(entry.getKey())==0){
									List<HRegionInfo> li = entry2.getValue();
									li.add(hri);
									tmp = 1;
									break;
								}
							}
						}
						if(tmp==0){//没有这个ServerName
							List<HRegionInfo> hl = new ArrayList<HRegionInfo>();
						    hl.add(hri);
						    Map<ServerName,List<HRegionInfo>> smap = new HashMap<ServerName,List<HRegionInfo>>();
						    smap.put(entry.getKey(), hl);
						    ltmap.add(smap);//这里不能一边遍历一边添加
						}
					}else{
						List<HRegionInfo> hl = new ArrayList<HRegionInfo>();
					    hl.add(hri);
					    Map<ServerName,List<HRegionInfo>> smap = new HashMap<ServerName,List<HRegionInfo>>();
					    smap.put(entry.getKey(), hl);
					    List<Map<ServerName,List<HRegionInfo>>> list = new ArrayList<Map<ServerName,List<HRegionInfo>>>();
					    list.add(smap);
					    tableInfo.put(tableName, list);
					}
				}else{
					List<HRegionInfo> hl = new ArrayList<HRegionInfo>();
				    hl.add(hri);
				    Map<ServerName,List<HRegionInfo>> smap = new HashMap<ServerName,List<HRegionInfo>>();
				    smap.put(entry.getKey(), hl);
				    List<Map<ServerName,List<HRegionInfo>>> list = new ArrayList<Map<ServerName,List<HRegionInfo>>>();
				    list.add(smap);
				    tableInfo.put(tableName, list);
				}
			}
		}
		return tableInfo;
	}
	
	public void showNavigableMap(NavigableMap<HRegionInfo,ServerName> nMap){
		System.out.println("--------------HRegionMap---------------");
		for(Entry<HRegionInfo, ServerName> entry:nMap.entrySet()){
			System.out.println("HRegionName: "+entry.getKey().getRegionNameAsString()+"---ServerName: "+entry.getValue().getServerName());
		}
		
	}
	
	public void showMap(Map<ServerName, List<HRegionInfo>> map){
		System.out.println("--------------ServerMap---------------");
		for(Entry<ServerName,List<HRegionInfo>> entry:map.entrySet()){
			System.out.print("ServerName: "+entry.getKey().getServerName()+" HRegionName: ");
			List<HRegionInfo> list = entry.getValue();
			for(HRegionInfo hr :list){
				System.out.print(hr.getRegionNameAsString()+" | ");
			}
			System.out.println();
		}
	}
	
	public void showTableInfoMap(Map<String,List<Map<ServerName,List<HRegionInfo>>>> tableInfo){
		System.out.println("-----------------TableInfoMap-------------------");
		for(Entry<String,List<Map<ServerName,List<HRegionInfo>>>> entry:tableInfo.entrySet()){
			System.out.println("TableName: "+entry.getKey()+" ###"+entry.getValue().size()+"###");
			for(Map<ServerName,List<HRegionInfo>> tmap:entry.getValue()){
				System.out.print("ServerName: ");
				for(Entry<ServerName,List<HRegionInfo>> entry2:tmap.entrySet()){
					System.out.println(""+entry2.getKey().getServerName()+" ");
					for(HRegionInfo hri:entry2.getValue()){
						System.out.print("HRegionName: "+hri.getRegionNameAsString());
					}
				}
				System.out.println();
			}
		}
	}
	
	public static void main(String args[]){
		RegionServerInfo info = new RegionServerInfo();
		try {
			Map<ServerName, List<HRegionInfo>> map = info.getRegionInfo();
			info.showMap(map);
			info.showTableInfoMap(info.tableInfo(map));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
