package com.xjtudlc.idc.hbase.loadbalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

/**
 * Table Level Load Balancer
 * @author song
 *
 */
public class TableLoadBalancer implements LoadBalancer {
	
	private static final Log LOG = LogFactory.getLog(LoadBalancer.class);
	private static final Random RANDOM = new Random(System.currentTimeMillis());
	  // slop for regions
	private float slop;
	private Configuration config;
	private ClusterStatus status;
	private MasterServices services;

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.config;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
	    if (slop < 0) slop = 0;
	    else if (slop > 1) slop = 1;
	    this.config = conf;

	}
	

	private static class TotalTableInfo{
		private int totalNum;
		private int min;
		private int max;
		public TotalTableInfo(int totalNum,int min,int max){
			this.totalNum = totalNum;
			this.min = min;
			this.max = max;
		}
		
		public void setTotalNum(int totalNum) {
			this.totalNum = totalNum;
		}

		public void setMin(int min) {
			this.min = min;
		}

		public void setMax(int max) {
			this.max = max;
		}

		public int getTotalNum() {
			return totalNum;
		}
		public int getMin() {
			return min;
		}
		public int getMax() {
			return max;
		}
		
	}
	
	

	/**
	 * 按表负载均衡重写算法
	 * @author song
	 */
	@Override
	public List<RegionPlan> balanceCluster(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		//这个方法是HBase Master内部的balancer线程定期执行调用，用来定期检查并ReBalance集群
	    long startTime = System.currentTimeMillis();

	    int numServers = clusterState.size();
	    if (numServers == 0) {
	      LOG.debug("numServers=0 so skipping load balancing");
	      return null;
	    }
	    //Table level load balancer
	    //1.遍历统计表信息
	    LOG.info("********************Step 1**********************");
	    Map<String,TotalTableInfo> table = new HashMap<String,TotalTableInfo>();//记录每个的表的临界值
	    Map<ServerName,List<TableAndLoad>> tableLoad = new HashMap<ServerName,List<TableAndLoad>>();//记录server上每个表的负载
	    List<RegionPlan> regionsToReturn = new ArrayList<RegionPlan>();
	    int regionNum = 0;
	    for(Entry<ServerName,List<HRegionInfo>> server:clusterState.entrySet()){
	    	List<HRegionInfo> list = server.getValue();
	    	List<TableAndLoad> tableLoadList = new ArrayList<TableAndLoad>();
	    	for(HRegionInfo hri:list){
	    		regionNum ++;
	    		String tableName = hri.getTableNameAsString();
	    		//table map
	    		if(table!=null){
	    			if(table.containsKey(tableName)){
	    				int total = table.get(tableName).getTotalNum();
	    				table.get(tableName).setTotalNum(total+1);
	    			}else{
	    				table.put(tableName, new TotalTableInfo(1,-1,-1));
	    			}
	    		}else{
	    			table.put(tableName, new TotalTableInfo(1,-1,-1));
	    		}
	    		//tableLoad map
	    		if(tableLoadList!=null){
	    			int tmp = 0;
	    			for(int i=0;i<tableLoadList.size();i++){
	    				TableAndLoad t = tableLoadList.get(i);
	    				if(t.getTableName().equals(tableName)){
	    					t.getTableHri().add(hri);
	    					tmp = 1;
	    					break;
	    				}
	    			}
	    			if(tmp == 0){//没有这个Table
	    				List<HRegionInfo> hril = new ArrayList<HRegionInfo>();
	    				hril.add(hri);
	    				TableAndLoad tad = new TableAndLoad(tableName,hril);
	    				tableLoadList.add(tad);
	    			}
	    		}else{
	    			List<HRegionInfo> hril = new ArrayList<HRegionInfo>();
	    			hril.add(hri);
	    			TableAndLoad tad = new TableAndLoad(tableName,hril);
	    			tableLoadList.add(tad);
	    		}
	    	}
	    	tableLoad.put(server.getKey(), tableLoadList);
	    }
	    //2.计算各个临界值
//	    float average = (float)regionNum / numServers; 
//	    int floor = (int) Math.floor(average * (1 - slop));
//	    int ceiling = (int) Math.ceil(average * (1 + slop));
	    LOG.info("*******************Step 2******************");
	    int min = regionNum / numServers;//rs min
	    int max = regionNum % numServers == 0 ? min : min + 1;//rs max
	    LOG.info("MIN: "+min+" MAX: "+max);
	    LOG.info("********TableInfo**********");
	    StringBuilder tss = new StringBuilder();
	    for(Entry<String,TotalTableInfo> tableInfo:table.entrySet() ){//table min&max
	    	TotalTableInfo tti = tableInfo.getValue();
	    	tss.append("TabelName:"+tableInfo.getKey());
	    	int total = tti.getTotalNum();
	    	int tm = total/numServers;
	    	tti.setMin(tm);
	    	tti.setMax(total%numServers==0?tm:tm+1);
	    	tss.append(" Total:"+total+" Min:"+tm+" Max:"+tti.getMax());
	    }
	    //3.检查表负载和rs负载,均衡返回null
	    int tmp = 0;
	    LOG.info("*******************Step 3********************");
	    for(Entry<ServerName,List<TableAndLoad>> server:tableLoad.entrySet()){
	    	StringBuilder sb = new StringBuilder();
	    	sb.append("ServerName: "+server.getKey().getServerName()+"-->");
	    	List<TableAndLoad> list = server.getValue();
	    	for(TableAndLoad tad:list){
	    		int trsize = tad.getTableHri().size();
	    		String tname = tad.getTableName();
	    		sb.append(" TableName: "+tname+" TableRegionSize: "+trsize);
	    		if(trsize<table.get(tname).getMin()||trsize>table.get(tname).getMax()){
	    			tmp = 1;
	    			//break;
	    		}
	    	}
	    	//if(tmp ==1)break;
	    	LOG.info(sb.toString());
	    }
	    if(tmp == 0){//check load of rs.
	    	for(Entry<ServerName,List<HRegionInfo>> server:clusterState.entrySet()){
		       	List<HRegionInfo> list = server.getValue();
		       	if(list.size()<min||list.size()>max){
		       		tmp = 1;
		       		break;
		       	}
		    }
	    }
	    LOG.info("#########################################");
	    if(tmp==0){
	    	LOG.info("******************Skipping load balancing because balanced cluster; #song#****************");
	    	return null;
	    }
	    LOG.info("******************Step 4********************");
	    Map<String,List<RegionPlan>> storeRegionPlan = new HashMap<String,List<RegionPlan>>();//临时按表保存RegionPlan List
	    //4.取出rs中overloaded Region，步骤：先取超过表限制的Region，最后检查是否超过rs限制，如果还是超过，则再取满足table限制的任意Region
	    for(Entry<ServerName,List<TableAndLoad>> server:tableLoad.entrySet()){
	    	List<TableAndLoad> tableLoadList = server.getValue();
	    	int totalRegions = 0;//the number of regions in this server.
	    	for(int i=0;i<tableLoadList.size();i++){
	    		TableAndLoad tal = tableLoadList.get(i);
	    		String tableName = tal.getTableName();
	    		List<HRegionInfo> list = tal.getTableHri();
	    		int size = list.size();
	    		totalRegions += size;
	    		int tmax = table.get(tableName).getMax();
	    		if(size>tmax){//rs上某个表的Region超过限制
	    			for(int t=0;t<size-tmax;t++){
	    				//regionsToReturn.add(new RegionPlan(list.remove(0),server.getKey(),null));//add to regionsToReturn without dest.并且将其从tableLoad中删除
	    				if(storeRegionPlan!=null){
	    					if(storeRegionPlan.containsKey(tableName)){
	    						List<RegionPlan> rpl = storeRegionPlan.get(tableName);
	    						rpl.add(new RegionPlan(list.remove(0),server.getKey(),null));
	    					}else{
	    						List<RegionPlan> rpl = new ArrayList<RegionPlan>();
		    					rpl.add(new RegionPlan(list.remove(0),server.getKey(),null));
		    					storeRegionPlan.put(tableName, rpl);
	    					}
	    				}else{
	    					List<RegionPlan> rpl = new ArrayList<RegionPlan>();
	    					rpl.add(new RegionPlan(list.remove(0),server.getKey(),null));
	    					storeRegionPlan.put(tableName, rpl);
	    				}
	    				totalRegions --;
	    			}
	    		}
	    	}
	    	if(totalRegions>max){//rs仍然超过限值
	    		int diff = totalRegions-max;
	    		while(diff>0){
	    			for(int i=0;i<tableLoadList.size();i++){
	    				TableAndLoad tal = tableLoadList.get(i);
	    				String tableName = tal.getTableName();
	    				List<HRegionInfo> list = tal.getTableHri();
	    				int size = list.size();
	    				int tmin = table.get(tableName).getMin();
	    				if(size>tmin){//tmax和tmin最多差值为1，所以直接break应该没问题
	    					//regionsToReturn.add(new RegionPlan(list.remove(0),server.getKey(),null));
	    					HRegionInfo hri = list.get(0);
	    					if(hri.isMetaRegion())continue;//Don't rebalance meta regions.
	    					if(storeRegionPlan!=null){
		    					if(storeRegionPlan.containsKey(tableName)){
		    						List<RegionPlan> rpl = storeRegionPlan.get(tableName);
		    						rpl.add(new RegionPlan(list.remove(0),server.getKey(),null));
		    					}else{
		    						List<RegionPlan> rpl = new ArrayList<RegionPlan>();
			    					rpl.add(new RegionPlan(list.remove(0),server.getKey(),null));
			    					storeRegionPlan.put(tableName, rpl);
		    					}
		    				}else{
		    					List<RegionPlan> rpl = new ArrayList<RegionPlan>();
		    					rpl.add(new RegionPlan(list.remove(0),server.getKey(),null));
		    					storeRegionPlan.put(tableName, rpl);
		    				}
	    					diff --;
	    					break;
	    				}
	    			}
	    		}
	    	}
	    }
	    LOG.info("**********************Show StoreRegionPlan(store RegionPlan with TableName)***********************");
	    for(Entry<String, List<RegionPlan>> entry:storeRegionPlan.entrySet()){
	    	LOG.info("%%%%%%%%%TableName: "+entry.getKey()+"%%%%%%%%%%%%%%");
	    	List<RegionPlan> list = entry.getValue();
	    	StringBuilder sb = new StringBuilder();
	    	sb.append("RegionName:");
	    	for(RegionPlan plan:list){
	    		//LOG.info(""+plan.getRegionInfo().getRegionNameAsString());
	    		sb.append(plan.getRegionInfo().getRegionNameAsString()+"****");
	    	}
	    	LOG.info(sb.toString());
	    }
	    LOG.info("********************Step 5************************");
	    //5.尝试添加目的地址,再次遍历rs
	    for(Entry<ServerName,List<TableAndLoad>> server:tableLoad.entrySet()){
	    	List<TableAndLoad> tableLoadList = server.getValue();
	    	for(Entry<String,TotalTableInfo> tableInfo:table.entrySet()){
	    		String tableName = tableInfo.getKey();
	    		int tmin = tableInfo.getValue().getMin();
	    		if(tmin>0){
	    			int bj = 0;
		    		for(int i=0;i<tableLoadList.size();i++){
		    			TableAndLoad tal = tableLoadList.get(i);
		    			if(tal.getTableName().equals(tableName)){
		    				bj = 1;
		    				List<HRegionInfo> list = tal.getTableHri();
		    				int rnum = list.size();
		    				if(rnum<tmin){
		    					List<RegionPlan> rpl = storeRegionPlan.get(tableName);
		    					for(int t=0;t<tmin-rnum;t++){
		    						RegionPlan rp = rpl.remove(0);
		    						rp.setDestination(server.getKey());
		    						regionsToReturn.add(rp);
		    						list.add(rp.getRegionInfo());
		    					}
		    				}
		    				break;
		    			}
		    		}
		    		if(bj==0){//rs中根本没有这个表
		    			List<RegionPlan> rpl = storeRegionPlan.get(tableName);
		    			for(int t=0;t<tmin;t++){
		    				RegionPlan rp = rpl.remove(0);
    						rp.setDestination(server.getKey());
    						regionsToReturn.add(rp);
    						//list.add(rp.getRegionInfo());
    						List<HRegionInfo> list = new ArrayList<HRegionInfo>();
    						list.add(rp.getRegionInfo());
    						TableAndLoad tal = new TableAndLoad(tableName,list);
    						tableLoadList.add(tal);
		    			}
		    		}
	    		}
	    	}
	    }
	    
	    //6.检查是否已经完成了负责均衡，即检查storeRegionPlan中value是否为空（感觉应该不用再去检查rs的限制了，应该不会超过）
	    int bj = 0;
	    for(Entry<String,List<RegionPlan>> store:storeRegionPlan.entrySet()){
	    	if(store.getValue().size()!=0){
	    		bj = 1;
	    		break;
	    	}
	    }
	    if(bj==0){
	    	 long endTime = System.currentTimeMillis();
	         LOG.info("***********Step 6:Calculated a load balance in " + (endTime-startTime) + "ms*************");
	         return regionsToReturn;
	    }
	    LOG.info("*****************Step 7******************");
	    //7.第六步没有返回就是还有空地址的RegionPlan，尝试再次分配，寻找“饥饿”rs
	    for(Entry<String,List<RegionPlan>> store:storeRegionPlan.entrySet()){
	    	String tableName = store.getKey();
	    	List<RegionPlan> list = store.getValue();
	    	for(Entry<ServerName,List<TableAndLoad>> server:tableLoad.entrySet()){
	    		if(list.size()>0){
	    			List<TableAndLoad> tableList = server.getValue();
	    			List<HRegionInfo> hril = null;
		    		int totalRegions = 0;
		    		int tableRegions = 0;
		    		for(int i =0;i<tableList.size();i++){//统计每个server总Region和表Region信息
		    			TableAndLoad tal = tableList.get(i);
		    			totalRegions += tal.getTableHri().size();
		    			if(tal.getTableName().equals(tableName)){
		    				hril = tal.getTableHri();
		    				tableRegions += hril.size();
		    			}
		    		}
		    		if(tableRegions==0){//该server上没有这个表的region
		    			hril = new ArrayList<HRegionInfo>();
		    			//本来应该把hril添加到server中的，但是应该不能一边遍历一边添加，考虑到这是最后一次遍历所以不添加也没问题。
		    			LOG.info("---------------------No Regions in Server--------------------");
		    		}
		    		int a = max-totalRegions;
		    		int b = table.get(tableName).getMax()-tableRegions;
		    		if(a>0&&b>0){
		    			int tmin = a>b?b:a;
		    			for(int t=0;t<tmin;t++){
		    				RegionPlan rp = list.remove(0);
		    				rp.setDestination(server.getKey());
		    				regionsToReturn.add(rp);
		    				hril.add(rp.getRegionInfo());
		    			}
		    		}
	    		}else break;
	    	}
	    }
	    
	    //8.到这里应该就结束了吧，检查如果仍然有未分配地址的regionplan则报错（不知道用不用检查RegionPlan中是否存在源地址和目的地址一样的项？）
	    bj = 0;
	    for(Entry<String,List<RegionPlan>> store:storeRegionPlan.entrySet()){
	    	if(store.getValue().size()!=0){
	    		bj = 1;
	    		break;
	    	}
	    }
	    if(bj==0){
	    	 long endTime = System.currentTimeMillis();
	         LOG.info("Step 8:Calculated a load balance in " + (endTime-startTime) + "ms.");
	         return regionsToReturn;
	    }else{
	    	 long endTime = System.currentTimeMillis();
	         LOG.error("Step 8:Error!! There is something wrong!!!! " + (endTime-startTime) + "ms.");
	         return regionsToReturn;
	    }
	}
	

	@Override
	public Map<HRegionInfo, ServerName> immediateAssignment(
			List<HRegionInfo> regions, List<ServerName> servers) {
		// 这个方法用来立即将目标regions进行assign，主要是要快速assign，可以暂时忽略均衡问题，交由balancer线程后续定期rebalance解决
		Map<HRegionInfo,ServerName> assignments =
		      new TreeMap<HRegionInfo,ServerName>();
		    for(HRegionInfo region : regions) {
		      assignments.put(region, servers.get(RANDOM.nextInt(servers.size())));
		    }
		return assignments;
	}

	@Override
	public ServerName randomAssignment(List<ServerName> servers) {
		// 这个方法是在随机assign一个region时被调用，从当前live的regionservers中选取一个随机的server作为assignregion的目标，
		 if (servers == null || servers.isEmpty()) {
		      LOG.warn("Wanted to do random assignment but no servers to assign to");
		      return null;
		    }
		    return servers.get(RANDOM.nextInt(servers.size()));
	}

	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(
			Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
		//这个方法是HBase Master在启动时调用的，可以保持上次集群中Region的分布位置不变（hbase.master.startup.retainassign设置为true时生效，与roundRobinAssignment互斥）
	    ArrayListMultimap<String, ServerName> serversByHostname =
	        ArrayListMultimap.create();
	    for (ServerName server : servers) {
	      serversByHostname.put(server.getHostname(), server);
	    }
	    
	    // Now come up with new assignments
	    Map<ServerName, List<HRegionInfo>> assignments =
	      new TreeMap<ServerName, List<HRegionInfo>>();
	    
	    for (ServerName server : servers) {
	      assignments.put(server, new ArrayList<HRegionInfo>());
	    }
	    
	    // Collection of the hostnames that used to have regions
	    // assigned, but for which we no longer have any RS running
	    // after the cluster restart.
	    Set<String> oldHostsNoLongerPresent = Sets.newTreeSet();
	    
	    int numRandomAssignments = 0;
	    int numRetainedAssigments = 0;
	    for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
	      HRegionInfo region = entry.getKey();
	      ServerName oldServerName = entry.getValue();
	      List<ServerName> localServers = new ArrayList<ServerName>();
	      if (oldServerName != null) {
	        localServers = serversByHostname.get(oldServerName.getHostname());
	      }
	      if (localServers.isEmpty()) {
	        // No servers on the new cluster match up with this hostname,
	        // assign randomly.
	        ServerName randomServer = servers.get(RANDOM.nextInt(servers.size()));
	        assignments.get(randomServer).add(region);
	        numRandomAssignments++;
	        if (oldServerName != null) oldHostsNoLongerPresent.add(oldServerName.getHostname());
	      } else if (localServers.size() == 1) {
	        // the usual case - one new server on same host
	        assignments.get(localServers.get(0)).add(region);
	        numRetainedAssigments++;
	      } else {
	        // multiple new servers in the cluster on this same host
	        int size = localServers.size();
	        ServerName target = localServers.get(RANDOM.nextInt(size));
	        assignments.get(target).add(region);
	        numRetainedAssigments++;
	      }
	    }
	    
	    String randomAssignMsg = "";
	    if (numRandomAssignments > 0) {
	      randomAssignMsg = numRandomAssignments + " regions were assigned " +
	      		"to random hosts, since the old hosts for these regions are no " +
	      		"longer present in the cluster. These hosts were:\n  " +
	          Joiner.on("\n  ").join(oldHostsNoLongerPresent);
	    }
	    
	    LOG.info("Reassigned " + regions.size() + " regions. " +
	        numRetainedAssigments + " retained the pre-restart assignment. " +
	        randomAssignMsg);
	    return assignments;
	}

	/**
	 * Imba Load Balancer for Test.
	 * @author song
	 */
	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
			List<HRegionInfo> regions, List<ServerName> servers) {
		//这个方法是HBase Master在启动时调用的，用来批量调度所有用户Region到RegionServer上（hbase.master.startup.retainassign设置为false时生效，与retainAssignment互斥）
		 if (regions.isEmpty() || servers.isEmpty()) {
		      return null;
		    }
		  Map<ServerName, List<HRegionInfo>> assignments =
		      new TreeMap<ServerName,List<HRegionInfo>>();
		 LOG.info("*************************Starting roundRobinAssignment for ImbaLoadBalancer*************************");
		    int numServers = servers.size();
		    int numTables = 0;
		    Map<String,ServerName> tableToServer = new HashMap<String,ServerName>();
		    for(int i=0;i<regions.size();i++){
		    	String tableName = regions.get(i).getTableNameAsString();
		    	if(tableToServer!=null){
		    		if(!tableToServer.containsKey(tableName)){
		    			tableToServer.put(tableName, servers.get(numTables%numServers));
		    			numTables++;
		    		}
		    	}else{
		    		tableToServer.put(tableName, servers.get(numTables%numServers));
		    		numTables++;
		    	}
		    }
		    
		    for(int i=0;i<regions.size();i++){
		    	String tableName = regions.get(i).getTableNameAsString();
		    	ServerName server = tableToServer.get(tableName);
		    	if(assignments!=null){
		    		if(assignments.containsKey(server)){
		    			List<HRegionInfo> hri = assignments.get(server);
		    			hri.add(regions.get(i));
		    			
		    		}else{
		    			List<HRegionInfo> hri = new ArrayList<HRegionInfo>();
			    	    hri.add(regions.get(i));
			    	    assignments.put(server, hri);
		    		}
		    	}else{
		    	    List<HRegionInfo> hri = new ArrayList<HRegionInfo>();
		    	    hri.add(regions.get(i));
		    	    assignments.put(server, hri);
		    	}
		    }
		    return assignments;
	}

	@Override
	public void setClusterStatus(ClusterStatus arg0) {
		// TODO Auto-generated method stub
		this.status = arg0;

	}

	@Override
	public void setMasterServices(MasterServices arg0) {
		// TODO Auto-generated method stub
		this.services = arg0;

	}

}
