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
 * Imbalance Load Balancer for experiment
 * @author song
 *
 */
public class ImbaLoadBalancer implements LoadBalancer {
	
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
	    LOG.info("***********Without Balance************");
		return null;
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

	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
			List<HRegionInfo> regions, List<ServerName> servers) {
		//这个方法是HBase Master在启动时调用的，用来批量调度所有用户Region到RegionServer上（hbase.master.startup.retainassign设置为false时生效，与retainAssignment互斥）
		 if (regions.isEmpty() || servers.isEmpty()) {
		      return null;
		    }
		    Map<ServerName, List<HRegionInfo>> assignments =
		      new TreeMap<ServerName,List<HRegionInfo>>();

		    /**
		     * Imba Balancer.
		     */
		    LOG.info("***********startup roundRobinAssignment*************");
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
