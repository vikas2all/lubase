package com.xjtudlc.idc.mapred.bulk;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;  
import org.apache.hadoop.hbase.util.Bytes;  
  
public class TestLoadIncrementalHFileToHBase {  
  
    /**
     * @use ./hadoop jar ../lib/hbase-0.92.0.jar completebulkload /song(dir of HFile in hadoop) song(name of table)
     * @param args
     * @throws Exception
     */
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = HBaseConfiguration.create();  
        byte[] TABLE = Bytes.toBytes("song");  
        HTable table = new HTable(TABLE);  
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);  
        loader.doBulkLoad(new Path("/song"), table);  
    }  
  
}  
