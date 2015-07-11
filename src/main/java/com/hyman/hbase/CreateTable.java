package com.hyman.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateTable {
	
	private static final String TABLE_NAME = "test1";
	private static final String  FAMILY_NAME = "fc1";

	public static void main(String[] args){
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop-ehp.hyman.com");
		conf.set("hbase.rootdir", "hdfs://hadoop-ehp.hyman.com:8020/hbase");
		HBaseAdmin hbaseAdmin = null;
		try {
			hbaseAdmin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		final TableName tableName = TableName.valueOf(TABLE_NAME);
		final HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor family = new HColumnDescriptor(FAMILY_NAME);
		tableDesc.addFamily(family);
		
		HTableDescriptor desc = new HTableDescriptor(tableDesc);
		
		try {
			if(!hbaseAdmin.tableExists(tableName)){
				hbaseAdmin.createTable(desc);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
