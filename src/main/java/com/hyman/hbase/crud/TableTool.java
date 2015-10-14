package com.hyman.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.hyman.hbase.conf.TableConf;
import com.hyman.hbase.util.HBaseUtil;

public class TableTool {
	
	private static HBaseAdmin admin = HBaseUtil.getHBaseAdmin();
	
	public static void createTable(TableConf conf){
		
		final TableName tn = TableName.valueOf(conf.getName());
		final HTableDescriptor tableDesc = new HTableDescriptor(tn);
		
		for(String family:conf.getFamilies()){
			HColumnDescriptor familyDesc = new HColumnDescriptor(family);
			tableDesc.addFamily(familyDesc);
		}
		HTableDescriptor desc = new HTableDescriptor(tableDesc);
		try {
			if(!admin.tableExists(conf.getName())){
				admin.createTable(desc);
			}
		} catch (IOException e) {
		
		}
	}
	
	
	public static void dropTable(TableConf conf){
		try {
			String tableName = conf.getName();
			if(!admin.tableExists(tableName)){
				return;
			}
			if(!admin.isTableDisabled(tableName)){
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
		} catch (IOException e) {
			
		}
	}
}
