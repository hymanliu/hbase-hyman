package com.hyman.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.hyman.hbase.util.HBaseUtil;

public class TableTool {
	
	private static HBaseAdmin admin = HBaseUtil.getHBaseAdmin();
	
	/**
	 * create tables
	 * @param tableName
	 * @param familyName
	 */
	public static void createTable(String tableName,String... families){
		
		final TableName tn = TableName.valueOf(tableName);
		final HTableDescriptor tableDesc = new HTableDescriptor(tn);
		
		for(String family:families){
			HColumnDescriptor familyDesc = new HColumnDescriptor(family);
			tableDesc.addFamily(familyDesc);
		}
		HTableDescriptor desc = new HTableDescriptor(tableDesc);
		try {
			if(!admin.tableExists(tableName)){
				admin.createTable(desc);
			}
		} catch (IOException e) {
		
		}
	}
	
	
	/**
	 * delete table
	 * @param tableName
	 */
	public static void dropTable(String tableName){
		try {
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
