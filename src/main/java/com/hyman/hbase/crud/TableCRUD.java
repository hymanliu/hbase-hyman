package com.hyman.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.hyman.hbase.util.HBaseUtil;

/**
 * @author hyman.liu
 * date: Jul 11, 2015 10:01:01 AM
 */
public class TableCRUD {

	private HBaseAdmin hbaseAdmin = HBaseUtil.getHBaseAdmin();
	
	/**
	 * create table s
	 * @param tableName
	 * @param familyName
	 */
	public void createTable(String tableName,String familyName){
		
		final TableName tn = TableName.valueOf(tableName);
		final HTableDescriptor tableDesc = new HTableDescriptor(tn);
		HColumnDescriptor family = new HColumnDescriptor(familyName);
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
	
	/**
	 * delete table
	 * @param tableName
	 */
	public void dropTable(String tableName){
		try {
			
			if(!hbaseAdmin.tableExists(tableName)){
				return;
			}
			if(!hbaseAdmin.isTableDisabled(tableName)){
				hbaseAdmin.disableTable(tableName);
			}
			hbaseAdmin.deleteTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
