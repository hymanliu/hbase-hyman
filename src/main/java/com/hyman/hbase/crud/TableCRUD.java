package com.hyman.hbase.crud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

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
	
	
	public List<KeyValue> list(List<Get> gets) throws IOException{
		
		List<KeyValue> ret = new ArrayList<KeyValue>();
		
		//HTable htable = new HTable(HBaseConfiguration.create(HBaseUtil.getConfiguration()),"user");
		
		HConnection connection = HConnectionManager.createConnection(HBaseUtil.getConfiguration());
	    HTableInterface table = connection.getTable(TableName.valueOf("user"));
		
	    Result[] results = table.get(gets);
	    
	    
	    for(Result result: results){
	    	for (Cell cell : result.listCells()) {
	              ret.add(new KeyValue(cell));
	          }
	    }
	    
	    table.close();
	    connection.close();
		return ret;
	}
	
}
