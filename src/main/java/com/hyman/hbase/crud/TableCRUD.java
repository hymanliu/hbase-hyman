package com.hyman.hbase.crud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hyman.hbase.util.HBaseUtil;

/**
 * @author hyman.liu
 * date: Jul 11, 2015 10:01:01 AM
 */
public class TableCRUD {

	private HBaseAdmin hbaseAdmin = HBaseUtil.getHBaseAdmin();

	/**
	 * create tables
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
		
		}finally{
			IOUtils.closeStream(hbaseAdmin);
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
			
		}finally{
			IOUtils.closeStream(hbaseAdmin);
		}
	}
	
	public List<KeyValue> list(String tableName,List<Get> gets){
		List<KeyValue> ret = new ArrayList<KeyValue>();
		HTable table = null;
		try {
			table = new HTable(HBaseUtil.getConfiguration(),tableName);
		    Result[] results = table.get(gets);
		    for(Result result: results){
		    	for (Cell cell : result.listCells()) {
		              ret.add(new KeyValue(cell));
		          }
		    }
		} catch (IOException e) {
			
		}
		finally{
			IOUtils.closeStream(table);
		}
		return ret;
	}
	
	public void put(String tableName,String rowId,String family,Map<String,String> colums){
		HTable table = null;
		try {
			table = new HTable(HBaseUtil.getConfiguration(),tableName);
			Put put = new Put(Bytes.toBytes(rowId));
			for(String key:colums.keySet()){
				put.add(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(colums.get(key)));
			}
			table.put(put);
		} catch (IOException e) {
			
		}
		finally{
			IOUtils.closeStream(table);
		}
	}
	
	
	public void delete(String tableName,String rowId){
		HTable table = null;
		try {
			table = new HTable(HBaseUtil.getConfiguration(),tableName);
			Delete delete = new Delete(Bytes.toBytes(rowId));
			table.delete(delete);
		} catch (IOException e) {
			
		}finally{
			IOUtils.closeStream(table);
		}
	}
}
