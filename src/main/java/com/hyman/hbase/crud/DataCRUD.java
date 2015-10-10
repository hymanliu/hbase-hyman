package com.hyman.hbase.crud;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

public class DataCRUD implements CRUD{
	
	private HTable table;
	
	public DataCRUD(String tableName){
		this.table = HTableFactory.createHTable(tableName);
	}
	
	@Override
	public List<KeyValue> list(List<Get> gets){
		List<KeyValue> ret = new ArrayList<KeyValue>();
		try {
		    Result[] results = table.get(gets);
		    for(Result result: results){
		    	if(!result.isEmpty()){
		    	for (Cell cell : result.listCells()) {
		              ret.add(new KeyValue(cell));
		          }
		    	}
		    }
		} catch (IOException e) {
			
		}
		return ret;
	}
	
	@Override
	public void put(String rowId,String family,Map<String,String> colums){
		Put put = new Put(Bytes.toBytes(rowId));
		for(String key:colums.keySet()){
			put.add(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(colums.get(key)));
		}
		try {
			table.put(put);
		} catch (RetriesExhaustedWithDetailsException e) {
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void delete(String rowId){
		try {
			Delete delete = new Delete(Bytes.toBytes(rowId));
			table.delete(delete);
		} catch (IOException e) {
			
		}
	}
}
