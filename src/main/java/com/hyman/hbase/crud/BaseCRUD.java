package com.hyman.hbase.crud;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hyman.hbase.entity.Page;

public abstract class BaseCRUD<T> implements CRUD<T>{
	
	private HTable table;
	static final byte[] FAMILY_NAME = "info".getBytes();
	private Class<T> clazz = null;
	
	@SuppressWarnings("unchecked")
	public BaseCRUD(String tableName){
		ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
		clazz = (Class<T>) type.getActualTypeArguments()[0];
		this.table = HTableFactory.createHTable(tableName);
	}
	
	@Override
	public List<T> list(List<Get> gets){
		List<T> ret = new ArrayList<T>();
		try {
		    Result[] results = table.get(gets);
		    for(Result result: results){
		    	ret.add(this.rowToClass(turnToRow(result)));
		    }
		} catch (IOException e) {
			
		}
		return ret;
	}
	
	@Override
	public Page<T> scanPage(String startRow,String endRow, int index, int length){
		
		Page<T> page = new Page<>();
		
		Scan scan = new Scan();
		if(StringUtils.isNotBlank(startRow)){
			scan.setStartRow(startRow.getBytes());
		}
		if(StringUtils.isNotBlank(endRow)){
			scan.setStopRow(endRow.getBytes());
		}
		ResultScanner scanner = null;
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
		}
		int i=0;
		
		//PageFilter
		List<T> list = new ArrayList<T>();
		for(Result result :scanner){
			if(i>=index && i<index+length){
				T o = this.rowToClass(turnToRow(result));
				if(o!=null){
					list.add(o);
				}
			}
			i++;
		}
		page.setResultList(list);
		return page;
	}
	
	@Override
	public T get(String id){
		Get get = this.buildGet(id);
		T ret = null;
		try {
			Result result = table.get(get);
			ret = this.rowToClass(turnToRow(result));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	@Override
	public void put(String rowId,Map<String,String> colums){
		Put put = new Put(Bytes.toBytes(rowId));
		for(String key:colums.keySet()){
			put.add(FAMILY_NAME, Bytes.toBytes(key), Bytes.toBytes(colums.get(key)));
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
	
	private Get buildGet(String id){
		Get get = new Get(Bytes.toBytes(id));
		get.addFamily(Bytes.toBytes("info"));
		return get;
	}

	private T rowToClass(DataRow row){
		if(row==null) return null;
		T t = null;
		Constructor<T> c = null;
		try {
			c = clazz.getConstructor(String.class,Map.class);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("no such Constructor(String.class,Map.class)");
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		try {
			t = c.newInstance(row.getId(), row.getCols());
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		return t;
	}
	
	
	private DataRow turnToRow(Result result){
		if(result==null || result.isEmpty()) return null;
		
		Map<String,String> cols = new HashMap<String,String>();

		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(FAMILY_NAME);
		for(byte[] qualifier : familyMap.keySet()){
			String name = Bytes.toString(qualifier);
			String value = Bytes.toString(familyMap.get(qualifier));
			cols.put(name, value);
		}
		String id = Bytes.toString(result.getRow());
		return new DataRow(id,cols) ;
	}
}
