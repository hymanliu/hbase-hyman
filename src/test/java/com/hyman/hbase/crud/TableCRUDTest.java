package com.hyman.hbase.crud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableCRUDTest {

	private static TableCRUD table = null;
	
	public static final String TABLE_NAME="test1";
	
	public static final String FAMILY_NAME="fn1";

	@BeforeClass
	public static void init(){
		table = new TableCRUD();
	}
	
	@Test
	public void testDropTable(){
		table.dropTable(TABLE_NAME);
	}
	
	
	@Test
	public void testCreateTable(){
		table.createTable(TABLE_NAME, FAMILY_NAME);
	}
	
	
	@Test
	public void testList() throws IOException{
		
		List<Get> gets = new ArrayList<Get>(); 
		    
	    Get get1 = new Get("001".getBytes());
	    get1.addColumn("info".getBytes(), "name".getBytes());
	    gets.add(get1);
		
		List<KeyValue> list = table.list("user",gets);
		
		for(KeyValue kv : list){
			
			String row = Bytes.toString(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()); 
			String fc = Bytes.toString(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());
			String qname = Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
			String val = Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
			
			System.out.println(row + "\t" + fc + "\t" +qname+ "\t" + val);
		}
	}
	
	
	@Test
	public void testPut(){
		Map<String,String> colums = new HashMap<String,String>();
		colums.put("name", "cathy");
		table.put(TABLE_NAME, "001", "fn1", colums);
	}
	
	
	@Test
	public void delete(){
		table.delete("user", "002");
	}
	
	
}
