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

public class DataCRUDTest {

	public static final String FAMILY_NAME="info";
	private static DataCRUD crud = null;
	
	@BeforeClass
	public static void init(){
		crud = new DataCRUD("test1");
	}
	
	@Test
	public void testList() throws IOException{
		
		List<Get> gets = new ArrayList<Get>(); 
		    
	    Get get1 = new Get("01".getBytes());
	    get1.addColumn("info".getBytes(), "name".getBytes());
	    gets.add(get1);
		
		List<KeyValue> list = crud.list(gets);
		
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
		for(int i=1;i<100;i++){
			Map<String,String> colums = new HashMap<String,String>();
			colums.put("name", "hyman-"+i);
			crud.put(i+"", FAMILY_NAME, colums);
		}
	}
	
	
	@Test
	public void delete(){
		crud.delete("1");
	}
	
	
}
