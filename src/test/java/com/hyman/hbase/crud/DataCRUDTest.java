package com.hyman.hbase.crud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hyman.hbase.entity.User;

public class DataCRUDTest {

	public static final String FAMILY_NAME="info";
	private static UserCRUD crud = null;
	
	@BeforeClass
	public static void init(){
		crud = new UserCRUD("user");
	}
	
	@Test
	public void testList() throws IOException{
		
		List<Get> gets = new ArrayList<Get>(); 
		    
	    Get get1 = new Get("1".getBytes());
	    get1.addColumn(FAMILY_NAME.getBytes(), "name".getBytes());
	    gets.add(get1);
		
		List<DataRow> list = crud.list(gets);
		
		for(DataRow row : list){
			System.out.println("rowkey:"+row.getId());
			Map<String,String> cols = row.getCols();
			for(String key : cols.keySet()){
				System.out.println(key+":"+cols.get(key));
			}
		}
	}
	
	@Test
	public void testPut(){
		for(int i=1;i<100;i++){
			Map<String,String> colums = new HashMap<String,String>();
			colums.put("name", "hyman-"+i);
			crud.put(i+"", colums);
		}
	}
	
	@Test
	public void delete(){
		crud.delete("1");
	}
	
	@Test
	public void testGet(){
		User user = crud.get("3");
		System.out.println(user);
	}
}
