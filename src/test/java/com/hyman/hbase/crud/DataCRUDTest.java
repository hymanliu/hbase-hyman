package com.hyman.hbase.crud;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hyman.hbase.entity.Page;
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
	    gets.add(get1);
		
		List<User> list = crud.list(gets);
		
		for(User u : list){
			System.out.println(u);
		}
	}
	
	@Test
	public void testPageScan(){
		Page<User> page = crud.scanPage("", 3);
		
		for(User u :page.getResultList()){
			System.out.println(u);
		}
	}
	
	@Test
	public void testPut(){
		for(int i=1;i<100;i++){
			Map<String,String> colums = new HashMap<String,String>();
			DecimalFormat df = new DecimalFormat("0000");
			colums.put("name", "hyman-"+i);
			colums.put("phone", "1868882"+df.format(i));
			crud.put(df.format(i), colums);
		}
	}
	
	@Test
	public void testPageScan2(){
		Page<User> page = crud.scanPage("0096",20,20,5);
		for(User u :page.getResultList()){
			System.out.println(u);
		}
	}
	
	@Test
	public void delete(){
		crud.delete("0001");
	}
	
	@Test
	public void testGet(){
		User user = crud.get("0003");
		System.out.println(user);
	}
}
