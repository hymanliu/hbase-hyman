package com.hyman.hbase.crud;

import org.junit.Test;

public class TableToolTest {

	public static final String FAMILY_NAME="info";
	public static final String TABLE_NAME = "user";
	
	
	@Test
	public void testCreateTable(){
		TableTool.createTable(TABLE_NAME, FAMILY_NAME);
	}
	
	@Test
	public void testDropTable(){
		TableTool.dropTable(TABLE_NAME);
	}
	
	
//	@Test
//	public void testList() throws IOException{
//		
//		List<Get> gets = new ArrayList<Get>(); 
//		    
//	    Get get1 = new Get("001".getBytes());
//	    get1.addColumn("info".getBytes(), "name".getBytes());
//	    gets.add(get1);
//		
//		List<KeyValue> list = crud.list("user",gets);
//		
//		for(KeyValue kv : list){
//			
//			String row = Bytes.toString(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()); 
//			String fc = Bytes.toString(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());
//			String qname = Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
//			String val = Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
//			
//			System.out.println(row + "\t" + fc + "\t" +qname+ "\t" + val);
//		}
//	}
//	
//	
//	@Test
//	public void testPut(){
//		Map<String,String> colums = new HashMap<String,String>();
//		colums.put("name", "cathy");
//		crud.put(TABLE_NAME, "001", "fn1", colums);
//	}
	
	
//	@Test
//	public void delete(){
//		crud.delete("user", "002");
//	}
	
	
}
