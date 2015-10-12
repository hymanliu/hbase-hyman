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
	
}
