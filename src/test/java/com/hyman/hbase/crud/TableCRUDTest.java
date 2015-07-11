package com.hyman.hbase.crud;

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
	
}
