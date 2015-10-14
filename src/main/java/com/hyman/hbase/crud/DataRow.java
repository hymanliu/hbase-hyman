package com.hyman.hbase.crud;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.hyman.hbase.conf.Column;

public class DataRow implements Serializable{
	
	private static final long serialVersionUID = -6268702812752053694L;
	private String id;
	private Map<Column,String> cols = new HashMap<>();
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Map<Column, String> getCols() {
		return cols;
	}
	public void putColumn(Column column, String value) {
		this.cols.put(column, value);
	}
}
