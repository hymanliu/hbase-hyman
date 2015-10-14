package com.hyman.hbase.crud;

import java.io.Serializable;
import java.util.Map;

import com.hyman.hbase.conf.Column;

public class DataRow implements Serializable{
	
	private static final long serialVersionUID = -6268702812752053694L;
	private String id;
	private Map<Column,String> cols;
	
	public DataRow(){}
	
	public DataRow(String id, Map<Column, String> cols) {
		this.id= id;
		this.cols = cols;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Map<Column, String> getCols() {
		return cols;
	}
	public void setCols(Map<Column, String> cols) {
		this.cols = cols;
	}
}
