package com.hyman.hbase.crud;

import java.util.List;

public class TableConf {

	private String rowkey;
	private String[] families;
	private List<String> cols;
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public String[] getFamilies() {
		return families;
	}
	public void setFamilies(String[] families) {
		this.families = families;
	}
	public List<String> getCols() {
		return cols;
	}
	public void setCols(List<String> cols) {
		this.cols = cols;
	}
}
