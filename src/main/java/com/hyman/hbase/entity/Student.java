package com.hyman.hbase.entity;

import com.hyman.hbase.annotation.Qualifier;
import com.hyman.hbase.annotation.RowKey;
import com.hyman.hbase.annotation.Table;

@Table(name="t_student",families={"info"})
public class Student {
	@RowKey(name="rowKey")
	private String rowKey;
	@Qualifier(family="info",qualifier="id")
	private String id;
	@Qualifier(family="info",qualifier="name")
	private String name;
	@Qualifier(family="info",qualifier="age")
	private String age;
	@Qualifier(family="info",qualifier="number")
	private String number;
	
	public String getRowKey() {
		return rowKey;
	}
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}
	public String getNumber() {
		return number;
	}
	public void setNumber(String number) {
		this.number = number;
	}
}
