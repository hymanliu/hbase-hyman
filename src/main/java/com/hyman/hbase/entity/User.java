package com.hyman.hbase.entity;

import com.hyman.hbase.annotation.Qualifier;
import com.hyman.hbase.annotation.RowKey;
import com.hyman.hbase.annotation.Table;

@Table(name="user",families={"info"})
public class User {
	
	@RowKey(name="id")
	private String id;
	@Qualifier(family="info",qualifier = "name")
	private String name;
	@Qualifier(family="info",qualifier = "phone")
	private String phone;
	
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
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}

	@Override
	public String toString() {
		return "id:"+id+"\tname:"+name+"\tphone:"+phone;
	}
	
}
