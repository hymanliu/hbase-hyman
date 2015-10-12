package com.hyman.hbase.entity;

import java.lang.reflect.Field;
import java.util.Map;

public class User{
	private String id;
	private String name;
	private String phone;
	
	public User(){}
	
	public User(String id,Map<String,String> cols){
		this.id = id;
		for(String key :cols.keySet()){
			try {
				Field field = this.getClass().getDeclaredField(key);
				field.setAccessible(true);
				field.set(this, cols.get(key));
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			}
			catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
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
