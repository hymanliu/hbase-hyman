package com.hyman.hbase.conf;

import java.util.ArrayList;
import java.util.List;

public class Family {
	private String name;
	private List<String> qualifiers = new ArrayList<>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getQualifiers() {
		return qualifiers;
	}
	public void putQualifier(String qualifier) {
		this.qualifiers.add(qualifier);
	}
}
