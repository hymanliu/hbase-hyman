package com.hyman.hbase.conf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
public class TableConf {

	private String name;
	private String rowkey;
	private Map<String,Collection<String>> families = new HashMap<>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public Map<String,Collection<String>> getFamilies() {
		return families;
	}
	
	public Collection<String> getQualifiers(String family) {
		return families.get(family);
	}
	
	public void addQualifier(String family,String qualifier){
		Collection<String> c = families.get(family);
		if(families.get(family) == null)
		{
		 c =  new ArrayList<String>();
		 families.put(family, c);
		}
		c.add(qualifier);
	}
	
	public Collection<String> getAllQualifiers() {
		
		Collection<String> ret = null;
		
		for(String key:families.keySet()){
			Collection<String> c = families.get(key);
			if(ret == null){
				ret = c;
			}else{
				ret.addAll(c);
			}
		}
		return ret;
	}
}
