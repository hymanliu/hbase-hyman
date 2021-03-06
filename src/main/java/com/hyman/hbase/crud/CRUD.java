package com.hyman.hbase.crud;

import java.util.List;

import org.apache.hadoop.hbase.client.Get;

import com.hyman.hbase.entity.Page;

public interface CRUD<T> {

	List<T> list(List<Get> gets);
	
	Page<T> scanPage(String startRow, int limit);

	void put(T o);

	void delete(String rowId);

	T get(String id);

	Page<T> scanPage(String startRow, int fromIndex, int pageIndex, int pageSize);
}
