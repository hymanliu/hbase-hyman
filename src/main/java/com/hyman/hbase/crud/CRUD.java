package com.hyman.hbase.crud;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;

public interface CRUD<T> {

	List<DataRow> list(List<Get> gets);

	void put(String rowId, Map<String, String> colums);

	void delete(String rowId);

	T get(String id);
}
