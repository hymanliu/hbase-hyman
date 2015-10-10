package com.hyman.hbase.crud;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;

public interface CRUD {

	List<KeyValue> list(List<Get> gets);

	void put(String rowId, String family, Map<String, String> colums);

	void delete(String rowId);
}
