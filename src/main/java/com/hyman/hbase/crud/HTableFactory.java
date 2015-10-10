package com.hyman.hbase.crud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hyman.hbase.util.HBaseUtil;

/**
 * @author hyman.liu
 * date: Jul 11, 2015 10:01:01 AM
 */
public class HTableFactory {
	
	private static final HBaseAdmin hbaseAdmin = HBaseUtil.getHBaseAdmin();
	
	private static Map<String,HTable> tableMap = new HashMap<String,HTable>();
	
	public static HTable createHTable(String tableName){
		HTable htable = tableMap.get(tableName);
		if(htable==null){
			try {
				htable = new HTable(HBaseUtil.getConfiguration(),tableName);
				tableMap.put(tableName, htable);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return htable;
	}

}
