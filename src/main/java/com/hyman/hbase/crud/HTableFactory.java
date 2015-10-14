package com.hyman.hbase.crud;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

import com.hyman.hbase.annotation.ORMConfigContext;
import com.hyman.hbase.conf.TableConf;
import com.hyman.hbase.util.HBaseUtil;

/**
 * @author hyman.liu
 * date: Jul 11, 2015 10:01:01 AM
 */
public class HTableFactory {
	private static final Logger log = Logger.getLogger(HTableFactory.class);
	private static Map<Class<?>,HTable> tableMap = new HashMap<Class<?>,HTable>();
	
	private static HTableFactory instance;
	
	private Map<Class<?>,TableConf> configuration = null;
	
	
	private HTableFactory(){
		configuration = ORMConfigContext.getInstance().getConfiguration();
		persistAll();
	}
	
	public static synchronized HTableFactory getInstance(){
		if(instance==null){
			instance = new HTableFactory();
		}
		return instance;
	}
	
	private void persistAll(){
		log.info("begin to persist all the tables");
		for(Class<?> clazz :configuration.keySet()){
			TableConf conf = configuration.get(clazz);
			TableTool.createTable(conf);
			log.info("persist table >>"+conf.getName()+"----"+ clazz.getName() +" \t ok");
		}
	}
	
	public HTable createHTable(Class<?> clazz){
		HTable htable = tableMap.get(clazz);
		TableConf tableConf = configuration.get(clazz);
		if(htable==null){
			try {
				htable = new HTable(HBaseUtil.getConfiguration(),tableConf.getName());
				tableMap.put(clazz, htable);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return htable;
	}

}
