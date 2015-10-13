package com.hyman.hbase.annotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hyman.hbase.conf.TableConf;
import com.hyman.hbase.entity.User;

public class LoadTableConfigraction {
	
	Map<Class<?>,TableConf> context = new HashMap<>();
	
	public Map<Class<?>, TableConf> getContext() {
		return context;
	}

	List<Class<?>> clazzes = null;
	
	public LoadTableConfigraction(){
		//TODO
		//load table Class
		
		// test code
		clazzes = new ArrayList<Class<?>>();
		clazzes.add(User.class);
		////
	}

	private TableProcessor TableProcessor = new TableProcessor();
	private RowKeyProcessor rowKeyProcessor = new RowKeyProcessor();
	private ColumnProcessor columnProcessor = new ColumnProcessor();
	
	public void process() {
		for(Class<?> clazz : clazzes){
			TableConf conf = new TableConf();
			TableProcessor.process(conf, clazz);
			rowKeyProcessor.process(conf, clazz);
			columnProcessor.process(conf, clazz);
			context.put(clazz, conf);
		}
	}
	
	
}
