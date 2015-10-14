package com.hyman.hbase.annotation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hyman.hbase.conf.TableConf;
import com.hyman.hbase.entity.Student;
import com.hyman.hbase.entity.User;

public class ORMConfigContext {
	
	private static ORMConfigContext instance;
	
	private final Map<Class<?>,TableConf> configuration = new HashMap<>();
	private final List<Class<?>> clazzes = new ArrayList<Class<?>>();
	
	public final Map<Class<?>, TableConf> getConfiguration() {
		return configuration;
	}

	private ORMConfigContext(){
		//TODO
		//load table Class
		clazzes.add(User.class);
		clazzes.add(Student.class);
		////
	}
	
	
	
	public static ORMConfigContext getInstance(){
		if(instance==null){
			instance = new ORMConfigContext();
			TableProcessor TableProcessor = new TableProcessor();
			RowKeyProcessor rowKeyProcessor = new RowKeyProcessor();
			QualifierProcessor columnProcessor = new QualifierProcessor();
			
			for(Class<?> clazz : instance.clazzes){
				TableConf conf = new TableConf();
				TableProcessor.process(conf, clazz);
				rowKeyProcessor.process(conf, clazz);
				columnProcessor.process(conf, clazz);
				instance.configuration.put(clazz, conf);
			}
		}
		return instance;
	}

	
	
}
