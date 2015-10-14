package com.hyman.hbase.annotation;

import com.hyman.hbase.conf.TableConf;

public class TableProcessor implements Processor {

	@Override
	public void process(TableConf conf,Class<?> clazz){
		if(clazz.isAnnotationPresent(Table.class)){
			Table table = clazz.getAnnotation(Table.class);
			conf.setName(table.name());
			String[] families = table.families();
			conf.setFamilies(families);
		}
	}
}
