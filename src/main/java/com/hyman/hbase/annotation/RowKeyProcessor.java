package com.hyman.hbase.annotation;

import java.lang.reflect.Field;

import com.hyman.hbase.conf.TableConf;

public class RowKeyProcessor implements Processor {

	@Override
	public void process(TableConf conf,Class<?> clazz){
		
		Field[] fields = clazz.getDeclaredFields();
		for(Field field : fields){
			if(field.isAnnotationPresent(RowKey.class)){
				RowKey rowKey = field.getAnnotation(RowKey.class);
				conf.setRowkey(rowKey.name());
			}
		}
	}
}
