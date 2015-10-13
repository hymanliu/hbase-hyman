package com.hyman.hbase.annotation;

import java.lang.reflect.Field;

import com.hyman.hbase.conf.TableConf;

public class ColumnProcessor implements Processor {

	@Override
	public void process(TableConf conf,Class<?> clazz){
		Field[] fields = clazz.getDeclaredFields();
		for(Field field : fields){
			if(field.isAnnotationPresent(Column.class)){
				Column column = field.getAnnotation(Column.class);
				conf.addQualifier(column.family(), column.qualifier());
			}
		}
		
	}
}
