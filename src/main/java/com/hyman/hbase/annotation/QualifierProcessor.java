package com.hyman.hbase.annotation;

import java.lang.reflect.Field;

import com.hyman.hbase.conf.Column;
import com.hyman.hbase.conf.TableConf;

public class QualifierProcessor implements Processor {

	@Override
	public void process(TableConf conf,Class<?> clazz){
		Field[] fields = clazz.getDeclaredFields();
		for(Field field : fields){
			if(field.isAnnotationPresent(Qualifier.class)){
				Qualifier q = field.getAnnotation(Qualifier.class);
				conf.putColumnField(new Column(q.family(), q.qualifier()),field);
			}
		}
	}
}
