package com.hyman.hbase.crud;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hyman.hbase.annotation.Qualifier;
import com.hyman.hbase.annotation.Table;
import com.hyman.hbase.conf.TableConf;
import com.hyman.hbase.entity.User;

public class TestAnnotation {

	public static void main(String[] args){
		
		
		Map<Class,TableConf> configration = new HashMap<Class,TableConf>();
		
		Class<User> clazz = User.class;
		
		Field[] fields = clazz.getDeclaredFields();
		
		Set<String> set = new HashSet<>();
		
		if(clazz.isAnnotationPresent(Table.class)){
			System.out.println(clazz.getAnnotation(Table.class).name());
		}
		
		for(Field field : fields){
			if(field.isAnnotationPresent(Qualifier.class)){
				Qualifier column = field.getAnnotation(Qualifier.class);
				//System.out.println(column.family()+"\t"+column.qualifier());
				
				set.add(column.family());
				set.add(column.qualifier());
				
			}
		}
		
	}
	
	public static void process(TableConf tableConf,Class<?> clazz){
		Field[] fields = clazz.getDeclaredFields();
		
		for(Field field : fields){
			if(field.isAnnotationPresent(Qualifier.class)){
				Qualifier column = field.getAnnotation(Qualifier.class);
				String family = column.family();
				
				String qualifier = column.qualifier();
			}
		}
	}
}
