package com.hyman.hbase.crud;

import java.util.Map;

import org.junit.Test;

import com.hyman.hbase.annotation.LoadTableConfigraction;
import com.hyman.hbase.conf.TableConf;
import com.hyman.hbase.entity.User;

public class LoadTableConfigractionTest {

	@Test
	public void test(){
		LoadTableConfigraction loadTableConfigraction = new LoadTableConfigraction();
		loadTableConfigraction.process();
		Map<Class<?>,TableConf> context = loadTableConfigraction.getContext();
		
		
		System.out.println(context.get(User.class).getAllQualifiers());
	}
}
