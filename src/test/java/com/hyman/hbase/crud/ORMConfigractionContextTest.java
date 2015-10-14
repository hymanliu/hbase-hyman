package com.hyman.hbase.crud;

import java.util.Map;

import org.junit.Test;

import com.hyman.hbase.annotation.ORMConfigContext;
import com.hyman.hbase.conf.TableConf;
import com.hyman.hbase.entity.User;

public class ORMConfigractionContextTest {

	@Test
	public void test(){
		ORMConfigContext configContext = ORMConfigContext.getInstance();
		Map<Class<?>,TableConf> context = configContext.getConfiguration();
		
		
		System.out.println(context.get(User.class).getColumns());
	}
}
