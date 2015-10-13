package com.hyman.hbase.annotation;

import com.hyman.hbase.conf.TableConf;

public interface Processor {

	void process(TableConf conf,Class<?> clazz);
	
}
