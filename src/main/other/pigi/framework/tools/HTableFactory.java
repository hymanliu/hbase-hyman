package pigi.framework.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

// FIXME ============= UWAGA !!! STATYCZNE HTABLE - FULL REUSING - MOZLIWE KONFLIKTY !!!! =============
/**
 * Factory for HTable objects
 */
public class HTableFactory {
	private static Log log = LogFactory.getLog(HTableFactory.class);
	private static HTableFactory factory;
	
	private Map<String, HTable> tables;
	private Configuration configuration;
	private HTableFactory() {
		if (tables == null) {
			tables = new HashMap<String, HTable>();
		}
		if (configuration == null){
             configuration = HBaseConfiguration.create();;
			 log.warn("NO CONFIGURATION - HTableFactory is using default local host master");
			 System.out.println(" NO CONFIGURATION - HTableFactory is using default local host master");
		}
	}

	static{
		factory = new HTableFactory();
	}

	/**
	 * sets HBaseConfiguration
	 * @param configuration
	 */
	public static void setHBaseConfiguration(Configuration configuration){
		factory.configuration = configuration;
	}

	/**
	 * returns current HBaseConfiguration
	 * @return
	 */
	public static Configuration getHBaseConfiguration(){
		return factory.configuration;
	}

	/**
	 * returns table
	 *
	 * @param name
	 * @return
	 * @throws HTableFactoryException
	 */
	public static HTable getHTable(String name) throws HTableFactoryException {
		try {
			if (!factory.tables.containsKey(name)) {
				HTable table = new HTable(factory.configuration,name);
				factory.tables.put(name, table);
			}
			return factory.tables.get(name);
		} catch (IOException e) {
			throw new HTableFactoryException(e);
		}
	}

	/**
	 * returns hbase admin for table's manipulation
	 *
	 * @return
	 * @throws HTableFactoryException
	 */
	public static HBaseAdmin getAdmin() throws HTableFactoryException {
		try {
			return new HBaseAdmin(factory.configuration);
		} catch (MasterNotRunningException e) {
			throw new HTableFactoryException(e);
		} catch (ZooKeeperConnectionException e) {
            throw new HTableFactoryException(e);
        } catch (IOException e) {
        	throw new HTableFactoryException(e);
		}
	}

}
