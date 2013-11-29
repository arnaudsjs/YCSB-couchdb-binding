package couchdbBinding.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.util.HashMap;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.junit.BeforeClass;
import org.junit.Test;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import couchdbBinding.java.CouchdbClient;

public class TestCouchdbClient {

	private static CouchdbClient client;
	private static final String DATABASE_NAME = "testdatabase";
	
	@BeforeClass
	public static void initializeDbConnector() throws MalformedURLException{
		HttpClient httpClient = new StdHttpClient.Builder().url("http://127.0.0.1:2222").build();
		CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
		CouchDbConnector connector = dbInstance.createConnector(DATABASE_NAME, true);
		client = new CouchdbClient(connector);
	}
	
	@Test
	public void test() {
		// Parameter initialization
		String mapKey = "mapKey";
		String mapValue = "mapValue";
		String keyDatabaseEntry = "akey";
		HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
		ByteIterator byteValue = new StringByteIterator(mapValue);
		data.put(mapKey, byteValue);
		// Insert operation
		int success = client.insert(DATABASE_NAME, keyDatabaseEntry, data);
		// Assert insert
		assertTrue(success == 0);
		// Read operation
		HashMap<String, ByteIterator> queryResult = new HashMap<String, ByteIterator>();
		success = client.read(DATABASE_NAME, keyDatabaseEntry, null, queryResult);
		// Assert read
		assertTrue(success == 0);
		assertTrue(queryResult.size() == 1);
		assertEquals(queryResult.get(mapKey).toString(), mapValue);
		// delete operation
		success = client.delete(DATABASE_NAME, keyDatabaseEntry);
		// Assert deletion
		assertTrue(success == 0);
		success = client.read(DATABASE_NAME, keyDatabaseEntry, null, new HashMap<String, ByteIterator>());
		assertTrue(success == -1);
	}
}
