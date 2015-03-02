package couchdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/*
 * Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Administrative Contact: dnet-project-office@cs.kuleuven.be
 * Technical Contact: arnaud.schoonjans@student.kuleuven.be
 */
public class StringToStringMap extends HashMap<String, String> {

	private static final long serialVersionUID = 1L;

	public StringToStringMap() {
		super();
	}

	/*
	 * After calling this constructor, the ByteIterator values of otherMap won't
	 * be useful anymore!!!
	 */
	public StringToStringMap(Map<String, ByteIterator> otherMap) {
		super();
		for (String key : otherMap.keySet()) {
			String value = otherMap.get(key).toString();
			super.put(key, value);
		}
	}

	/* 
	 * Extracts only the string values from the json string. Assumes the json
	 * string only contains simple objects (no arrays, etc).
	 * 
	 * Parsed mapreduce result
	 */

	public StringToStringMap(String contentOfMapAsJson) throws ParseException {
		super();
		JSONParser parser = new JSONParser();
		JSONObject jsonObj = (JSONObject) parser.parse(contentOfMapAsJson);
		@SuppressWarnings("unchecked")
		Set<String> keys = jsonObj.keySet();
		for (String key : keys) {
			Object value = jsonObj.get(key);
			// Remove key from json string
			if (value instanceof String) {
				super.put(key, (String) value);
			}
		}
	}

	public void put(String key, ByteIterator value) {
		String valueAsString = value.toString();
		super.put(key, valueAsString);
	}

	public ByteIterator getAsByteIt(String key) {
		String value = super.get(key);
		return new StringByteIterator(value);
	}

	/*
	 * This method checks whether all values belonging to the values of expected
	 * match with the corresponding values in real.
	 */
	public static boolean doesValuesMatch(StringToStringMap expected,
			StringToStringMap real) {
		for (String key : expected.keySet()) {
			String expectedValue = expected.get(key);
			String realValue = real.get(key);
			if (realValue == null || !expectedValue.equals(realValue))
				return false;
		}
		return true;
	}

	public static void print(StringToStringMap map) {
		for (String key : map.keySet()) {
			String value = map.get(key);
			System.err.println("Key=" + key + "; Values=" + value);
		}
	}
}
