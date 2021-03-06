/* 
 * Copyright (C) 2019 Phil Gaiser
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
 */

package com.kilo52.common.io;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.kilo52.common.struct.BooleanColumn;
import com.kilo52.common.struct.ByteColumn;
import com.kilo52.common.struct.CharColumn;
import com.kilo52.common.struct.DataFrame;
import com.kilo52.common.struct.DefaultDataFrame;
import com.kilo52.common.struct.DoubleColumn;
import com.kilo52.common.struct.FloatColumn;
import com.kilo52.common.struct.IntColumn;
import com.kilo52.common.struct.LongColumn;
import com.kilo52.common.struct.NullableBooleanColumn;
import com.kilo52.common.struct.NullableByteColumn;
import com.kilo52.common.struct.NullableCharColumn;
import com.kilo52.common.struct.NullableDataFrame;
import com.kilo52.common.struct.NullableDoubleColumn;
import com.kilo52.common.struct.NullableFloatColumn;
import com.kilo52.common.struct.NullableIntColumn;
import com.kilo52.common.struct.NullableLongColumn;
import com.kilo52.common.struct.NullableShortColumn;
import com.kilo52.common.struct.NullableStringColumn;
import com.kilo52.common.struct.ShortColumn;
import com.kilo52.common.struct.StringColumn;

/**
 * Tests for DataFrameSerializer implementation.
 * 
 * @author Phil Gaiser
 *
 */
public class DataFrameSerializerTest {
	
	static String[] columnNames;
	static DataFrame df;
	static byte[] truth;
	
	static String[] columnNamesEscaped;
	static DataFrame dfEscaped;
	static byte[] truthEscaped;
	
	static String[] columnNamesEscapedNullable;
	static DataFrame dfEscapedNullable;
	static byte[] truthEscapedNullable;
	static String truthBase64;

	@BeforeClass
	public static void setUpBeforeClass(){
		columnNames = new String[]{
				"byteCol",   // 0
				"shortCol",  // 1
				"intCol",    // 2
				"longCol",   // 3
				"stringCol", // 4
				"charCol",   // 5
				"floatCol",  // 6
				"doubleCol", // 7
				"booleanCol" // 8
				};
		
		df = new DefaultDataFrame(
				columnNames, 
				new ByteColumn(new byte[]{
						10,20,30,40,50
						}),
				new ShortColumn(new short[]{
						11,21,31,41,51
						}),
				new IntColumn(new int[]{
						12,22,32,42,52
						}),
				new LongColumn(new long[]{
						13l,23l,33l,43l,53l
						}),
				new StringColumn(new String[]{
						"10","20","30","40","50"
						}),
				new CharColumn(new char[]{
						'a','b','c','d','e'
						}),
				new FloatColumn(new float[]{
						10.1f,20.2f,30.3f,40.4f,50.5f
						}),
				new DoubleColumn(new double[]{
						11.1,21.2,31.3,41.4,51.5
						}),
				new BooleanColumn(new boolean[]{
						true,false,true,false,true
						}));
		
		//expected
		truth = new byte[]{123,118,58,49,59,105,58,100,101,102,97,117,108,116,
				59,114,58,53,59,99,58,57,59,110,58,98,121,116,101,67,111,108,44,115,
				104,111,114,116,67,111,108,44,105,110,116,67,111,108,44,108,111,110,
				103,67,111,108,44,115,116,114,105,110,103,67,111,108,44,99,104,97,114,
				67,111,108,44,102,108,111,97,116,67,111,108,44,100,111,117,98,108,101,
				67,111,108,44,98,111,111,108,101,97,110,67,111,108,44,59,116,58,66,121,
				116,101,67,111,108,117,109,110,44,83,104,111,114,116,67,111,108,117,109,
				110,44,73,110,116,67,111,108,117,109,110,44,76,111,110,103,67,111,108,117,
				109,110,44,83,116,114,105,110,103,67,111,108,117,109,110,44,67,104,97,114,
				67,111,108,117,109,110,44,70,108,111,97,116,67,111,108,117,109,110,44,68,
				111,117,98,108,101,67,111,108,117,109,110,44,66,111,111,108,101,97,110,67,
				111,108,117,109,110,44,59,125,49,48,44,50,48,44,51,48,44,52,48,44,53,48,44,
				49,49,44,50,49,44,51,49,44,52,49,44,53,49,44,49,50,44,50,50,44,51,50,44,52,
				50,44,53,50,44,49,51,44,50,51,44,51,51,44,52,51,44,53,51,44,49,48,44,50,48,
				44,51,48,44,52,48,44,53,48,44,97,44,98,44,99,44,100,44,101,44,49,48,46,49,44,
				50,48,46,50,44,51,48,46,51,44,52,48,46,52,44,53,48,46,53,44,49,49,46,49,44,
				50,49,46,50,44,51,49,46,51,44,52,49,46,52,44,53,49,46,53,44,116,114,117,101,
				44,102,97,108,115,101,44,116,114,117,101,44,102,97,108,115,101,44,116,
				114,117,101,44};
		
		
		
		//*************************************************//
		//                                                 //
		//        Test with escaped characters             //
		//                                                 //
		//*************************************************//
		
		columnNamesEscaped = new String[]{
				"byte,Col",
				"sh,or,tCol",
				"intC,ol",
				"lon,gCol",
				"str,i,ngCol",
				"cha,r,Col",
				"floa<>t,<Col",
				"dou>,bl>eCol",
				"bo?o_le.anCol<>>"
				};
		
		dfEscaped = new DefaultDataFrame(
				columnNamesEscaped, 
				new ByteColumn(new byte[]{
						10,20,30,40,50
						}),
				new ShortColumn(new short[]{
						11,21,31,41,51
						}),
				new IntColumn(new int[]{
						12,22,32,42,52
						}),
				new LongColumn(new long[]{
						13l,23l,33l,43l,53l
						}),
				new StringColumn(new String[]{
						"1,,0<","2!\"0,.","3<>0","<40>","#5{=0>}"
						}),
				new CharColumn(new char[]{
						',','b',',','d','e'
						}),
				new FloatColumn(new float[]{
						10.1f,20.2f,30.3f,40.4f,50.5f
						}),
				new DoubleColumn(new double[]{
						11.1,21.2,31.3,41.4,51.5
						}),
				new BooleanColumn(new boolean[]{
						true,false,true,false,true
						}));
		
		truthEscaped = new byte[]{123,118,58,49,59,105,58,100,101,102,97,117,108,
				116,59,114,58,53,59,99,58,57,59,110,58,98,121,116,101,60,44,62,
				67,111,108,44,115,104,60,44,62,111,114,60,44,62,116,67,111,108,
				44,105,110,116,67,60,44,62,111,108,44,108,111,110,60,44,62,103,
				67,111,108,44,115,116,114,60,44,62,105,60,44,62,110,103,67,111,
				108,44,99,104,97,60,44,62,114,60,44,62,67,111,108,44,102,108,
				111,97,60,60,62,62,116,60,44,62,60,60,62,67,111,108,44,100,111,
				117,62,60,44,62,98,108,62,101,67,111,108,44,98,111,63,111,95,
				108,101,46,97,110,67,111,108,60,60,62,62,62,44,59,116,58,66,121,
				116,101,67,111,108,117,109,110,44,83,104,111,114,116,67,111,
				108,117,109,110,44,73,110,116,67,111,108,117,109,110,44,76,111,
				110,103,67,111,108,117,109,110,44,83,116,114,105,110,103,67,111,
				108,117,109,110,44,67,104,97,114,67,111,108,117,109,110,44,
				70,108,111,97,116,67,111,108,117,109,110,44,68,111,117,98,108,
				101,67,111,108,117,109,110,44,66,111,111,108,101,97,110,67,111,
				108,117,109,110,44,59,125,49,48,44,50,48,44,51,48,44,52,48,44,
				53,48,44,49,49,44,50,49,44,51,49,44,52,49,44,53,49,44,49,50,44,
				50,50,44,51,50,44,52,50,44,53,50,44,49,51,44,50,51,44,51,51,44,
				52,51,44,53,51,44,49,60,44,62,60,44,62,48,60,60,62,44,50,33,
				34,48,60,44,62,46,44,51,60,60,62,62,48,44,60,60,62,52,48,62,44,
				35,53,123,61,48,62,125,44,60,44,62,44,98,44,60,44,62,44,100,
				44,101,44,49,48,46,49,44,50,48,46,50,44,51,48,46,51,44,52,48,
				46,52,44,53,48,46,53,44,49,49,46,49,44,50,49,46,50,44,51,49,
				46,51,44,52,49,46,52,44,53,49,46,53,44,116,114,117,101,44,102,
				97,108,115,101,44,116,114,117,101,44,102,97,108,115,101,44,
				116,114,117,101,44};
		
		
		
		//**************************************************************//
		//                                                              //
		//        Test NullableDataFRame with escaped characters        //
		//                                                              //
		//**************************************************************//
		
		columnNamesEscapedNullable = new String[]{
				"byte,Col",
				"sh,or,tCol",
				"intC,ol",
				"lon,gCol",
				"str,i,ngCol",
				"cha,r,Col",
				"floa<>t,<Col",
				"dou>,bl>eCol",
				"bo?o_le.anCol<>>"
				};
		
		dfEscapedNullable = new NullableDataFrame(
				columnNamesEscapedNullable, 
				new NullableByteColumn(new Byte[]{
						1,null,3
						}),
				new NullableShortColumn(new Short[]{
						1,null,3
						}),
				new NullableIntColumn(new Integer[]{
						1,null,3
						}),
				new NullableLongColumn(new Long[]{
						1l,null,3l
						}),
				new NullableStringColumn(new String[]{
						"1,,0<","2!\"0,.","3<>0"
						}),
				new NullableCharColumn(new Character[]{
						',',null,','
						}),
				new NullableFloatColumn(new Float[]{
						1.0f,null,3.0f
						}),
				new NullableDoubleColumn(new Double[]{
						1.0,null,3.0
						}),
				new NullableBooleanColumn(new Boolean[]{
						true,false,null
						}));
		
		truthEscapedNullable = new byte[]{123,118,58,49,59,105,58,110,117,108,
				108,97,98,108,101,59,114,58,51,59,99,58,57,59,110,58,98,121,
				116,101,60,44,62,67,111,108,44,115,104,60,44,62,111,114,60,44,
				62,116,67,111,108,44,105,110,116,67,60,44,62,111,108,44,108,
				111,110,60,44,62,103,67,111,108,44,115,116,114,60,44,62,105,
				60,44,62,110,103,67,111,108,44,99,104,97,60,44,62,114,60,44,
				62,67,111,108,44,102,108,111,97,60,60,62,62,116,60,44,62,60,
				60,62,67,111,108,44,100,111,117,62,60,44,62,98,108,62,101,67,
				111,108,44,98,111,63,111,95,108,101,46,97,110,67,111,108,60,
				60,62,62,62,44,59,116,58,78,117,108,108,97,98,108,101,66,121,
				116,101,67,111,108,117,109,110,44,78,117,108,108,97,98,108,
				101,83,104,111,114,116,67,111,108,117,109,110,44,78,117,108,
				108,97,98,108,101,73,110,116,67,111,108,117,109,110,44,78,
				117,108,108,97,98,108,101,76,111,110,103,67,111,108,117,109,
				110,44,78,117,108,108,97,98,108,101,83,116,114,105,110,103,
				67,111,108,117,109,110,44,78,117,108,108,97,98,108,101,67,
				104,97,114,67,111,108,117,109,110,44,78,117,108,108,97,98,
				108,101,70,108,111,97,116,67,111,108,117,109,110,44,78,117,
				108,108,97,98,108,101,68,111,117,98,108,101,67,111,108,117,
				109,110,44,78,117,108,108,97,98,108,101,66,111,111,108,101,
				97,110,67,111,108,117,109,110,44,59,125,49,44,110,117,108,
				108,44,51,44,49,44,110,117,108,108,44,51,44,49,44,110,117,
				108,108,44,51,44,49,44,110,117,108,108,44,51,44,49,60,44,
				62,60,44,62,48,60,60,62,44,50,33,34,48,60,44,62,46,44,51,
				60,60,62,62,48,44,60,44,62,44,110,117,108,108,44,60,44,
				62,44,49,46,48,44,110,117,108,108,44,51,46,48,44,49,46,
				48,44,110,117,108,108,44,51,46,48,44,116,114,117,101,44,
				102,97,108,115,101,44,110,117,108,108,44};
		
		truthBase64 = "ZGZ9kMFqwzAMhp9lPYuQLKfawYOmFApllz1AcTp3CagWuHJh"
				+ "jL37pDQ9JIUdbP//Z9n80s/NVHYwMSP6DoNNprYns7bRdN8cGnAtI"
				+ "Vx7EZRkY7VD5FYBAlIU8TXWsN4PsuLoT70XnaYfzki+aZxj8XIq+qTs"
				+ "xHXogtqO3uiIofBRnJY6sGzep1wbCSM8XyI80EdPiRdsH5fkQGOc2UNO"
				+ "wxNse58WaCeZl99tKWvxHG6IMIy5ldrfCnScUMN/QgcBrpRO4fVlVYouo"
				+ "Na+SxB9L1NRFeX0RsTMcMoBzh6v4c7+AN+Cmao=";
		
	}

	@AfterClass
	public static void tearDownAfterClass(){ 
		columnNames = null;
		df = null;
		truth = null;
		columnNamesEscaped = null;
		dfEscaped = null;
		truthEscaped = null;
	}

	@Before
	public void setUp(){ }

	@After
	public void tearDown(){ }

	@Test
	public void testSerialization() throws Exception{
		byte[] bytes = new DataFrameSerializer().serialize(df);
		assertArrayEquals("Serialized Dataframe does not match expected bytes", truth, bytes);
	}
	
	@Test
	public void testSerializationEscaped() throws Exception{
		byte[] bytes = new DataFrameSerializer().serialize(dfEscaped);
		assertArrayEquals("Serialized Dataframe does not match expected bytes", truthEscaped, bytes);
	}
	
	@Test
	public void testSerializationNullableEscaped() throws Exception{
		byte[] bytes = new DataFrameSerializer().serialize(dfEscapedNullable);
		assertArrayEquals("Serialized Dataframe does not match expected bytes", truthEscapedNullable, bytes);
	}
	
	@Test
	public void testDeserialization() throws Exception{
		DataFrame res = new DataFrameSerializer().deserialize(truth);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 5", res.rows() == 5);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type DefaultDataFrame", res instanceof DefaultDataFrame);
	}
	
	@Test
	public void testDeserializationEscaped() throws Exception{
		DataFrame res = new DataFrameSerializer().deserialize(truthEscaped);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 5", res.rows() == 5);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type DefaultDataFrame", res instanceof DefaultDataFrame);
	}
	
	@Test
	public void testDeserializationNullableEscaped() throws Exception{
		DataFrame res = new DataFrameSerializer().deserialize(truthEscapedNullable);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 3", res.rows() == 3);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type NullableDataFrame", res instanceof NullableDataFrame);
	}
	
	@Test
	public void testToBase64String() throws Exception{
		String s = new DataFrameSerializer().toBase64(dfEscapedNullable);
		assertEquals("Serialized Dataframe does not match expected Base64 string", truthBase64, s);
	}
	
	@Test
	public void testFromBase64String() throws Exception{
		DataFrame res = new DataFrameSerializer().fromBase64(truthBase64);
		assertFalse("DataFrame should not be empty", res.isEmpty());
		assertTrue("DataFrame row count should be 3", res.rows() == 3);
		assertTrue("DataFrame column count should be 9", res.columns() == 9);
		assertTrue("DataFrame should have column names set", res.hasColumnNames());
		assertTrue("DataFrame should be of type NullableDataFrame", res instanceof NullableDataFrame);
	}

}
