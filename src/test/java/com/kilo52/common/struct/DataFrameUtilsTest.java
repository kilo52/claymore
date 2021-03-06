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

package com.kilo52.common.struct;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for static utility methods provided by the DataFrame interface.
 * 
 * @author Phil Gaiser
 *
 */
public class DataFrameUtilsTest {
	
	String[] columnNames;
	DefaultDataFrame df;
	NullableDataFrame nulldf;

	@BeforeClass
	public static void setUpBeforeClass(){ }

	@AfterClass
	public static void tearDownAfterClass(){ }

	@Before
	public void setUp(){ 
		
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
						1,2,3
						}),
				new ShortColumn(new short[]{
						1,2,3
						}),
				new IntColumn(new int[]{
						1,2,3
						}),
				new LongColumn(new long[]{
						1l,2l,3l
						}),
				new StringColumn(new String[]{
						"1","2","3"
						}),
				new CharColumn(new char[]{
						'a','b','c'
						}),
				new FloatColumn(new float[]{
						1f,2f,3f
						}),
				new DoubleColumn(new double[]{
						1.0,2.0,3.0
						}),
				new BooleanColumn(new boolean[]{
						true,false,true
						}));
		
		nulldf = new NullableDataFrame(
				columnNames, 
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
						"1",null,"3"
						}),
				new NullableCharColumn(new Character[]{
						'a',null,'c'
						}),
				new NullableFloatColumn(new Float[]{
						1f,null,3f
						}),
				new NullableDoubleColumn(new Double[]{
						1.0,null,3.0
						}),
				new NullableBooleanColumn(new Boolean[]{
						true,null,false
						}));
	}

	@After
	public void tearDown(){ }

	@Test
	public void testExactCopyForDefault(){
		final String MSG = "Value missmatch. Is not exact copy of original";
		DataFrame copy = DataFrame.copyOf(df);
		assertTrue("DataFrame should be of type DefaultDataFrame", copy instanceof DefaultDataFrame);
		assertTrue("DataFrame should have 3 rows", copy.rows() == 3);
		assertTrue("DataFrame should have 9 columns", copy.columns() == 9);
		assertArrayEquals("Column names should match", columnNames, copy.getColumnNames());
		
		for(int i=1; i<=copy.rows(); ++i){
			assertTrue(MSG, copy.getByte(0, i-1) == i);
		}
		for(int i=1; i<=copy.rows(); ++i){
			assertTrue(MSG, copy.getShort(1, i-1) == i);
		}
		for(int i=1; i<=copy.rows(); ++i){
			assertTrue(MSG, copy.getInt(2, i-1) == i);
		}
		for(int i=1; i<=copy.rows(); ++i){
			assertTrue(MSG, copy.getLong(3, i-1) == i);
		}
		for(int i=1; i<=copy.rows(); ++i){
			assertTrue(MSG, copy.getString(4, i-1).equals(String.valueOf(i)));
		}
		assertTrue(MSG, copy.getChar(5, 0) == 'a');
		assertTrue(MSG, copy.getChar(5, 1) == 'b');
		assertTrue(MSG, copy.getChar(5, 2) == 'c');
		for(int i=1; i<=copy.rows(); ++i){
			assertTrue(MSG, copy.getFloat(6, i-1).equals(Float.valueOf(i)));
		}
		for(int i=1; i<=copy.rows(); ++i){
			assertTrue(MSG, copy.getDouble(7, i-1).equals(Double.valueOf(i)));
		}
		assertTrue(MSG, copy.getBoolean(8, 0));
		assertFalse(MSG, copy.getBoolean(8, 1));
		assertTrue(MSG, copy.getBoolean(8, 2));
	}
	
	@Test
	public void testExactCopyForNullable(){
		final String MSG = "Value missmatch. Is not exact copy of original";
		DataFrame copy = DataFrame.copyOf(nulldf);
		assertTrue("DataFrame should be of type NullableDataFrame", copy instanceof NullableDataFrame);
		assertTrue("DataFrame should have 3 rows", copy.rows() == 3);
		assertTrue("DataFrame should have 9 columns", copy.columns() == 9);
		assertArrayEquals("Column names should match", columnNames, copy.getColumnNames());
		
		for(int i=1; i<=copy.rows(); ++i){
			if(i == 2){
				assertNull(MSG, copy.getByte(0, i-1));
			}else{
				assertTrue(MSG, copy.getByte(0, i-1) == i);
			}
		}
		for(int i=1; i<=copy.rows(); ++i){
			if(i == 2){
				assertNull(MSG, copy.getShort(1, i-1));
			}else{
				assertTrue(MSG, copy.getShort(1, i-1) == i);
			}
		}
		for(int i=1; i<=copy.rows(); ++i){
			if(i == 2){
				assertNull(MSG, copy.getInt(2, i-1));
			}else{
				assertTrue(MSG, copy.getInt(2, i-1) == i);
			}
		}
		for(int i=1; i<=copy.rows(); ++i){
			if(i == 2){
				assertNull(MSG, copy.getLong(3, i-1));
			}else{
				assertTrue(MSG, copy.getLong(3, i-1) == i);
			}
		}
		for(int i=1; i<=copy.rows(); ++i){
			if(i == 2){
				assertNull(MSG, copy.getString(4, i-1));
			}else{
				assertTrue(MSG, copy.getString(4, i-1).equals(String.valueOf(i)));
			}
		}
		assertTrue(MSG, copy.getChar(5, 0) == 'a');
		assertNull(MSG, copy.getChar(5, 1));
		assertTrue(MSG, copy.getChar(5, 2) == 'c');
		for(int i=1; i<=copy.rows(); ++i){
			if(i == 2){
				assertNull(MSG, copy.getFloat(6, i-1));
			}else{
				assertTrue(MSG, copy.getFloat(6, i-1).equals(Float.valueOf(i)));
			}
		}
		for(int i=1; i<=copy.rows(); ++i){
			if(i == 2){
				assertNull(MSG, copy.getDouble(7, i-1));
			}else{
				assertTrue(MSG, copy.getDouble(7, i-1).equals(Double.valueOf(i)));
			}
		}
		assertTrue(MSG, copy.getBoolean(8, 0));
		assertNull(MSG, copy.getBoolean(8, 1));
		assertFalse(MSG, copy.getBoolean(8, 2));
	}
	
	@Test
	public void testMerge(){
		DataFrame df1 = new DefaultDataFrame(
				new String[]{"c1","c2","c3"}, 
				new ByteColumn(new byte[]{
						1,2,3
						}),
				new ShortColumn(new short[]{
						1,2,3
						}),
				new IntColumn(new int[]{
						1,2,3
						}));
		
		DataFrame df2 = new DefaultDataFrame(
				new String[]{"c4","c5","c6"}, 
				new CharColumn(new char[]{
						'a','b','c'
						}),
				new FloatColumn(new float[]{
						1f,2f,3f
						}),
				new DoubleColumn(new double[]{
						1.0,2.0,3.0
						}));
		
		DataFrame res = DataFrame.merge(df1, df2);
		
		assertTrue("DataFrame should be of type DefaultDataFrame", res instanceof DefaultDataFrame);
		assertTrue("DataFrame should have 3 rows", res.rows() == 3);
		assertTrue("DataFrame should have 6 columns", res.columns() == 6);
		assertArrayEquals("Column names should match", new String[]{"c1","c2","c3","c4","c5","c6"},
				res.getColumnNames());
		
	}
	
	@Test
	public void testConvertFromDefaultToNullable(){
		DataFrame conv = DataFrame.convert(df, NullableDataFrame.class);
		assertTrue("DataFrame should be of type NullableDataFrame", conv instanceof NullableDataFrame);
		assertTrue("DataFrame should have 3 rows", conv.rows() == 3);
		assertTrue("DataFrame should have 9 columns", conv.columns() == 9);
		assertArrayEquals("Column names should match", columnNames, conv.getColumnNames());
	}
	
	@Test
	public void testConvertFromNullableToDefault(){
		DataFrame conv = DataFrame.convert(nulldf, DefaultDataFrame.class);
		assertTrue("DataFrame should be of type DefaultDataFrame", conv instanceof DefaultDataFrame);
		assertTrue("DataFrame should have 3 rows", conv.rows() == 3);
		assertTrue("DataFrame should have 9 columns", conv.columns() == 9);
		assertArrayEquals("Column names should match", columnNames, conv.getColumnNames());
		
		//check against null
		Object[][] o = conv.asArray();
		for(int i=0; i<o.length; ++i){
			for(int j=0; j<o[i].length; ++j){
				assertNotNull("Converted DataFrame should not contain any null values", o[i][j]);
			}
		}
	}

}
