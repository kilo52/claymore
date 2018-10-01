/* 
 * Copyright (C) 2018 Phil Gaiser
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * DataFrame implementation using primitive wrapper objects as the underlying 
 * data structure.<br>This implementation does permit null values.<br>
 * Columns which support the use of null values must be used with this implementation.
 * 
 * <p>As described in the {@link DataFrame} interface, most methods of this class can
 * throw a {@link DataFrameException} at runtime if any argument passed to it is invalid,
 * for example an out of bounds index, or if that operation would result in an 
 * incoherent/invalid state of that DataFrame.
 * 
 * <p>A NullableDataFrame is {@link Cloneable}, {@link Iterable}, Serializable
 * 
 * <p>This implementation is NOT thread-safe.
 * 
 * @author Phil Gaiser
 * @see DefaultDataFrame
 * @since 1.0.0
 *
 */
public class NullableDataFrame implements DataFrame {

	private static final long serialVersionUID = 1L;
	
	private Column[] columns;
	private Map<String, Integer> names;
	private int next;

	/**
	 * Constructs an empty <code>NullableDataFrame</code> without any columns set.
	 */
	public NullableDataFrame(){
		this.next = -1;
	}
	
	/**
	 * Constructs a new <code>NullableDataFrame</code> with the specified columns.<br>
	 * Columns will have no name assigned to them.<br>
	 * The order of the columns within the constructed DataFrame is defined by the order
	 * of the arguments passed to this constructor. All columns must have the same size.
	 * <p>This implementation must use {@link Column} instances which permit null values
	 * 
	 * @param columns The Column instances comprising the constructed DataFrame 
	 */
	public NullableDataFrame(final Column... columns){
		if((columns == null) || (columns.length == 0)){
			throw new DataFrameException("Arg must not be null or empty");
		}
		int colSize = columns[0].capacity();
		for(int i=1; i<columns.length; ++i){
			if(columns[i].capacity() != colSize){
				throw new DataFrameException("Columns have deviating sizes");
			}
		}
		this.columns = new Column[columns.length];
		for(int i=0; i<columns.length; ++i){
			if(!(columns[i] instanceof NullableColumn)){
				throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
			}
			this.columns[i] = columns[i];
		}
		this.next = colSize;
	}
	
	/**
	 * Constructs a new <code>NullableDataFrame</code> with the specified columns and assigning 
	 * them the specified names. The number of columns must be equal to the number of 
	 * names.<br>
	 * The order of the columns within the constructed DataFrame is defined by the order
	 * of the arguments passed to this constructor. The index of the name in the array determines
	 * to which column that name will be assigned to.<br>All columns must have the same size.
	 * <p>This implementation must use {@link Column} instances which permit null values
	 * 
	 * @param names The names of all columns
	 * @param columns The Column instances comprising the constructed DataFrame 
	 */
	public NullableDataFrame(final String[] names, final Column... columns){
		this(columns);
		if(names.length != columns.length){
			throw new DataFrameException("Args must have equal length");
		}
		this.names = new HashMap<String, Integer>(16);
		for(int i=0; i<names.length; ++i){
			this.names.put(names[i], i);
		}
	}
	
	public Byte getByte(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableByteColumn)){
			throw new DataFrameException("Is not NullableByteColumn");
		}
		return ((NullableByteColumn)columns[col]).get(row);
	}

	public Byte getByte(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableByteColumn)){
			throw new DataFrameException("Is not NullableByteColumn");
		}
		return ((NullableByteColumn)columns[col]).get(row);
	}

	public Short getShort(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableShortColumn)){
			throw new DataFrameException("Is not NullableShortColumn");
		}
		return ((NullableShortColumn)columns[col]).get(row);
	}

	public Short getShort(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableShortColumn)){
			throw new DataFrameException("Is not NullableShortColumn");
		}
		return ((NullableShortColumn)columns[col]).get(row);
	}

	public Integer getInt(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableIntColumn)){
			throw new DataFrameException("Is not NullableIntColumn");
		}
		return ((NullableIntColumn)columns[col]).get(row);
	}

	public Integer getInt(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableIntColumn)){
			throw new DataFrameException("Is not NullableIntColumn");
		}
		return ((NullableIntColumn)columns[col]).get(row);
	}
	
	public Long getLong(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableLongColumn)){
			throw new DataFrameException("Is not NullableLongColumn");
		}
		return ((NullableLongColumn)columns[col]).get(row);
	}

	public Long getLong(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableLongColumn)){
			throw new DataFrameException("Is not NullableLongColumn");
		}
		return ((NullableLongColumn)columns[col]).get(row);
	}

	public String getString(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableStringColumn)){
			throw new DataFrameException("Is not NullableStringColumn");
		}
		return ((NullableStringColumn)columns[col]).get(row);
	}

	public String getString(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableStringColumn)){
			throw new DataFrameException("Is not NullableStringColumn");
		}
		return ((NullableStringColumn)columns[col]).get(row);
	}
	
	public Float getFloat(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableFloatColumn)){
			throw new DataFrameException("Is not NullableFloatColumn");
		}
		return ((NullableFloatColumn)columns[col]).get(row);
	}

	public Float getFloat(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableFloatColumn)){
			throw new DataFrameException("Is not NullableFloatColumn");
		}
		return ((NullableFloatColumn)columns[col]).get(row);
	}

	public Double getDouble(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableDoubleColumn)){
			throw new DataFrameException("Is not NullableDoubleColumn");
		}
		return ((NullableDoubleColumn)columns[col]).get(row);
	}

	public Double getDouble(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableDoubleColumn)){
			throw new DataFrameException("Is not NullableDoubleColumn");
		}
		return ((NullableDoubleColumn)columns[col]).get(row);
	}

	public Character getChar(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableCharColumn)){
			throw new DataFrameException("Is not NullableCharColumn");
		}
		return ((NullableCharColumn)columns[col]).get(row);
	}

	public Character getChar(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableCharColumn)){
			throw new DataFrameException("Is not NullableCharColumn");
		}
		return ((NullableCharColumn)columns[col]).get(row);
	}

	public Boolean getBoolean(final int col, final int row){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableBooleanColumn)){
			throw new DataFrameException("Is not NullableBooleanColumn");
		}
		return ((NullableBooleanColumn)columns[col]).get(row);
	}

	public Boolean getBoolean(final String colName, final int row){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableBooleanColumn)){
			throw new DataFrameException("Is not NullableBooleanColumn");
		}
		return ((NullableBooleanColumn)columns[col]).get(row);
	}
	
	public void setByte(final int col, final int row, final Byte value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableByteColumn)){
			throw new DataFrameException("Is not NullableByteColumn");
		}
		((NullableByteColumn)columns[col]).set(row, value);
	}

	public void setByte(final String colName, final int row, final Byte value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableByteColumn)){
			throw new DataFrameException("Is not NullableByteColumn");
		}
		((NullableByteColumn)columns[col]).set(row, value);
	}

	public void setShort(final int col, final int row, final Short value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableShortColumn)){
			throw new DataFrameException("Is not NullableShortColumn");
		}
		((NullableShortColumn)columns[col]).set(row, value);
	}

	public void setShort(final String colName, final int row, final Short value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableShortColumn)){
			throw new DataFrameException("Is not NullableShortColumn");
		}
		((NullableShortColumn)columns[col]).set(row, value);
	}

	public void setInt(final int col, final int row, final Integer value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableIntColumn)){
			throw new DataFrameException("Is not NullableIntColumn");
		}
		((NullableIntColumn)columns[col]).set(row, value);
	}

	public void setInt(final String colName, final int row, final Integer value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableIntColumn)){
			throw new DataFrameException("Is not NullableIntColumn");
		}
		((NullableIntColumn)columns[col]).set(row, value);
	}
	
	public void setLong(final int col, final int row, final Long value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableLongColumn)){
			throw new DataFrameException("Is not NullableLongColumn");
		}
		((NullableLongColumn)columns[col]).set(row, value);
	}

	public void setLong(final String colName, final int row, final Long value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableLongColumn)){
			throw new DataFrameException("Is not NullableLongColumn");
		}
		((NullableLongColumn)columns[col]).set(row, value);
	}

	public void setString(final int col, final int row, final String value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableStringColumn)){
			throw new DataFrameException("Is not NullableStringColumn");
		}
		((NullableStringColumn)columns[col]).set(row, value);
	}

	public void setString(final String colName, final int row, final String value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableStringColumn)){
			throw new DataFrameException("Is not NullableStringColumn");
		}
		((NullableStringColumn)columns[col]).set(row, value);
	}
	
	public void setFloat(final int col, final int row, final Float value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableFloatColumn)){
			throw new DataFrameException("Is not NullableFloatColumn");
		}
		((NullableFloatColumn)columns[col]).set(row, value);
	}

	public void setFloat(final String colName, final int row, final Float value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableFloatColumn)){
			throw new DataFrameException("Is not NullableFloatColumn");
		}
		((NullableFloatColumn)columns[col]).set(row, value);
	}

	public void setDouble(final int col, final int row, final Double value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableDoubleColumn)){
			throw new DataFrameException("Is not NullableDoubleColumn");
		}
		((NullableDoubleColumn)columns[col]).set(row, value);
	}

	public void setDouble(final String colName, final int row, final Double value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableDoubleColumn)){
			throw new DataFrameException("Is not NullableDoubleColumn");
		}
		((NullableDoubleColumn)columns[col]).set(row, value);
	}

	public void setChar(final int col, final int row, final Character value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableCharColumn)){
			throw new DataFrameException("Is not NullableCharColumn");
		}
		((NullableCharColumn)columns[col]).set(row, value);
	}

	public void setChar(final String colName, final int row, final Character value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableCharColumn)){
			throw new DataFrameException("Is not NullableCharColumn");
		}
		((NullableCharColumn)columns[col]).set(row, value);
	}

	public void setBoolean(final int col, final int row, final Boolean value){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableBooleanColumn)){
			throw new DataFrameException("Is not NullableBooleanColumn");
		}
		((NullableBooleanColumn)columns[col]).set(row, value);
	}

	public void setBoolean(final String colName, final int row, final Boolean value){
		final int col = enforceName(colName);
		if((row < 0) || (row >= next)){
			throw new DataFrameException("Invalid row index: "+row);
		}
		if(!(columns[col] instanceof NullableBooleanColumn)){
			throw new DataFrameException("Is not NullableBooleanColumn");
		}
		((NullableBooleanColumn)columns[col]).set(row, value);
	}

	public String[] getColumnNames(){
		if(names != null){
			final String[] names = new String[columns.length];
			for(int i=0; i<columns.length; ++i){
				final String s = getColumnName(i);
				names[i] = ((s == null) ? String.valueOf(i) : s);
			}
			return names;
		}
		return null;
	}

	public String getColumnName(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if(names != null){
			for(Map.Entry<String, Integer> e : names.entrySet()){
				if(e.getValue() == col){
					return e.getKey();
				}
			}
		}
		return null;
	}

	public int getColumnIndex(final String colName){
		return enforceName(colName);
	}

	public void setColumnNames(String... names){
		if((names == null) || (names.length == 0)){
			throw new DataFrameException("Arg must not be null or empty");
		}
		if((next == -1) || (names.length != columns.length)){
			throw new DataFrameException("Length does not match number of columns: "
					+names.length);
		}
		this.names = new HashMap<String, Integer>(16);
		for(int i=0; i<names.length; ++i){
			if((names[i] == null) || (names[i].isEmpty())){
				throw new DataFrameException("Column name must not be null or empty");
			}
			this.names.put(names[i], i);
		}
	}

	public boolean setColumnName(final int col, final String name){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((name == null) || (name.isEmpty())){
			throw new DataFrameException("Column name must not be null or empty");
		}
		if(names == null){
			this.names = new HashMap<String, Integer>(16);
		}
		boolean overridden = false;
		final Iterator<String> iter = names.keySet().iterator();
		while(iter.hasNext()){
			final String key = iter.next();
			if(names.get(key) == col){
				iter.remove();
				overridden = true;
			}
		}
		names.put(name, col);
		return overridden;
	}

	public void removeColumnNames(){
		this.names = null;
	}

	public boolean hasColumnNames(){
		return (this.names != null);
	}

	public Object[] getRowAt(final int index){
		if((index >= next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		Object[] row = new Object[columns.length];
		for(int i=0; i<columns.length; ++i){
			row[i] = columns[i].getValueAt(index);
		}
		return row;
	}

	public void setRowAt(final int index, final Object[] row){
		if((index >= next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		enforceTypes(row);
		for(int i=0; i<columns.length; ++i){
			columns[i].setValueAt(index, row[i]);
		}
	}

	public void addRow(final Object[] row){
		enforceTypes(row);
		if(next >= columns[0].capacity()){
			resize();
		}
		for(int i=0; i<columns.length; ++i){
			columns[i].setValueAt(next, row[i]);
		}
		++next;
	}

	public void insertRowAt(final int index, final Object[] row){
		if((index > next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		if(index == next){
			addRow(row);
			return;
		}
		enforceTypes(row);
		if(next >= columns[0].capacity()){
			resize();
		}
		for(int i=0; i<columns.length; ++i){
			columns[i].insertValueAt(index, next, row[i]);
		}
		++next;
	}

	public void removeRow(final int index){
		if((index >= next) || (index < 0)){
			throw new DataFrameException("Invalid row index: "+index);
		}
		for(final Column col : columns){
			col.remove(index, index+1, next);
		}
		--next;
		if((next*3) < columns[0].capacity()){
			flushAll(4);
		}
	}

	public void removeRows(final int from, final int to){
		if(from >= to){
			throw new DataFrameException("'to' must be greater than 'from'");
		}
		if((from < 0) || (to < 0) || (from >= next) || (to > next)){
			throw new DataFrameException("Invalid row index: "
					+((from < 0) || (from >= next) ? from : to));
		}
		for(final Column col : columns){
			col.remove(from, to, next);
		}
		next-=(to-from);
		if((next*3) < columns[0].capacity()){
			flushAll(4);
		}
	}

	public void addColumn(final Column col){
		if(col == null){
			throw new DataFrameException("Arg must not be null");
		}
		if(!(col instanceof NullableColumn)){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if(next == -1){
			this.columns = new Column[1];
			this.columns[0] = col;
			this.next = col.capacity();
		}else{
			if(col.capacity() > next){
				final int diff = (col.capacity() - next);
				for(int i=0; i<diff; ++i){
					addRow(new Object[columns.length]);
				}
			}
			col.matchLength(capacity());
			final Column[] tmp = new Column[columns.length+1];
			for(int i=0; i<columns.length; ++i){
				tmp[i] = columns[i];
			}
			tmp[columns.length] = col;
			this.columns = tmp;
		}
	}

	public void addColumn(final String colName, final Column col){
		if((colName == null) || (colName.isEmpty()) || (col == null)){
			throw new DataFrameException("Arg must not be null or empty");
		}
		if(!(col instanceof NullableColumn)){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if(next == -1){
			this.columns = new Column[1];
			this.columns[0] = col;
			this.next = col.capacity();
			this.names = new HashMap<String, Integer>(16);
			this.names.put(colName, 0);
		}else{
			if(col.capacity() > next){
				final int diff = (col.capacity() - next);
				for(int i=0; i<diff; ++i){
					addRow(new Object[columns.length]);
				}
			}
			col.matchLength(capacity());
			final Column[] tmp = new Column[columns.length+1];
			for(int i=0; i<columns.length; ++i){
				tmp[i] = columns[i];
			}
			tmp[columns.length] = col;
			this.columns = tmp;
			if(this.names == null){
				this.names = new HashMap<String, Integer>(16);
			}
			this.names.put(colName, columns.length-1);
		}
	}

	public void removeColumn(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		final Column[] tmp = new Column[columns.length-1];
		int idx = 0;
		for(int i=0; i<columns.length; ++i){
			if(i != col){
				tmp[idx++] = columns[i];
			}
		}
		if(names != null){
			final String name = getColumnName(col);
			if(name != null){
				this.names.remove(name);
			}
			Iterator<Map.Entry<String, Integer>> iter = names.entrySet().iterator();
			while(iter.hasNext()){
				final Map.Entry<String, Integer> entry = iter.next();
				if(entry.getValue()>=col){
					entry.setValue(entry.getValue()-1);
				}
			}
		}
		this.columns = tmp;
	}

	public void removeColumn(final String colName){
		final int col = enforceName(colName);
		final Column[] tmp = new Column[columns.length-1];
		int idx = 0;
		for(int i=0; i<columns.length; ++i){
			if(i != col){
				tmp[idx++] = columns[i];
			}
		}
		this.names.remove(colName);
		Iterator<Map.Entry<String, Integer>> iter = names.entrySet().iterator();
		while(iter.hasNext()){
			final Map.Entry<String, Integer> entry = iter.next();
			if(entry.getValue()>=col){
				entry.setValue(entry.getValue()-1);
			}
		}
		this.columns = tmp;
	}

	public void insertColumnAt(final int index, final Column col){
		if(col == null){
			throw new DataFrameException("Arg must not be null");
		}
		if(!(col instanceof NullableColumn)){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if(next == -1){
			if(index != 0){
				throw new DataFrameException("Invalid column index: "+index);
			}
			this.columns = new Column[1];
			this.columns[0] = col;
			this.next = col.capacity();
		}else{
			if((index < 0) || (index > columns.length)){
				throw new DataFrameException("Invalid column index: "+index);
			}
			if(col.capacity() > next){
				final int diff = (col.capacity() - next);
				for(int i=0; i<diff; ++i){
					addRow(new Object[columns.length]);
				}
			}
			col.matchLength(capacity());
			final Column[] tmp = new Column[columns.length+1];
			for(int i=tmp.length-1; i>index; --i){
				tmp[i] = columns[i-1];
			}
			tmp[index] = col;
			for(int i=0; i<index; ++i){
				tmp[i] = columns[i];
			}
			this.columns = tmp;
			if(names != null){
				Iterator<Map.Entry<String, Integer>> iter = names.entrySet().iterator();
				while(iter.hasNext()){
					final Map.Entry<String, Integer> entry = iter.next();
					if(entry.getValue()>=index){
						entry.setValue(entry.getValue()+1);
					}
				}
			}
		}
	}

	public void insertColumnAt(final int index, final String colName, final Column col){
		if((col == null) || (colName == null) || (colName.isEmpty())){
			throw new DataFrameException("Arg must not be null or empty");
		}
		if(!(col instanceof NullableColumn)){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if(next == -1){
			if(index != 0){
				throw new DataFrameException("Invalid column index: "+index);
			}
			this.columns = new Column[1];
			this.columns[0] = col;
			this.next = col.capacity();
			this.names = new HashMap<String, Integer>(16);
			this.names.put(colName, 0);
		}else{
			if((index < 0) || (index > columns.length)){
				throw new DataFrameException("Invalid column index: "+index);
			}
			if(col.capacity() > next){
				final int diff = (col.capacity() - next);
				for(int i=0; i<diff; ++i){
					addRow(new Object[columns.length]);
				}
			}
			col.matchLength(capacity());
			final Column[] tmp = new Column[columns.length+1];
			for(int i=tmp.length-1; i>index; --i){
				tmp[i] = columns[i-1];
			}
			tmp[index] = col;
			for(int i=0; i<index; ++i){
				tmp[i] = columns[i];
			}
			this.columns = tmp;
			if(names == null){
				this.names = new HashMap<String, Integer>(16);
			}
			if(names != null){
				Iterator<Map.Entry<String, Integer>> iter = names.entrySet().iterator();
				while(iter.hasNext()){
					final Map.Entry<String, Integer> entry = iter.next();
					if(entry.getValue()>=index){
						entry.setValue(entry.getValue()+1);
					}
				}
			}
			this.names.put(colName, index);
		}
	}
	
	public int columns(){
		return (columns != null ? columns.length : 0);
	}

	public int capacity(){
		return (columns != null ? columns[0].capacity() : 0);
	}

	public int rows(){
		return (columns != null ? next : 0);
	}

	public boolean isEmpty(){
		return (next <= 0);
	}

	public boolean isNullable(){
		return true;
	}
	
	public void clear(){
		for(final Column col : columns){
			col.remove(0, next, next);
		}
		this.next = 0;
		flushAll(2);
	}
	
	public void flush(){
		if((next != -1) && (next != columns[0].capacity())){
			flushAll(0);
		}
	}

	public Column getColumnAt(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		return columns[col];
	}

	public Column getColumn(final String colName){
		return getColumnAt(enforceName(colName));
	}

	public void setColumnAt(final int index, final Column col){
		if(col == null){
			throw new DataFrameException("Arg must not be null");
		}
		if(!(col instanceof NullableColumn)){
			throw new DataFrameException("NullableDataFrame must use NullableColumn instance");
		}
		if((next == -1) || (index < 0) || (index >= columns.length)){
			throw new DataFrameException("Invalid column index: "+index);
		}
		if(col.capacity() != next){
			throw new DataFrameException("Invalid column length. Must be of length "+next);
		}
		col.matchLength(capacity());
		columns[index] = col;
	}

	public int indexOf(final int col, final String regex){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((regex == null) || (regex.isEmpty())){
			throw new DataFrameException("Arg must not be null");
		}
		final Column c = columns[col];//cache
		for(int i=0; i<next; ++i){
			if(String.valueOf(c.getValueAt(i)).matches(regex)){
				return i;
			}
		}
		return -1;
	}

	public int indexOf(final String colName, final String regex){
		return indexOf(enforceName(colName), regex);
	}
	
	public int indexOf(final int col, final int startFrom, final String regex){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((regex == null) || (regex.isEmpty())){
			throw new DataFrameException("Arg must not be null");
		}
		if((startFrom < 0) || (startFrom >= next)){
			throw new DataFrameException("Invalid start argument: "+startFrom);
		}
		final Column c = columns[col];//cache
		for(int i=startFrom; i<next; ++i){
			if(String.valueOf(c.getValueAt(i)).matches(regex)){
				return i;
			}
		}
		return -1;
	}
	
	public int indexOf(final String colName, final int startFrom, final String regex){
		return indexOf(enforceName(colName), startFrom, regex);
	}

	public int[] indexOfAll(final int col, final String regex){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((regex == null) || (regex.isEmpty())){
			throw new DataFrameException("Arg must not be null or empty");
		}
		final Column c = columns[col];
		int[] res = new int[16];
		int hits = 0;
		for(int i=0; i<next; ++i){
			if(String.valueOf(c.getValueAt(i)).matches(regex)){
				if(hits>=res.length){//resize
					final int[] tmp = new int[res.length*2];
					for(int j=0; j<hits; ++j){
						tmp[j] = res[j];
					}
					res = tmp;
				}
				res[hits++] = i;
			}
		}
		if(res.length != hits){//trim
			final int[] tmp = new int[hits];
			for(int j=0; j<hits; ++j){
				tmp[j] = res[j];
			}
			res = tmp;
		}
		return (hits == 0 ? null : res);
	}
	
	public int[] indexOfAll(final String colName, final String regex){
		return indexOfAll(enforceName(colName), regex);
	}
	
	public DataFrame findAll(final int col, final String regex){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		if((regex == null) || (regex.isEmpty())){
			throw new DataFrameException("Arg must not be null or empty");
		}
		final int[] indices = indexOfAll(col, regex);
		if(indices == null){
			return null;
		}
		final DataFrame df = new NullableDataFrame();
		try{
			for(final Column c : columns){
				df.addColumn(c.getClass().newInstance());
			}
		}catch(InstantiationException | IllegalAccessException ex){
			throw new DataFrameException("Unable to instantiate columns");
		}
		for(int i=0; i<indices.length; ++i){
			df.addRow(getRowAt(indices[i]));
		}
		if(names != null){
			df.setColumnNames(getColumnNames());
		}
		return df;
	}
	
	public DataFrame findAll(final String colName, final String regex){
		return findAll(enforceName(colName), regex);
	}
	
	public double average(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		final Column c = columns[col];
		if(isNaN(c) || (next == 0)){
			throw new DataFrameException("Unable to compute average. Column consists of NaNs");
		}
		double avg = 0;
		int total = 0;
		switch(c.memberClass().getSimpleName()){
		case "Float":
			final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
			for(int i=0; i<next; ++i){
				if(columnFloat.get(i) != null){
					avg+=columnFloat.get(i);
					++total;
				}
			}
			break;
		case "Double":
			final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
			for(int i=0; i<next; ++i){
				if(columnDouble.get(i) != null){
					avg+=columnDouble.get(i);
					++total;
				}
			}
			break;
		case "Byte":
			final NullableByteColumn columnByte = ((NullableByteColumn)c);
			for(int i=0; i<next; ++i){
				if(columnByte.get(i) != null){
					avg+=columnByte.get(i);
					++total;
				}
			}
			break;
		case "Short":
			final NullableShortColumn columnShort = ((NullableShortColumn)c);
			for(int i=0; i<next; ++i){
				if(columnShort.get(i) != null){
					avg+=columnShort.get(i);
					++total;
				}
			}
			break;
		case "Integer":
			final NullableIntColumn columnInt = ((NullableIntColumn)c);
			for(int i=0; i<next; ++i){
				if(columnInt.get(i) != null){
					avg+=columnInt.get(i);
					++total;
				}
			}
			break;
		case "Long":
			final NullableLongColumn columnLong = ((NullableLongColumn)c);
			for(int i=0; i<next; ++i){
				if(columnLong.get(i) != null){
					avg+=columnLong.get(i);
					++total;
				}
			}
			break;
		default:
			throw new DataFrameException("Unrecognized column type");
		}
		return (avg/total);
	}
	
	public double average(final String colName){
		return average(enforceName(colName));
	}
	
	public double minimum(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		final Column c = columns[col];
		if(isNaN(c) || (next == 0)){
			throw new DataFrameException("Unable to compute minimum. Column consists of NaNs");
		}
		Double min = 0.0;
		switch(c.memberClass().getSimpleName()){
		case "Float":
			float minFloat = Float.MAX_VALUE;
			final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
			for(int i=0; i<next; ++i){
				if((columnFloat.get(i) != null) && (columnFloat.get(i)<minFloat)){
					minFloat = columnFloat.get(i);
				}
			}
			min = (double)minFloat;
			break;
		case "Double":
			double minDouble = Double.MAX_VALUE;
			final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
			for(int i=0; i<next; ++i){
				if((columnDouble.get(i) != null) && (columnDouble.get(i)<minDouble)){
					minDouble = columnDouble.get(i);
				}
			}
			min = minDouble;
			break;
		case "Byte":
			byte minByte = Byte.MAX_VALUE;
			final NullableByteColumn columnByte = ((NullableByteColumn)c);
			for(int i=0; i<next; ++i){
				if((columnByte.get(i) != null) && (columnByte.get(i)<minByte)){
					minByte = columnByte.get(i);
				}
			}
			min = (double)minByte;
			break;
		case "Short":
			short minShort = Short.MAX_VALUE;
			final NullableShortColumn columnShort = ((NullableShortColumn)c);
			for(int i=0; i<next; ++i){
				if((columnShort.get(i) != null) && (columnShort.get(i)<minShort)){
					minShort = columnShort.get(i);
				}
			}
			min = (double)minShort;
			break;
		case "Integer":
			int minInt = Integer.MAX_VALUE;
			final NullableIntColumn columnInt = ((NullableIntColumn)c);
			for(int i=0; i<next; ++i){
				if((columnInt.get(i) != null) && (columnInt.get(i)<minInt)){
					minInt = columnInt.get(i);
				}
			}
			min = (double)minInt;
			break;
		case "Long":
			long minLong = Long.MAX_VALUE;
			final NullableLongColumn columnLong = ((NullableLongColumn)c);
			for(int i=0; i<next; ++i){
				if((columnLong.get(i) != null) && (columnLong.get(i)<minLong)){
					minLong = columnLong.get(i);
				}
			}
			min = (double)minLong;
			break;
		default:
			throw new DataFrameException("Unrecognized column type");
		}
		return min;
	}
	
	public double minimum(final String colName){
		return minimum(enforceName(colName));
	}
	
	public double maximum(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		final Column c = columns[col];
		if(isNaN(c) || (next == 0)){
			throw new DataFrameException("Unable to compute maximum. Column consists of NaNs");
		}
		Double max = 0.0;
		switch(c.memberClass().getSimpleName()){
		case "Float":
			float maxFloat = Float.MIN_VALUE;
			final NullableFloatColumn columnFloat = ((NullableFloatColumn)c);
			for(int i=0; i<next; ++i){
				if((columnFloat.get(i) != null) && (columnFloat.get(i)>maxFloat)){
					maxFloat = columnFloat.get(i);
				}
			}
			max = (double)maxFloat;
			break;
		case "Double":
			double maxDouble = Double.MIN_VALUE;
			final NullableDoubleColumn columnDouble = ((NullableDoubleColumn)c);
			for(int i=0; i<next; ++i){
				if((columnDouble.get(i) != null) && (columnDouble.get(i)>maxDouble)){
					maxDouble = columnDouble.get(i);
				}
			}
			max = maxDouble;
			break;
		case "Byte":
			byte maxByte = Byte.MIN_VALUE;
			final NullableByteColumn columnByte = ((NullableByteColumn)c);
			for(int i=0; i<next; ++i){
				if((columnByte.get(i) != null) && (columnByte.get(i)>maxByte)){
					maxByte = columnByte.get(i);
				}
			}
			max = (double)maxByte;
			break;
		case "Short":
			short maxShort = Short.MIN_VALUE;
			final NullableShortColumn columnShort = ((NullableShortColumn)c);
			for(int i=0; i<next; ++i){
				if((columnShort.get(i) != null) && (columnShort.get(i)>maxShort)){
					maxShort = columnShort.get(i);
				}
			}
			max = (double)maxShort;
			break;
		case "Integer":
			int maxInt = Integer.MIN_VALUE;
			final NullableIntColumn columnInt = ((NullableIntColumn)c);
			for(int i=0; i<next; ++i){
				if((columnInt.get(i) != null) && (columnInt.get(i)>maxInt)){
					maxInt = columnInt.get(i);
				}
			}
			max = (double)maxInt;
			break;
		case "Long":
			long maxLong = Long.MIN_VALUE;
			final NullableLongColumn columnLong = ((NullableLongColumn)c);
			for(int i=0; i<next; ++i){
				if((columnLong.get(i) != null) && (columnLong.get(i)>maxLong)){
					maxLong = columnLong.get(i);
				}
			}
			max = (double)maxLong;
			break;
		default:
			throw new DataFrameException("Unrecognized column type");
		}
		return max;
	}
	
	public double maximum(final String colName){
		return maximum(enforceName(colName));
	}
	
	public void sortBy(final int col){
		if((next == -1) || (col < 0) || (col >= columns.length)){
			throw new DataFrameException("Invalid column index: "+col);
		}
		NullableDataFrame.QuickSort.sort(columns[col], columns, next);
	}
	
	public void sortBy(final String colName){
		final int col = enforceName(colName);
		NullableDataFrame.QuickSort.sort(columns[col], columns, next);
	}
	
	public Object[][] asArray(){
		if(next == -1){
			return null;
		}
		final Object[][] a = new Object[columns.length][next];
		for(int i=0; i<columns.length; ++i){
			final Column c = (Column)getColumnAt(i).clone();
			for(int j=0; j<next; ++j){
				a[i][j] = c.getValueAt(j);
			}
		}
		return a;
	}
	
	@Override
	public String toString(){
		if(columns == null){
			return "uninitialized DataFrame instance";
		}
		final String nl = System.lineSeparator();
		int[] max = new int[columns.length];
		int maxIdx = String.valueOf(next-1).length();
		for(int i=0; i<columns.length; ++i){
			int k = 0;
			for(int j=0; j<next; ++j){
				if(String.valueOf(columns[i].getValueAt(j)).length() > k){
					k = String.valueOf(columns[i].getValueAt(j)).length();
				}
			}
			max[i] = k;
		}
		String[] n = new String[columns.length];
		if(names != null){
			final Set<Map.Entry<String, Integer>> set = names.entrySet();
			for(int i=0; i<columns.length; ++i){
				String s = null;
				for(final Map.Entry<String, Integer> e : set){
					if(e.getValue() == i){
						s = e.getKey();
						break;
					}
				}
				n[i] = (s != null ? s : String.valueOf(i));
			}
		}else{
			for(int i=0; i<columns.length; ++i){
				n[i] = (i+" ");
			}
		}
		for(int i=0; i<columns.length; ++i){
			max[i] = (max[i]>=n[i].length() ? max[i] : n[i].length());
		}
		final StringBuilder sb = new StringBuilder();
		for(int i=0; i<maxIdx; ++i){
			sb.append("_");
		}
		sb.append("|");
		for(int i=0; i<columns.length; ++i){
			sb.append(" ");
			sb.append(n[i]);
			for(int j=(max[i]-n[i].length()); j>0; --j){
				sb.append(" ");
			}
		}
		sb.append(nl);
		for(int i=0; i<next; ++i){
			sb.append(i);
			for(int ii=0; ii<(maxIdx-String.valueOf(i).length()); ++ii){
				sb.append(" ");
			}
			sb.append("| ");
			for(int j=0; j<columns.length; ++j){
				final Object val = columns[j].getValueAt(i);
				final String s = (val != null ? val.toString() : "null");	
				sb.append(s);
				for(int k=(max[j]-s.length()); k>=0; --k){
					sb.append(" ");
				}
			}
			sb.append(nl);
		}
		return sb.toString();
	}
	
	@Override
	public Object clone(){
		return DataFrame.copyOf(this);
	}
	
	@Override
	public Iterator<Column> iterator(){
		return new ColumnIterator(this);
	}
	
	/**
	 * Resizes all columns sequentially
	 */
	private void resize(){
		for(final Column col : columns){
			col.resize();
		}
	}
	
	/**
	 * Enforces that all entries in the given row adhere to the column types in this DataFrame
	 * 
	 * @param row The row to check against type missmatches
	 */
	private void enforceTypes(final Object[] row){
		if((next == -1) || (row.length != columns.length)){
			throw new DataFrameException("Length does not match number of columns: "+row.length);
		}
		for(int i=0; i<columns.length; ++i){
			if(row[i] != null){
				if(!(columns[i].memberClass().equals(row[i].getClass()))){
					throw new DataFrameException(String.format(
							"Type missmatch at column %s. Expected %s but found %s",
							i, row[i].getClass().getSimpleName(),
							columns[i].memberClass().getSimpleName()));

				}
			}
		}
	}
	
	/**
	 * Enforces that all requirements are met in order to access a column by its name.
	 * Throws an exception in the case of failure or returns the index of the column in
	 * the case of success
	 * 
	 * @param colName The name to check
	 * @return The index of the column with the specified name 
	 */
	private int enforceName(final String colName){
		if((colName == null) || (colName.isEmpty())){
			throw new DataFrameException("Arg must not be null or empty");
		}
		if(names == null){
			throw new DataFrameException("Column names not set");
		}
		final Integer col = names.get(colName);
		if(col == null){
			throw new DataFrameException("Invalid column name: "+colName);
		}
		return col;
	}
	
	/**
	 * Indicates whether a given Column contains NaN values
	 * 
	 * @param col The Column instance to check
	 * @return True if the given column contains NaNs, false otherwise
	 */
	private boolean isNaN(final Column col){
		return (col.memberClass().getSimpleName().equals("String") 
				|| col.memberClass().getSimpleName().equals("Character") 
				|| col.memberClass().getSimpleName().equals("Boolean"));
	}
	
	/**
	 * Sequentially performs a flush operation on all columns. A buffer can be set to keep
	 * some extra space between the current entries and the column capacity 
	 * 
	 * @param buffer A buffer applied to each column. Using 0 (zero) will apply no buffer
	 * 	   	  at all and will shrink each column to its minimum required length
	 */
	private void flushAll(final int buffer){
		for(final Column col : columns){
			col.matchLength(next+buffer);
		}
	}
	
	/**
	 * Internal Quicksort implementation for sorting NullableDataFrame instances.
	 * Presorts the column by putting all null values at the end and then only sorting
	 * the remaining part.
	 *
	 */
	private static class QuickSort {

		private static void sort(Column col, Column[] cols, int next){
			
			switch(col.memberClass().getSimpleName()){
			case "Byte":
				sort(((NullableByteColumn)col).asArray(), cols, 0, 
						presort(((NullableByteColumn)col).asArray(), cols, next));
				break;
			case "Short":
				sort(((NullableShortColumn)col).asArray(), cols, 0,
						presort(((NullableShortColumn)col).asArray(), cols, next));
				break;
			case "Integer":
				sort(((NullableIntColumn)col).asArray(), cols, 0, 
						presort(((NullableIntColumn)col).asArray(), cols, next));
				break;
			case "Long":
				sort(((NullableLongColumn)col).asArray(), cols, 0, 
						presort(((NullableLongColumn)col).asArray(), cols, next));
				break;
			case "String":
				sort(((NullableStringColumn)col).asArray(), cols, 0, 
						presort(((NullableStringColumn)col).asArray(), cols, next));
				break;
			case "Float":
				sort(((NullableFloatColumn)col).asArray(), cols, 0, 
						presort(((NullableFloatColumn)col).asArray(), cols, next));
				break;
			case "Double":
				sort(((NullableDoubleColumn)col).asArray(), cols, 0, 
						presort(((NullableDoubleColumn)col).asArray(), cols, next));
				break;
			case "Character":
				sort(((NullableCharColumn)col).asArray(), cols, 0, 
						presort(((NullableCharColumn)col).asArray(), cols, next));
				break;
			case "Boolean":
				sort(((NullableBooleanColumn)col).asArray(), cols, 0, 
						presort(((NullableBooleanColumn)col).asArray(), cols, next));
				break;
			default:
				//undefined
			}
		}	
		
	    private static void sort(Byte[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final byte MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Short[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final short MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
		
	    private static void sort(Integer[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final int MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Long[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final long MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(String[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final String MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l].compareTo(MID)<0){ ++l; }
	            while(list[r].compareTo(MID)>0){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Float[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final float MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Double[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final double MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Character[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final char MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(list[l] < MID){ ++l; }
	            while(list[r] > MID){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void sort(Boolean[] list, Column[] cols, int left, int right){
	    	if(right <= -1){
	    		return;
	    	}
	        final Boolean MID = list[(left+right)/2];
	        int l = left;
	        int r = right;
	        while(l < r){
	            while(new Boolean(list[l]).compareTo(MID)<0){ ++l; }
	            while(new Boolean(list[r]).compareTo(MID)>0){ --r; }
	            if(l <= r){
	                swap(cols, l++, r--);
	            }
	        }
	        if(left < r){
	            sort(list, cols, left, r );
	        }
	        if(right > l){
	            sort(list, cols, l, right);
	        }
	    }
	    
	    private static void swap(Column[] cols, int i, int j){
	    	for(final Column c : cols){
		        final Object cache = c.getValueAt(i);
		        c.setValueAt(i, c.getValueAt(j));
		        c.setValueAt(j, cache);
	    	}
	    }
	    
	    private static int presort(Object[] list, Column[] cols, int next){
	    	int ptr = next-1;
	    	for(int i=0; i<ptr; ++i){
	    		while(list[i] == null){
	    			if(i == ptr){
	    				break;
	    			}
	    			swap(cols, i, ptr--);
	    		}
	    	}
	    	return (list[ptr] != null ? ptr : ptr-1);
	    }
	}

}
