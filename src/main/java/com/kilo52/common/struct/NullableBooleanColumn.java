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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Column holding nullable boolean values.<br>
 * Any values not explicitly set are considered null. This class uses the primitive 
 * wrapper object as the underlying data structure.
 * 
 * @see BooleanColumn
 *
 */
public class NullableBooleanColumn extends NullableColumn implements Cloneable, Serializable {

	private static final long serialVersionUID = 1L;
	
	private Boolean[] entries;
	
	/**
	 * 	Constructs an empty <code>NullableBooleanColumn</code>.
	 */
	public NullableBooleanColumn(){
		this.entries = new Boolean[0];
	}

	/**
	 * Constructs a new <code>NullableBooleanColumn</code> composed of the content of 
	 * the specified boolean array 
	 * 
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public NullableBooleanColumn(final boolean[] column){
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		Boolean[] obj = new Boolean[column.length];
		for(int i=0; i<column.length; ++i){
			obj[i] = column[i];
		}
		this.entries = obj;
	}
	
	/**
	 * Constructs a new <code>NullableBooleanColumn</code> composed of the content of 
	 * the specified Boolean array. Individual entries may be null
	 * 
	 * @param column The entries of the column to be constructed. Must not be null
	 */
	public NullableBooleanColumn(final Boolean[] column){
		if(column == null){
			throw new IllegalArgumentException("Arg must not be null");
		}
		this.entries = column;
	}
	
	/**
	 * Constructs a new <code>NullableBooleanColumn</code> composed of the content of 
	 * the specified List. Individual items may be null
	 * 
	 * @param list The entries of the column to be constructed. Must not be null or empty
	 */
	public NullableBooleanColumn(final List<Boolean> list){
		if((list == null) || (list.isEmpty())){
			throw new IllegalArgumentException("Arg must not be null or empty");
		}
		Boolean[] tmp = new Boolean[list.size()];
		Iterator<Boolean> iter = list.iterator();
		int i=0;
		while(iter.hasNext()){
			tmp[i++] = iter.next();
		}
		this.entries = tmp;
	}
	
	/**
	 * Gets the entry of this column at the specified index
	 * 
	 * @param index The index of the entry to get
	 * @return The Boolean value at the specified index. May be null
	 */
	public Boolean get(final int index){
		return entries[index];
	}
	
	/**
	 * Sets the entry of this column at the specified index
	 * to the given value
	 * 
	 * @param index The index of the entry to set
	 * @param value The Boolean value to set the entry to. May be null
	 */
	public void set(final int index, final Boolean value){
		entries[index] = value;
	}
	
	/**
	 * Returns a reference to the internal array of this column
	 * 
	 * @return The internal Boolean array
	 */
	public Boolean[] asArray(){
		return this.entries;
	}
	
	public Object clone(){
		final Boolean[] clone = new Boolean[entries.length];
		for(int i=0; i<entries.length; ++i){
			clone[i] = (entries[i] != null ? new Boolean(entries[i]) : null);
		}
		return new NullableBooleanColumn(clone);
	}

	public Object getValueAt(int index){
		return entries[index];
	}

	public void setValueAt(int index, Object value){
		entries[index] = (Boolean)value;
	}
	
	protected int capacity(){
		return entries.length;
	}
	
	protected void insertValueAt(int index, int next, Object value){
		for(int i=next; i>index; --i){
			entries[i] = entries[i-1];
		}
		entries[index] = (Boolean)value;
	}

	protected Class<?> memberClass(){
		return Boolean.class;
	}

	protected void resize(){
		Boolean[] newEntries = new Boolean[(entries.length > 0 ? entries.length*2 : 2)];
		for(int i=0; i<entries.length; ++i){
			newEntries[i] = entries[i];
		}
		this.entries = newEntries;
	}
	
	protected void remove(int from, int to, int next){
		for(int i=from, j=0; j<(next-to); ++i, ++j){
			entries[i] = entries[(to-from)+i];
		}
		for(int i=next-1, j=0; j<(to-from); --i, ++j){
			entries[i] = null;
		}
	}

	protected void matchLength(int length){
		if(length != entries.length){
			final Boolean[] tmp = new Boolean[length];
			for(int i=0; i<length; ++i){
				if(i < entries.length){
					tmp[i] = entries[i];
				}else{
					break;
				}
			}
			this.entries = tmp;
		}
	}
}
