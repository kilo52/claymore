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

import java.io.Serializable;

/**
 * Abstract class defining methods all columns to be used in DataFrames must implement.
 * 
 * 
 */
public abstract class Column implements Cloneable, Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Gets the value at the specified index
	 * 
	 * @param index The index of the value to get
	 * @return The value at the specified index as an Object
	 * @throws ArrayIndexOutOfBoundsException If the specified index is out of bounds
	 */
	public abstract Object getValueAt(int index);
	
	/**
	 * Sets the value at the specified index
	 * 
	 * @param index The index of the value to set
	 * @param value The value to set at the specified position
	 * @throws ArrayIndexOutOfBoundsException If the specified index is out of bounds
	 * @throws ClassCastException If the Object provided cannot be cast to the type 
	 * 		   this Column object can hold
	 */
	public abstract void setValueAt(int index, Object value);
	
	/**
	 * Creates and returns a copy of this Column
	 * 
	 * @return A copy of this Column
	 * @see java.lang.Object#clone()
	 */
	@Override
	public abstract Object clone();
	
	/**
	 * Inserts the specified value at the given index into the column. Shifts all 
	 * entries currently at that position and any subsequent entries down 
	 * (adds one to their indices)
	 * 
	 * @param index The index to insert the value at
	 * @param next The index of the next free position
	 * @param value The value to insert
	 */
	protected abstract void insertValueAt(int index, int next, Object value);
	
	/**
	 * Removes all entries from the first index given, to the second index.
	 * Shifts all entries currently next to the last position removed and any 
	 * subsequent entries up
	 * 
	 * @param from The index from which to start removing (inclusive)
	 * @param to The index to which to remove to (exclusive)
	 * @param next The index of the next free position
	 */
	protected abstract void remove(int from, int to, int next);
	
	/**
	 * Returns the current capacity of this column, i.e. the length of its
	 * internal array
	 * 
	 * @return The capacity of this column
	 */
	protected abstract int capacity();
	
	/**
	 * Returns the class of the entries this Column can hold and operate with
	 * 
	 * @return The class type of the entries
	 */
	protected abstract Class<?> memberClass();
	
	/**
	 * Resizes the internal array holding the column entries according to its 
	 * resizing strategy
	 */
	protected abstract void resize();
	
	/**
	 * Resizes the internal array to match the given length
	 * 
	 * @param length The length to resize the column to
	 */
	protected abstract void matchLength(int length);

}
