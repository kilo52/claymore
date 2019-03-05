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

/**
 * Abstract class all columns permitting the use of null values must extend.
 *
 */
public abstract class NullableColumn extends Column implements Cloneable, Serializable {

	private static final long serialVersionUID = 1L;

	public abstract Object getValueAt(int index);

	public abstract void setValueAt(int index, Object value);

	public abstract Object clone();

	protected abstract void insertValueAt(int index, int next, Object value);

	protected abstract void remove(int from, int to, int next);

	protected abstract int capacity();

	protected abstract Class<?> memberClass();

	protected abstract void resize();

	protected abstract void matchLength(int length);

}
