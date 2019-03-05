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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.kilo52.common.struct.BooleanColumn;
import com.kilo52.common.struct.ByteColumn;
import com.kilo52.common.struct.CharColumn;
import com.kilo52.common.struct.Column;
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
 * Serializes and deserializes {@link DataFrame} instances.<br>
 * This class is also used to work with <code>.df</code> files. You can persist a
 * <code>DataFrame</code> to a file by passing it to one of the <code>writeFile()</code> 
 * methods. By calling one of the <code>readFile()</code> methods you can get the original
 * <code>DataFrame</code> instance back from the file. 
 * 
 * <p>Additionally, this class is also capable to serialize a <code>DataFrame</code> to a
 * <code>Base64</code> encoded string.
 * 
 * @author Phil Gaiser
 * @see CSVFileReader
 * @see CSVFileWriter
 * @since 1.0.0
 *
 */
public class DataFrameSerializer {
	
	/** The file extension used for DataFrames **/
	public static final String DF_FILE_EXTENSION = ".df";
	
	private static final byte DF_BYTE0 = 0x64;
	private static final byte DF_BYTE1 = 0x66;
	
	private byte[] bytes;
	
	/** Used for concurrent write operations **/
	private ConcurrentDFWriter parallelWrite;
	/** Used for concurrent read operations **/
	private ConcurrentDFReader parallelRead;

	/**
	 * Constructs a new <code>DataFrameSerializer</code> to serialize or deserialize
	 * a {@link DataFrame}<br>
	 */
	public DataFrameSerializer(){ }
	
	/**
	 * Deserializes the given <code>Base64</code> encoded string to a DataFrame
	 * 
	 * @param string The Base64 encoded string representing the DataFrame to deserialize
	 * @return A DataFrame from the given string
	 * @throws IOException If any errors occur during deserialization
	 */
	public DataFrame fromBase64(final String string) throws IOException{
		return deserialize(decompress(Base64.getDecoder().decode(string)));
	}
	
	/**
	 * Serializes the given DataFrame to a <code>Base64</code> encoded string
	 * 
	 * @param df The DataFrame to serialize to a Base64 encoded string
	 * @return A string representing the given DataFrame
	 * @throws IOException If any errors occur during serialization
	 */
	public String toBase64(final DataFrame df) throws IOException{
		return Base64.getEncoder().encodeToString(compress(serialize(df)));
	}
	
	/**
	 * Reads the specified file and returns a DataFrame constituted by the 
	 * content of that file
	 * 
	 * @param file The file to read. Must be a <code>.df</code> file
	 * @return A DataFrame from the specified file
	 * @throws IOException If any errors occur during deserialization
	 */
	public DataFrame readFile(final File file) throws IOException{
		final BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
		byte[] bytes = new byte[2048];
		final ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);
		while((is.read(bytes, 0, bytes.length)) != -1){
			baos.write(bytes, 0, bytes.length);
		}
		is.close();
		bytes = baos.toByteArray();
		if(bytes[0] != DF_BYTE0 || bytes[1] != DF_BYTE1){
			throw new IOException(String.format("Is not a %s file. Starts with 0x%02X 0x%02X",
					DF_FILE_EXTENSION, bytes[0], bytes[1]));
		}
		return deserialize(decompress(bytes));
	}
	
	/**
	 * Reads the specified file and returns a DataFrame constituted by the 
	 * content of that file
	 * 
	 * @param file The file to read. Must be a <code>.df</code> file
	 * @return A DataFrame from the specified file
	 * @throws IOException If any errors occur during deserialization
	 */
	public DataFrame readFile(final String file) throws IOException{
		return readFile(new File(file));
	}
	
	/**
	 * Creates a background thread which will read the df-file and return a 
	 * DataFrame to the specified callback. This method can only be called once. 
	 * Subsequent calls will result in an <code>IOException</code>.<br>
	 * <p>This method is meant to be used for large df-files.<br>
	 * <p>When called, this method will return immediately. The result of the 
	 * background operation will be passed to the {@link ConcurrentReader} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background 
	 * thread will be silently dropped. Check the DataFrame passed to the callback
	 * against null in order to spot any errors
	 * 
	 * @param file The file to read. Must be a <code>.df</code> file
	 * @param delegate The callback for the result of this operation
	 * @throws IOException If this method has already been called
	 */
	public void parallelReadFile(File file, ConcurrentReader delegate) throws IOException{
		if(parallelRead != null){
			throw new IOException("parallelReadFile() already called");
		}
		this.parallelRead = new ConcurrentDFReader(file, delegate);
		new Thread(this.parallelRead).start();
	}
	
	/**
	 * Creates a background thread which will read the df-file and return a 
	 * DataFrame to the specified callback. This method can only be called once. 
	 * Subsequent calls will result in an <code>IOException</code>.<br>
	 * <p>This method is meant to be used for large df-files.<br>
	 * <p>When called, this method will return immediately. The result of the 
	 * background operation will be passed to the {@link ConcurrentReader} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background 
	 * thread will be silently dropped. Check the DataFrame passed to the callback
	 * against null in order to spot any errors
	 * 
	 * @param file The file to read. Must be a <code>.df</code> file
	 * @param delegate The callback for the result of this operation
	 * @throws IOException If this method has already been called
	 */
	public void parallelReadFile(String file, ConcurrentReader delegate) throws IOException{
		parallelReadFile(new File(file), delegate);
	}
	
	/**
	 * Persists the given DataFrame to the specified file
	 * 
	 * @param file The file to write the DataFrame to
	 * @param df The DataFrame to persist
	 * @throws IOException If any errors occur during serialization
	 */
	public void writeFile(File file, DataFrame df) throws IOException{
		if(!file.getName().endsWith(DF_FILE_EXTENSION)){
			file = new File(file.getAbsolutePath()+DF_FILE_EXTENSION);
		}
		final BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));
		os.write(compress(serialize(df)));
		os.close();
	}
	
	/**
	 * Persists the given DataFrame to the specified file
	 * 
	 * @param file The file to write the DataFrame to
	 * @param df The DataFrame to persist
	 * @throws IOException If any errors occur during serialization
	 */
	public void writeFile(String file, DataFrame df) throws IOException{
		writeFile(new File(file), df);
	}
	
	/**
	 * Creates a background thread which will persist the given DataFrame to the specified
	 * file and execute the provided callback when finished.<br> This method can only be called
	 * once. Subsequent calls will result in an <code>IOException</code>.<br>
	 * <p>This method is meant to be used for large DataFrames/df-files.<br>
	 * <p>When called, this method will return immediately. The result of the background operation 
	 * will be passed to the {@link ConcurrentWriter} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background thread will be 
	 * silently dropped. Check the File object passed to the callback against null in 
	 * order to spot any errors
	 * 
	 * @param file The file to write the DataFrame to
	 * @param df The DataFrame to persist
	 * @param delegate The callback for the result of this operation. May be null
	 * @throws IOException If this method has already been called
	 */
	public void parallelWriteFile(File file, DataFrame df, ConcurrentWriter delegate) throws IOException{
		if(parallelWrite != null){
			throw new IOException("parallelWriteFile() already called");
		}
		this.parallelWrite = new ConcurrentDFWriter(file, df, delegate);
		new Thread(this.parallelWrite).start();
	}
	
	/**
	 * Creates a background thread which will persist the given DataFrame to the specified
	 * file and execute the provided callback when finished.<br> This method can only be called
	 * once. Subsequent calls will result in an <code>IOException</code>.<br>
	 * <p>This method is meant to be used for large DataFrames/df-files.<br>
	 * <p>When called, this method will return immediately. The result of the background operation 
	 * will be passed to the {@link ConcurrentWriter} callback.
	 * <p>Please note that any IOExceptions encountered by the launched background thread will be 
	 * silently dropped. Check the File object passed to the callback against null in 
	 * order to spot any errors
	 * 
	 * @param file The file to write the DataFrame to
	 * @param df The DataFrame to persist
	 * @param delegate The callback for the result of this operation. May be null
	 * @throws IOException If this method has already been called
	 */
	public void parallelWriteFile(String file, DataFrame df, ConcurrentWriter delegate) throws IOException{
		parallelWriteFile(new File(file), df, delegate);
	}
	
	/**
	 * Serializes the given <code>DataFrame</code> to an array of bytes.<br>
	 * The returned array is not compressed
	 * 
	 * @param df The DataFrame to serialize
	 * @return A byte array representing the given DataFrame
	 * @throws IOException If any errors occur during serialization
	 */
	public byte[] serialize(final DataFrame df) throws IOException{
		int ptr = -1;
		bytes = new byte[2048];
		
		//HEADER
		for(final byte b : "{v:1;i:".getBytes()){
			bytes[++ptr] = b;
		}
		if(df.isNullable()){
			for(final byte b : "nullable;".getBytes()){
				bytes[++ptr] = b;
			}
		}else{
			for(final byte b : "default;".getBytes()){
				bytes[++ptr] = b;
			}
		}
		for(final byte b : ("r:"+df.rows()+";").getBytes()){
			bytes[++ptr] = b;
		}
		for(final byte b : ("c:"+df.columns()+";").getBytes()){
			bytes[++ptr] = b;
		}
		for(final byte b : ("n:").getBytes()){
			bytes[++ptr] = b;
		}
		if(df.hasColumnNames()){
			for(final String name : escapeColumnNames(df.getColumnNames())){
				ensureCapacity(ptr+name.length()+1);
				for(final byte b : (name+",").getBytes()){
					bytes[++ptr] = b;
				}
			}
		}
		ensureCapacity(ptr+3);
		bytes[++ptr] = ';';
		bytes[++ptr] = 't';
		bytes[++ptr] = ':';
		for(final Column col : df){
			final String name = col.getClass().getSimpleName();
			ensureCapacity(ptr+name.length()+1);
			for(final byte b : name.getBytes()){
				bytes[++ptr] = b;
			}
			bytes[++ptr] = ',';
		}
		ensureCapacity(ptr+2);
		bytes[++ptr] = ';';
		bytes[++ptr] = '}';
		//END HEADER
		
		//PAYLOAD
		for(final Column col : df){
			if((col instanceof StringColumn) 
					|| (col instanceof CharColumn) 
					|| (col instanceof NullableStringColumn) 
					|| (col instanceof NullableCharColumn)){
				
				for(int i=0; i<df.rows(); ++i){
					final Object o = col.getValueAt(i);
					final byte[] b = (o == null ? "null,".getBytes() : escapeString(o));
					ensureCapacity(ptr+b.length);
					for(int j=0; j<b.length; ++j){
						bytes[++ptr] = b[j];
					}
				}
			}else{
				for(int i=0; i<df.rows(); ++i){
					final Object o = col.getValueAt(i);
					final byte[] b = (o == null ? "null,".getBytes() : (o.toString()+",").getBytes());
					ensureCapacity(ptr+b.length);
					for(int j=0; j<b.length; ++j){
						bytes[++ptr] = b[j];
					}
				}
			}

		}
		//END PAYLOAD
		final byte[] b = new byte[ptr+1];//trim
		for(int i=0; i<b.length; ++i){
			b[i] = bytes[i];
		}
		bytes = b;
		return bytes;
	}
	
	/**
	 * Deserializes the given array of bytes to a <code>DataFrame</code>.<br>
	 * The given byte array must not be compressed
	 * 
	 * @param bytes The byte array representing the DataFrame to deserialize
	 * @return A DataFrame from the given array of bytes
	 * @throws IOException If any errors occur during deserialization or if the
	 * 					   given byte array does not constitute a DataFrame
	 */
	public DataFrame deserialize(final byte[] bytes) throws IOException{
		if(bytes[3] != '1'){
			throw new IOException("Unsupported encoding");
		}
		DataFrame df = null;
		int rows = 0;
		int cols = 0;
		String dfType = null;
		String[] columnNames = null;
		String[] columnTypes = new String[0];//avoid null warning
		Column[] columns = null;
		
		//HEADER
		@SuppressWarnings("unused")
		byte b = 0;//only used in while loops. Triggers 'unused' warning
		int i1 = 7;//'begin' pointer
		int i2 = 6;//'end' pointer
		while((b = bytes[++i2]) != ';');
		byte[] tmp = copyBytes(bytes, i1, i2);
		dfType = new String(tmp);
		if(!dfType.equals("default") && !dfType.equals("nullable")){
			throw new IOException("Unsupported DataFrame implementation");	
		}
		i1 = i2+3;
		i2 += 2;
		while((b = bytes[++i2]) != ';');
		tmp = copyBytes(bytes, i1, i2);
		rows = Integer.valueOf(new String(tmp));
		i1 = i2+3;
		i2 += 2;
		while((b = bytes[++i2]) != ';');
		tmp = copyBytes(bytes, i1, i2);
		cols = Integer.valueOf(new String(tmp));
		if(bytes[i2+3] != ';'){//has column names
			columnNames = new String[cols];
			i1 = i2+3;
			i2 += 2;
			for(int j=0; j<cols; ++j){
				while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') && (bytes[i2+1] == '>')));
				columnNames[j] = new String(copyBytes(bytes, i1, i2)).replace("<,>", ",").replace("<<>", "<");
				i1 = i2+1;
			}
			i2 += 3;
			i1 = i2+1;
		}else{
			i2 += 5;
			i1 = i2+1;
		}
		if(cols > 0){//is not empty
			columnTypes = new String[cols];
			for(int j=0; j<cols; ++j){
				while((b = bytes[++i2]) != ',');
				columnTypes[j] = new String(copyBytes(bytes, i1, i2));
				i1 = i2+1;
			}
			i2 += 1;
		}
		//END HEADER
		
		//PAYLOAD
		columns = new Column[cols];
		i1 += 2;
		if(dfType.equals("default")){
			for(int j=0; j<cols; ++j){
				switch(columnTypes[j]){
				case "StringColumn":
					final String[] stringCol = new String[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') && (bytes[i2+1] == '>')));
						stringCol[k] = new String(copyBytes(bytes, i1, i2)).replace("<,>", ",").replace("<<>", "<");
						i1 = i2+1;
					}
					columns[j] = new StringColumn(stringCol);
					break;
				case "ByteColumn":
					final byte[] byteCol = new byte[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						byteCol[k] = Byte.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new ByteColumn(byteCol);
					break;
				case "ShortColumn":
					final short[] shortCol = new short[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						shortCol[k] = Short.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new ShortColumn(shortCol);
					break;
				case "IntColumn":
					final int[] intCol = new int[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						intCol[k] = Integer.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new IntColumn(intCol);
					break;
				case "LongColumn":
					final long[] longCol = new long[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						longCol[k] = Long.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new LongColumn(longCol);
					break;
				case "FloatColumn":
					final float[] floatCol = new float[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						floatCol[k] = Float.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new FloatColumn(floatCol);
					break;
				case "DoubleColumn":
					final double[] doubleCol = new double[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						doubleCol[k] = Double.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new DoubleColumn(doubleCol);
					break;
				case "BooleanColumn":
					final boolean[] booleanCol = new boolean[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						booleanCol[k] = Boolean.valueOf(new String(copyBytes(bytes, i1, i2)));
						i1 = i2+1;
					}
					columns[j] = new BooleanColumn(booleanCol);
					break;
				case "CharColumn":
					final char[] charCol = new char[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') && (bytes[i2+1] == '>')));
						charCol[k] = new String(copyBytes(bytes, i1, i2)).replace("<,>", ",").charAt(0);
						i1 = i2+1;
					}
					columns[j] = new CharColumn(charCol);
					break;
				}
			}
		}else if(dfType.equals("nullable")){
			for(int j=0; j<cols; ++j){
				switch(columnTypes[j]){
				case "NullableStringColumn":
					final String[] stringCol = new String[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') && (bytes[i2+1] == '>')));
						final String s = new String(copyBytes(bytes, i1, i2)).replace("<,>", ",").replace("<<>", "<");
						stringCol[k] = (!s.equals("null") ? s : null);
						i1 = i2+1;
					}
					columns[j] = new NullableStringColumn(stringCol);
					break;
				case "NullableByteColumn":
					final Byte[] byteCol = new Byte[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						byteCol[k] = (!s.equals("null") ? Byte.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableByteColumn(byteCol);
					break;
				case "NullableShortColumn":
					final Short[] shortCol = new Short[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						shortCol[k] = (!s.equals("null") ? Short.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableShortColumn(shortCol);
					break;
				case "NullableIntColumn":
					final Integer[] intCol = new Integer[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						intCol[k] = (!s.equals("null") ? Integer.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableIntColumn(intCol);
					break;
				case "NullableLongColumn":
					final Long[] longCol = new Long[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						longCol[k] = (!s.equals("null") ? Long.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableLongColumn(longCol);
					break;
				case "NullableFloatColumn":
					final Float[] floatCol = new Float[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						floatCol[k] = (!s.equals("null") ? Float.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableFloatColumn(floatCol);
					break;
				case "NullableDoubleColumn":
					final Double[] doubleCol = new Double[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						doubleCol[k] = (!s.equals("null") ? Double.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableDoubleColumn(doubleCol);
					break;
				case "NullableBooleanColumn":
					final Boolean[] booleanCol = new Boolean[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',');
						final String s = new String(copyBytes(bytes, i1, i2));
						booleanCol[k] = (!s.equals("null") ? Boolean.valueOf(s) : null);
						i1 = i2+1;
					}
					columns[j] = new NullableBooleanColumn(booleanCol);
					break;
				case "NullableCharColumn":
					final Character[] charCol = new Character[rows];
					for(int k=0; k<rows; ++k){
						while((b = bytes[++i2]) != ',' || ((bytes[i2-1] == '<') && (bytes[i2+1] == '>')));
						final String s = new String(copyBytes(bytes, i1, i2));
						charCol[k] = (!s.equals("null") 
								? new String(copyBytes(bytes, i1, i2))
										.replace("<,>", ",")
										.charAt(0) 
								: null);
						
						i1 = i2+1;
					}
					columns[j] = new NullableCharColumn(charCol);
					break;
				}
			}
		}
		//END PAYLOAD
		
		switch(dfType){
		case "default":
			if(columns.length == 0){
				df = new DefaultDataFrame();
			}else if(columnNames == null){
				df = new DefaultDataFrame(columns);
			}else{
				df = new DefaultDataFrame(columnNames, columns);
			}
			break;
		case "nullable":
			if(columns.length == 0){
				df = new NullableDataFrame();
			}else if(columnNames == null){
				df = new NullableDataFrame(columns);
			}else{
				df = new NullableDataFrame(columnNames, columns);
			}
			break;
		}
		return df;
	}
	
	/**
	 * Compresses the given array of bytes and modifies the first two bytes of the compressed 
	 * instance to represent a serialized DataFrame
	 * 
	 * @param bytes The bytes to compress
	 * @return The compressed array of bytes
	 * @throws IOException If any errors occur during compression
	 */
	private byte[] compress(byte[] bytes) throws IOException{
		final Deflater deflater = new Deflater();
		deflater.setInput(bytes);
		final ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length);
		deflater.finish();
		byte[] buffer = new byte[2048];
		while(!deflater.finished()){
			os.write(buffer, 0, deflater.deflate(buffer));
		}
		bytes = os.toByteArray();
		bytes[0] = DF_BYTE0;
		bytes[1] = DF_BYTE1;
		return bytes;
	}

	/**
	 * Decompresses the given array of bytes
	 * 
	 * @param bytes The bytes to decompress
	 * @return The decompressed array of bytes
	 * @throws IOException If any errors occur during decompression
	 */
	private byte[] decompress(final byte[] bytes) throws IOException{
		final Inflater inflater = new Inflater();
		//set zlib compression magic numbers
		bytes[0] = 0x78;
		bytes[1] = (byte)0x9C;
		inflater.setInput(bytes);
		final ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length);
		byte[] buffer = new byte[2048];
		try{
			while(!inflater.finished()){
				os.write(buffer, 0, inflater.inflate(buffer));
			}
		}catch(DataFormatException ex){
			throw new IOException("Invalid data format");
		}
		return os.toByteArray();
	}


	/**
	 * Escapes special characters in all given column names
	 * 
	 * @param names The column names to potentially escape
	 * @return The escaped version of all column names
	 */
	private String[] escapeColumnNames(final String[] names){
		final String[] escaped = new String[names.length];
		for(int i=0; i<names.length; ++i){
			escaped[i] = names[i].replace("<", "<<>");
			escaped[i] = escaped[i].replace(",", "<,>");
		}
		return escaped;
	}
	
	/**
	 * Escapes special characters in String- and Character objects
	 * 
	 * @param obj The String- or Character object to potentially escape
	 * @return The escaped string or character encoded as a byte array
	 */
	private byte[] escapeString(final Object obj){
		return obj.toString().replace("<", "<<>").replace(",", "<,>").concat(",").getBytes();
	}
	
	/**
	 * Ensures that the internal byte array has at least the specified minimum capacity
	 * 
	 * @param min The minimum capacity of the internal byte array
	 */
	private void ensureCapacity(final int min){
        if((min-bytes.length) >= 0){
            resize(min);
        }
    }

	/**
	 * Resizes the internal byte array to make sure it can hold at least the specifed
	 * amount of bytes
	 * 
	 * @param min The minimum capacity of the resized byte array
	 */
	private void resize(final int min){
        int newCapacity = 0;
        while(newCapacity < min){
        	newCapacity = bytes.length << 1;
        	if(newCapacity >= (1 << 30)){
        		newCapacity = Integer.MAX_VALUE;
        		break;
        	}
        }
        this.bytes = Arrays.copyOf(bytes, newCapacity);
	}
	
	/**
	 * Copies all bytes from the given byte array from (inclusive) the specified index 
	 * to (exclusive) the specified index
	 * 
	 * @param bytes The array of bytes
	 * @param from The position to copy from
	 * @param to The position to copy to
	 * @return An array holding all bytes from the given array from the specified position 
	 * 		   to the specified position
	 */
	private byte[] copyBytes(final byte[] bytes, final int from, final int to){
		int j = -1;
		final byte[] b = new byte[to-from];
		for(int i=from; i<to; ++i){
			b[++j] = bytes[i];
		}
		return b;
	}
	
	/**
	 * Background thread for concurrent write operations of DataFrames files.
	 *
	 */
	private class ConcurrentDFWriter implements Runnable {
		
		private ConcurrentWriter delegate;
		private File file;
		private DataFrame df;
		
		/**
		 * Constructs a new <code>ConcurrentDFWriter</code>
		 * 
		 * @param file The file to write
		 * @param df The DataFrame to write
		 * @param delegate The delegate for the callback
		 */
		ConcurrentDFWriter(final File file, final DataFrame df, final ConcurrentWriter delegate){
			this.file = file;
			this.df = df;
			this.delegate = delegate;
		}

		@Override
		public void run(){
			try{
				writeFile(file, df);
			}catch(IOException ex){
				if(delegate != null){
					delegate.onWritten(null);
				}
			}
			if(delegate != null){
				delegate.onWritten(file);
			}
		}
	}
	
	/**
	 * Background thread for concurrent read operations of DataFrames files.
	 *
	 */
	private class ConcurrentDFReader implements Runnable {
		
		private ConcurrentReader delegate;
		private File file;
		
		/**
		 * Constructs a new <code>ConcurrentDFReader</code>
		 * 
		 * @param file The file to read
		 * @param delegate The delegate for the callback
		 */
		ConcurrentDFReader(final File file, final ConcurrentReader delegate){
			this.file = file;
			this.delegate = delegate;
		}

		@Override
		public void run(){
			DataFrame df = null;
			try{
				df = readFile(file);
			}catch(IOException ex){ }
			
			if(delegate != null){
				delegate.onRead(df);
			}
		}
	}
}
