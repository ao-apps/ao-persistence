/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2012, 2013, 2016, 2017, 2019, 2020, 2021, 2022  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-persistence.
 *
 * ao-persistence is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-persistence is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-persistence.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.aoapps.persistence;

import com.aoapps.collections.AoArrays;
import com.aoapps.lang.util.BufferManager;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A set of static utility methods to help in the selection, creation, and management
 * of persistent collections.
 *
 * @author  AO Industries, Inc.
 */
public abstract class PersistentCollections {

	/** Make no instances. */
	private PersistentCollections() {throw new AssertionError();}

	private static final Logger logger = Logger.getLogger(PersistentCollections.class.getName());

	/**
	 * The value is never modified therefore thread safe.
	 */
	private static final byte[] zeros = new byte[BufferManager.BUFFER_SIZE];

	/**
	 * Writes the requested number of zeros to the provided output.
	 */
	/*
	static void fillZeros(DataOutput out, long count) throws IOException {
		if(count<0) throw new IllegalArgumentException("count<0: "+count);
		while(count>BufferManager.BUFFER_SIZE) {
			out.write(zeros, 0, BufferManager.BUFFER_SIZE);
			count -= BufferManager.BUFFER_SIZE;
		}
		if(count>0) out.write(zeros, 0, (int)count);
	}*/

	/**
	 * Writes the requested number of zeros to the provided RandomAccessFile, but
	 * only if they do not already contain zeros.  This is to avoid unnecessary
	 * writes on flash media.  This may also have a positive interaction with
	 * sparse files.
	 *
	 * @return  true  when the byteBuffer was written to
	 */
	public static boolean ensureZeros(RandomAccessFile raf, long position, long count) throws IOException {
		if(count<0) throw new IllegalArgumentException("count<0: "+count);
		boolean modified = false;
		byte[] buff = BufferManager.getBytes();
		try {
			while(count>BufferManager.BUFFER_SIZE) {
				raf.seek(position);
				raf.readFully(buff, 0, BufferManager.BUFFER_SIZE);
				if(!Arrays.equals(buff, zeros)) {
					raf.seek(position);
					raf.write(zeros, 0, BufferManager.BUFFER_SIZE);
					modified = true;
				}
				position += BufferManager.BUFFER_SIZE;
				count -= BufferManager.BUFFER_SIZE;
			}
			if(count>0) {
				raf.seek(position);
				raf.readFully(buff, 0, (int)count);
				if(!AoArrays.equals(buff, zeros, 0, (int)count)) {
					raf.seek(position);
					raf.write(zeros, 0, (int)count);
					modified = true;
				}
			}
		} finally {
			BufferManager.release(buff, false);
		}
		return modified;
	}

	/**
	 * Stores the requested number of zeros to the provided ByteBuffer, but
	 * only if they do not already contain zeros.  This is to avoid unnecessary
	 * writes on flash media.  This may also have a positive interaction with
	 * sparse files.
	 *
	 * @return  true  when the byteBuffer was written to
	 */
	// TODO: Try with readLong instead of copying the array values
	public static boolean ensureZeros(ByteBuffer byteBuffer, int position, int count) throws IOException {
		if(count<0) throw new IllegalArgumentException("count<0: "+count);
		boolean modified = false;
		/*
		byte[] buff = BufferManager.getBytes();
		try {
			while(count>BufferManager.BUFFER_SIZE) {
				byteBuffer.position(position);
				byteBuffer.get(buff, 0, BufferManager.BUFFER_SIZE);
				if(!Arrays.equals(buff, zeros)) {
					byteBuffer.position(position);
					byteBuffer.put(zeros, 0, BufferManager.BUFFER_SIZE);
					modified = true;
				}
				position += BufferManager.BUFFER_SIZE;
				count -= BufferManager.BUFFER_SIZE;
			}
			if(count>0) {
				byteBuffer.position(position);
				byteBuffer.get(buff, 0, count);
				if(!com.aoapps.collections.AoArrays.equals(buff, zeros, 0, count)) {
					byteBuffer.position(position);
					byteBuffer.put(zeros, 0, count);
					modified = true;
				}
			}
		} finally {
			BufferManager.release(buff);
		}
		 */
		ByteOrder previousByteOrder = byteBuffer.order();
		ByteOrder nativeByteOrder = ByteOrder.nativeOrder();
		try {
			if(previousByteOrder!=nativeByteOrder) byteBuffer.order(nativeByteOrder);
			// Align to 8-byte boundary
			byteBuffer.position(position);
			while(count > 0 && (position & (Long.BYTES - 1)) != 0) {
				byte b = byteBuffer.get();
				if(b != 0) {
					byteBuffer.put(position, (byte)0);
					modified = true;
				}
				position++;
				count--;
			}
			// Read in long values to get 64 bits at a time
			do {
				int nextCount = count - Long.BYTES;
				if(nextCount < 0) break;
				if(byteBuffer.getLong() != 0) {
					byteBuffer.putLong(position, 0);
					modified = true;
				}
				position += Long.BYTES;
				count = nextCount;
			} while(true);
			// Trailing bytes
			while(count > 0) {
				byte b = byteBuffer.get();
				if(b != 0) {
					byteBuffer.put(position, (byte)0);
					modified = true;
				}
				position++;
				count--;
			}
		} finally {
			if(previousByteOrder!=nativeByteOrder) byteBuffer.order(previousByteOrder);
		}

		return modified;
	}

	/**
	 * Writes the requested number of zeros to the provided buffer.
	 */
	/*
	static void fillZeros(ByteBuffer buffer, long count) throws IOException {
		if(count<0) throw new IllegalArgumentException("count<0: "+count);
		while(count>BufferManager.BUFFER_SIZE) {
			buffer.put(zeros, 0, BufferManager.BUFFER_SIZE);
			count -= BufferManager.BUFFER_SIZE;
		}
		if(count>0) buffer.put(zeros, 0, (int)count);
	}*/

	/**
	 * Selects the most efficient temporary <code>PersistentBuffer</code> for the current
	 * machine and the provided maximum buffer size.  The buffer will be backed by a temporary
	 * file that will be deleted on buffer close or JVM shutdown.  The protection level will be
	 * <code>NONE</code>.  The order of preference is:
	 * <ol>
	 *   <li><code>MappedPersistentBuffer</code></li>
	 *   <li><code>LargeMappedPersistentBuffer</code></li>
	 *   <li><code>RandomAccessFileBuffer</code></li>
	 * </ol>
	 *
	 * @param maximumCapacity The maximum size of data that may be stored in the
	 *                        buffer.  To ensure no limits, use <code>Long.MAX_VALUE</code>.
	 */
	public static PersistentBuffer getPersistentBuffer(long maximumCapacity) throws IOException {
		// If < 1 GB, use mapped buffer
		if(maximumCapacity < (1L << 30)) {
			return new MappedPersistentBuffer();
		}
		// No mmap for 32-bit
		String dataModel = System.getProperty("sun.arch.data.model");
		if(
			"32".equals(dataModel)
		) {
			return new RandomAccessFileBuffer();
		}
		// Use mmap for 64-bit
		if(
			!"64".equals(dataModel)
		) {
			logger.log(Level.WARNING, "Unexpected value for system property sun.arch.data.model, assuming 64-bit virtual machine: sun.arch.data.model={0}", dataModel);
		}
		return new LargeMappedPersistentBuffer();
	}

	/**
	 * Selects the most efficient <code>PersistentBuffer</code> for the current
	 * machine and the provided maximum buffer size.  The order of preference is:
	 * <ol>
	 *   <li><code>MappedPersistentBuffer</code></li>
	 *   <li><code>LargeMappedPersistentBuffer</code></li>
	 *   <li><code>RandomAccessFileBuffer</code></li>
	 * </ol>
	 *
	 * @param maximumCapacity The maximum size of data that may be stored in the
	 *                        buffer.  If the random access file is larger than this value,
	 *                        the length of the file is used instead.
	 *                        To ensure no limits, use <code>Long.MAX_VALUE</code>.
	 */
	public static PersistentBuffer getPersistentBuffer(RandomAccessFile raf, ProtectionLevel protectionLevel, long maximumCapacity) throws IOException {
		if(maximumCapacity < (1L << 30)) {
			long len = raf.length();
			if(maximumCapacity<len) maximumCapacity = len;
		}
		// If < 1 GB, use mapped buffer
		if(maximumCapacity < (1L << 30)) {
			return new MappedPersistentBuffer(raf, protectionLevel);
		}
		// No mmap for 32-bit
		String dataModel = System.getProperty("sun.arch.data.model");
		if(
			"32".equals(dataModel)
		) {
			return new RandomAccessFileBuffer(raf, protectionLevel);
		}
		// Use mmap for 64-bit
		if(
			!"64".equals(dataModel)
		) {
			logger.log(Level.WARNING, "Unexpected value for system property sun.arch.data.model, assuming 64-bit virtual machine: sun.arch.data.model={0}", dataModel);
		}
		return new LargeMappedPersistentBuffer(raf, protectionLevel);
	}

	/**
	 * Selects the most efficient <code>Serializer</code> for the provided class.
	 */
	@SuppressWarnings("unchecked")
	public static <E> Serializer<E> getSerializer(Class<E> type) {
		if(type==Boolean.class) return (Serializer<E>)new BooleanSerializer();
		if(type==Byte.class) return (Serializer<E>)new ByteSerializer();
		if(type==Character.class) return (Serializer<E>)new CharacterSerializer();
		if(type==Double.class) return (Serializer<E>)new DoubleSerializer();
		if(type==Float.class) return (Serializer<E>)new FloatSerializer();
		if(type==Integer.class) return (Serializer<E>)new IntegerSerializer();
		if(type==Long.class) return (Serializer<E>)new LongSerializer();
		if(type==Short.class) return (Serializer<E>)new ShortSerializer();
		// Arrays
		Class<?> componentType = type.getComponentType();
		if(componentType!=null) {
			if(componentType==Byte.class) return (Serializer<E>)new ByteArraySerializer();
			if(componentType==Character.class) return (Serializer<E>)new CharArraySerializer();
		}
		// Default Java serialization
		return new ObjectSerializer<>(type);
	}

	/**
	 * Gets the most efficient <code>PersistentBlockBuffer</code> for the provided
	 * provided <code>Serializer</code>.  If using fixed record sizes, the size of
	 * the block buffer is rounded up to the nearest power of two, to help
	 * alignment with system page tables.
	 *
	 * @param serializer            The <code>Serializer</code> that will be used to write to the blocks
	 * @param pbuffer               The <code>PersistenceBuffer</code> that will be wrapped by the block buffer
	 * @param additionalBlockSpace  The maximum additional space needed beyond the space used by the serializer.  This may be used
	 *                              for linked list pointers, for example.
	 */
	public static PersistentBlockBuffer getPersistentBlockBuffer(Serializer<?> serializer, PersistentBuffer pbuffer, long additionalBlockSpace) throws IOException {
		if(additionalBlockSpace<0) throw new IllegalArgumentException("additionalBlockSpace<0: "+additionalBlockSpace);
		if(serializer.isFixedSerializedSize()) {
			// Use power-of-two fixed size blocks if possible
			long serSize = serializer.getSerializedSize(null);
			long minimumSize = serSize + additionalBlockSpace;
			if(minimumSize<0) throw new AssertionError("Long wraparound: "+serSize+"+"+minimumSize+"="+minimumSize);
			if(minimumSize==0) minimumSize=1;
			long highestOneBit = Long.highestOneBit(minimumSize);
			return new FixedPersistentBlockBuffer(
				pbuffer,
				highestOneBit==(1L<<62)
				? minimumSize           // In range 2^62-2^63-1, cannot round up to next highest, use minimum size
				: minimumSize==highestOneBit
				? minimumSize           // minimumSize is a power of two
				: (highestOneBit<<1)    // use next-highest power of two
			);
		}
		// Then use dynamic sized blocks
		return new DynamicPersistentBlockBuffer(pbuffer);
	}

	/**
	 * Gets the most efficient <code>RandomAccessPersistentBlockBuffer</code> for the provided
	 * provided <code>Serializer</code>.  The serializer must be provide a fixed serializer size.
	 * The size of the block buffer is rounded up to the nearest power of two, to help alignment
	 * with system page tables.
	 *
	 * @param serializer            The <code>Serializer</code> that will be used to write to the blocks
	 * @param pbuffer               The <code>PersistenceBuffer</code> that will be wrapped by the block buffer
	 * @param additionalBlockSpace  The maximum additional space needed beyond the space used by the serializer.  This may be used
	 *                              for linked list pointers, for example.
	 */
	/*
	public static RandomAccessPersistentBlockBuffer getRandomAccessPersistentBlockBuffer(Serializer<?> serializer, PersistentBuffer pbuffer, long additionalBlockSpace) throws IOException {
		if(additionalBlockSpace<0) throw new IllegalArgumentException("additionalBlockSpace<0: "+additionalBlockSpace);
		// Use power-of-two fixed size blocks if possible
		if(!serializer.isFixedSerializedSize()) throw new IllegalArgumentException("serializer does not created fixed size output");
		long serSize = serializer.getSerializedSize(null);
		long minimumSize = serSize + additionalBlockSpace;
		if(minimumSize<0) throw new AssertionError("Long wraparound: "+serSize+"+"+minimumSize+"="+minimumSize);
		long highestOneBit = Long.highestOneBit(minimumSize);
		return new FixedPersistentBlockBuffer(
			pbuffer,
			highestOneBit==(1L<<62)
			? minimumSize           // In range 2^62-2^63-1, cannot round up to next highest, use minimum size
			: minimumSize==highestOneBit
			? minimumSize           // minimumSize is a power of two
			: (highestOneBit<<1)    // use next-highest power of two
		);
	}*/
}
