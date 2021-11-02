/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2017, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.lang.io.IoUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
// import org.checkthread.annotations.NotThreadSafe;
// import org.checkthread.annotations.ThreadSafe;

/**
 * Provides a base implementation of <code>PersistentBuffer</code> in terms of
 * basic read/write methods.  This class is not thread safe.
 *
 * @author  AO Industries, Inc.
 */
public abstract class AbstractPersistentBuffer implements PersistentBuffer {

	protected final ProtectionLevel protectionLevel;
	private final byte[] ioBuffer = new byte[Long.BYTES];

	protected AbstractPersistentBuffer(ProtectionLevel protectionLevel) {
		this.protectionLevel = protectionLevel;
	}

	// @ThreadSafe
	@Override
	public ProtectionLevel getProtectionLevel() {
		return protectionLevel;
	}

	/**
	 * Implemented as calls to <code>getSome(long,byte[],int,int)</code>
	 *
	 * @see  #getSome(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public void get(long position, byte[] buff, int off, int len) throws IOException {
		while(len > 0) {
			int count = getSome(position, buff, off, len);
			position += count;
			off += count;
			len -= count;
		}
	}

	/**
	 * Implemented as call to <code>readByte(long)</code>
	 *
	 * @see  #get(long)
	 */
	// @NotThreadSafe
	@Override
	public boolean getBoolean(long position) throws IOException {
		return get(position) != 0;
	}

	/**
	 * Implemented as call to <code>get(long,byte[],int,int)</code>.  For performance
	 * reasons, it is strongly recommended to provide a more efficient implementation
	 * of this method.
	 *
	 * @see  #get(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public byte get(long position) throws IOException {
		get(position, ioBuffer, 0, Byte.BYTES);
		return ioBuffer[0];
	}

	/**
	 * Implemented as call to <code>get(long,byte[],int,int)</code>
	 *
	 * @see  #get(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public int getInt(long position) throws IOException {
		get(position, ioBuffer, 0, Integer.BYTES);
		return IoUtils.bufferToInt(ioBuffer);
	}

	/**
	 * Implemented as call to <code>get(long,byte[],int,int)</code>
	 *
	 * @see  #get(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public long getLong(long position) throws IOException {
		get(position, ioBuffer, 0, Long.BYTES);
		return IoUtils.bufferToLong(ioBuffer);
	}

	/**
	 * Implemented as call to <code>put(long,byte[],int,int)</code>.  For performance
	 * reasons, it is strongly recommended to provide a more efficient implementation
	 * of this method.
	 *
	 * @see  #put(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public void put(long position, byte value) throws IOException {
		ioBuffer[0] = value;
		put(position, ioBuffer, 0, Byte.BYTES);
	}

	/**
	 * Implemented as call to <code>write(long,byte[],int,int)</code>
	 *
	 * @see  #put(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public void putInt(long position, int value) throws IOException {
		IoUtils.intToBuffer(value, ioBuffer);
		put(position, ioBuffer, 0, Integer.BYTES);
	}

	/**
	 * Implemented as call to <code>write(long,byte[],int,int)</code>
	 *
	 * @see  #put(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public void putLong(long position, long value) throws IOException {
		IoUtils.longToBuffer(value, ioBuffer);
		put(position, ioBuffer, 0, Long.BYTES);
	}

	/**
	 * Implemented as calls to <code>get(long)</code> and <code>getSome(long,byte[],int,int)</code>
	 *
	 * @see  #get(long)
	 * @see  #getSome(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public InputStream getInputStream(final long position, final long length) throws IOException, BufferUnderflowException {
		return new InputStream() {
			private boolean closed = false;
			private long curPosition = position;
			private long curRemaining = length;

			@Override
			public int read() throws IOException {
				if(closed) throw new IOException("Stream closed");
				if(curRemaining<1) throw new BufferUnderflowException();
				int value = get(curPosition++)&255;
				curRemaining--;
				return value;
			}

			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				if(closed) throw new IOException("Stream closed");
				if(len<0) throw new IllegalArgumentException("len<0: "+len);
				if(len==0) return 0;
				if(curRemaining<len) {
					if(curRemaining<=0) return -1; // End of file
					len = (int)curRemaining;
				}
				try {
					int numBytes = getSome(curPosition, b, off, len);
					curPosition+=numBytes;
					curRemaining-=numBytes;
					return numBytes;
				} catch(BufferUnderflowException err) {
					return -1;
				}
			}

			@Override
			public long skip(long n) throws IOException {
				if(closed) throw new IOException("Stream closed");
				if(n<=0) return 0;
				if(curRemaining<n) {
					if(curRemaining<=0) return 0;
					n = curRemaining;
				}
				curPosition+=n;
				curRemaining-=n;
				return n;
			}

			@Override
			public int available() throws IOException {
				if(closed) throw new IOException("Stream closed");
				return 0;
			}

			@Override
			public void close() {
				closed = true;
			}
		};
	}

	/**
	 * Implemented as calls to <code>put(long,byte)</code>
	 * and <code>put(long,byte[],int,int)</code>.
	 *
	 * @see  #put(long, byte)
	 * @see  #put(long, byte[], int, int)
	 */
	// @NotThreadSafe
	@Override
	public OutputStream getOutputStream(final long position, final long length) throws IOException, BufferOverflowException {
		return new OutputStream() {
			private boolean closed = false;
			private long curPosition = position;
			private long curRemaining = length;

			@Override
			public void write(int b) throws IOException {
				if(closed) throw new IOException("Stream closed");
				if(curRemaining<1) throw new BufferOverflowException();
				put(curPosition++, (byte)b);
				curRemaining--;
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				if(closed) throw new IOException("Stream closed");
				if(curRemaining<len) throw new BufferOverflowException();
				put(curPosition, b, off, len);
				curPosition+=len;
				curRemaining-=len;
			}

			@Override
			public void close() {
				closed = true;
			}
		};
	}
}
