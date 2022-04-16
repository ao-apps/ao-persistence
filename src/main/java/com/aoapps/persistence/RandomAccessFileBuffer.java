/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2012, 2016, 2017, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.tempfiles.TempFileContext;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;

/**
 * Uses <code>RandomAccessFile</code> for persistence.  Obtains a shared lock
 * on the channel for read-only mode or an exclusive lock for write mode.  The
 * lock is held until the buffer is closed.
 *
 * @author  AO Industries, Inc.
 */
public class RandomAccessFileBuffer extends AbstractPersistentBuffer {

	private final TempFileContext tempFileContext;
	private final RandomAccessFile raf;
	private final FileChannel channel;
	private boolean closed;

	/**
	 * Creates a read-write buffer backed by a temporary file.  The protection level
	 * is set to <code>NONE</code>.  The temporary file will be deleted when this
	 * buffer is closed or on JVM shutdown.
	 */
	public RandomAccessFileBuffer() throws IOException {
		super(ProtectionLevel.NONE);
		tempFileContext = new TempFileContext();
		raf = new RandomAccessFile(tempFileContext.createTempFile("RandomAccessFileBuffer_").getFile(), "rw");
		channel = raf.getChannel();
		// Lock the file
		channel.lock(0L, Long.MAX_VALUE, false);
	}

	/**
	 * Creates a read-write buffer with <code>BARRIER</code> protection level.
	 */
	public RandomAccessFileBuffer(String name) throws IOException {
		this(new RandomAccessFile(name, "rw"), ProtectionLevel.BARRIER);
	}

	/**
	 * Creates a buffer.
	 */
	public RandomAccessFileBuffer(String name, ProtectionLevel protectionLevel) throws IOException {
		this(new RandomAccessFile(name, protectionLevel==ProtectionLevel.READ_ONLY ? "r" : "rw"), protectionLevel);
	}

	/**
	 * Creates a read-write buffer with <code>BARRIER</code> protection level.
	 */
	public RandomAccessFileBuffer(File file) throws IOException {
		this(new RandomAccessFile(file, "rw"), ProtectionLevel.BARRIER);
	}

	/**
	 * Creates a buffer.
	 */
	public RandomAccessFileBuffer(File file, ProtectionLevel protectionLevel) throws IOException {
		this(new RandomAccessFile(file, protectionLevel==ProtectionLevel.READ_ONLY ? "r" : "rw"), protectionLevel);
	}

	/**
	 * Creates a buffer using the provided <code>RandomAccessFile</code>.
	 */
	public RandomAccessFileBuffer(RandomAccessFile raf, ProtectionLevel protectionLevel) throws IOException {
		super(protectionLevel);
		this.tempFileContext = null;
		this.raf = raf;
		channel = raf.getChannel();
		// Lock the file
		channel.lock(0L, Long.MAX_VALUE, protectionLevel==ProtectionLevel.READ_ONLY);
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	/**
	 * @deprecated The finalization mechanism is inherently problematic.
	 */
	@Deprecated(since="9")
	@Override
	@SuppressWarnings("FinalizeDeclaration")
	protected void finalize() throws Throwable {
		try {
			close();
		} finally {
			super.finalize();
		}
	}

	@Override
	public void close() throws IOException {
		closed = true;
		raf.close();
		if(tempFileContext != null) tempFileContext.close();
	}

	@Override
	public long capacity() throws IOException {
		return raf.length();
	}

	@Override
	public void setCapacity(long newLength) throws IOException {
		long oldLength = capacity();
		raf.setLength(newLength);
		if(newLength>oldLength) {
			// Ensure zero-filled
			ensureZeros(oldLength, newLength - oldLength);
		}
	}

	@Override
	public int getSome(long position, byte[] buff, int off, int len) throws IOException {
		raf.seek(position);
		int count = raf.read(buff, off, len);
		if(count<0) throw new BufferUnderflowException();
		return count;
	}

	/**
	 * Gets a single byte from the buffer.
	 */
	@Override
	public byte get(long position) throws IOException {
		raf.seek(position);
		return raf.readByte();
	}

	@Override
	public void ensureZeros(long position, long len) throws IOException {
		PersistentCollections.ensureZeros(raf, position, len);
	}


	/**
	 * Puts a single byte in the buffer.
	 */
	@Override
	public void put(long position, byte value) throws IOException {
		if(position>=capacity()) throw new BufferOverflowException();
		raf.seek(position);
		raf.write(value);
	}

	@Override
	public void put(long position, byte[] buff, int off, int len) throws IOException {
		if((position+len)>capacity()) throw new BufferOverflowException();
		raf.seek(position);
		raf.write(buff, off, len);
	}

	/**
	 * There is not currently a way to provide a barrier without using <code>force</code>.
	 * This just uses force for each case.
	 */
	@Override
	public void barrier(boolean force) throws IOException {
		if(protectionLevel.compareTo(ProtectionLevel.BARRIER)>=0) channel.force(false);
	}
}
