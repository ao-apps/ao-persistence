/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2012, 2013, 2016, 2019, 2020, 2021  AO Industries, Inc.
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
 * along with ao-persistence.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoapps.persistence;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ReadOnlyBufferException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
// import org.checkthread.annotations.NotThreadSafe;
// import org.checkthread.annotations.ThreadSafe;

/**
 * This buffer allows very large address spaces for testing purposes.  It is backed by
 * a map of 4096-byte buffers in heap.  Each buffer will only be created when first written to.
 * The buffer will not be removed, even if it becomes all zeros again.
 *
 * @author  AO Industries, Inc.
 */
public class SparseBuffer extends AbstractPersistentBuffer {

	private static final int BIT_SHIFT = 12;

	private static final int BLOCK_SIZE = 1 << BIT_SHIFT;

	private static final int BLOCK_MASK = BLOCK_SIZE - 1;

	private boolean isClosed = false;
	private long capacity = 0L;
	private final Map<Long, byte[]> buffers = new HashMap<>();

	/**
	 * Creates a read-write test buffer with protection level <code>NONE</code>.
	 */
	public SparseBuffer() {
		this(ProtectionLevel.NONE);
	}

	public SparseBuffer(ProtectionLevel protectionLevel) {
		super(protectionLevel);
	}

	// @NotThreadSafe
	@Override
	public boolean isClosed() {
		return isClosed;
	}

	// @NotThreadSafe
	@Override
	public void close() throws IOException {
		isClosed = true;
	}

	// @NotThreadSafe
	@Override
	public long capacity() throws IOException {
		return capacity;
	}

	// @NotThreadSafe
	@Override
	public void setCapacity(long newCapacity) throws IOException {
		if(newCapacity<0) throw new IllegalArgumentException("capacity<0: "+capacity);
		if(protectionLevel==ProtectionLevel.READ_ONLY) throw new ReadOnlyBufferException();
		if(newCapacity>capacity) {
			// TODO: Zero the partial part of the last page when growing
		}
		this.capacity = newCapacity;
		// Discard any pages above new capacity
		long highestPage = newCapacity >>> BIT_SHIFT;
		if((newCapacity&0x7ff)!=0) highestPage++;
		Iterator<Long> keyIter = buffers.keySet().iterator();
		while(keyIter.hasNext()) {
			long key = keyIter.next();
			if(key>highestPage) keyIter.remove();
		}
	}

	// @NotThreadSafe
	@Override
	public int getSome(long position, byte[] buff, int off, int len) throws IOException {
		get(position, buff, off, len);
		return len;
	}

	@Override
	// @NotThreadSafe
	public void get(long position, byte[] buff, int off, int len) throws IOException {
		if((position+len)>capacity) throw new BufferUnderflowException();
		// TODO: More efficient algorithm using blocks calling System.arraycopy.
		long lastBufferNum = -1;
		byte[] lastBuffer = null;
		while(len>0) {
			long blockNum = position >>> BIT_SHIFT;
			if(blockNum!=lastBufferNum) lastBuffer = buffers.get(lastBufferNum = blockNum);
			buff[off] = lastBuffer==null ? 0 : lastBuffer[(int)(position & BLOCK_MASK)];
			position++;
			off++;
			len--;
		}
	}

	// @NotThreadSafe
	@Override
	public void ensureZeros(long position, long len) throws IOException {
		throw new NotImplementedException("Implement when first needed");
	}

	// @NotThreadSafe
	@Override
	public void put(long position, byte[] buff, int off, int len) throws IOException {
		if(protectionLevel==ProtectionLevel.READ_ONLY) throw new ReadOnlyBufferException();
		if((position+len)>capacity) throw new BufferOverflowException();
		// TODO: More efficient algorithm using blocks calling System.arraycopy.
		long lastBufferNum = -1;
		byte[] lastBuffer = null;
		while(len>0) {
			long blockNum = position >>> BIT_SHIFT;
			if(blockNum!=lastBufferNum) lastBuffer = buffers.get(lastBufferNum = blockNum);
			byte value = buff[off];
			// Only create the buffer when a non-zero value is being added
			if(lastBuffer==null && value!=0) buffers.put(lastBufferNum, lastBuffer = new byte[4096]);
			if(lastBuffer!=null) lastBuffer[(int)(position & BLOCK_MASK)] = value;
			position++;
			off++;
			len--;
		}
	}

	/**
	 * Does nothing because this is only a volatile test buffer.
	 */
	// @ThreadSafe
	@Override
	public void barrier(boolean force) throws IOException {
	}
}
