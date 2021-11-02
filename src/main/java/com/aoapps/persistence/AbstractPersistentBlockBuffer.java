/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2020, 2021  AO Industries, Inc.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
// import org.checkthread.annotations.NotThreadSafe;
// import org.checkthread.annotations.ThreadSafe;

/**
 * Base class for any implementation that treats a <code>PersistentBuffer</code>
 * as a set of allocatable blocks.
 *
 * @author  AO Industries, Inc.
 */
public abstract class AbstractPersistentBlockBuffer implements PersistentBlockBuffer {

	protected final PersistentBuffer pbuffer;

	protected AbstractPersistentBlockBuffer(PersistentBuffer pbuffer) {
		this.pbuffer = pbuffer;
	}

	// @NotThreadSafe
	@Override
	public boolean isClosed() {
		return pbuffer.isClosed();
	}

	// @NotThreadSafe
	@Override
	public void close() throws IOException {
		pbuffer.close();
	}

	// @ThreadSafe
	@Override
	public ProtectionLevel getProtectionLevel() {
		return pbuffer.getProtectionLevel();
	}

	// @NotThreadSafe
	@Override
	public void barrier(boolean force) throws IOException {
		pbuffer.barrier(force);
	}

	/**
	 * Checks that a request is within the bounds of the block.
	 */
	// @NotThreadSafe
	@SuppressWarnings("AssertWithSideEffects")
	protected void checkBounds(long id, long offset, long len) throws IllegalArgumentException, IOException {
		if(id < 0) throw new IllegalArgumentException("id < 0: " + id);
		if(offset < 0) throw new IllegalArgumentException("offset < 0: " + offset);
		if(len < 0) throw new IllegalArgumentException("len < 0: " + len);
		long totalSize = offset + len;
		if(totalSize < 0) {
			throw new IllegalArgumentException(
				"(offset + len) > Long.MAX_VALUE: offset = " + offset+", len = " + len
			);
		}
		// Only check block size when assertions are enabled, since this can be an expensive operation
		boolean assertEnabled = false;
		long blockSize = Long.MAX_VALUE;
		assert (assertEnabled = true) && (blockSize = getBlockSize(id)) >= 0;
		if(assertEnabled && totalSize > blockSize) {
			throw new IllegalArgumentException(
				"(offset + len) > blockSize: offset = " + offset+", len = " + len + ", blockSize = " + blockSize
			);
		}
	}

	// @NotThreadSafe
	@Override
	public void get(long id, long offset, byte[] buff, int off, int len) throws IOException {
		checkBounds(id, offset, len);
		long startAddress = getBlockAddress(id)+offset;
		ensureCapacity(startAddress+len);
		pbuffer.get(startAddress, buff, off, len);
	}

	// @NotThreadSafe
	@Override
	public int getInt(long id, long offset) throws IOException {
		checkBounds(id, offset, Integer.BYTES);
		long startAddress = getBlockAddress(id) + offset;
		ensureCapacity(startAddress + Integer.BYTES);
		return pbuffer.getInt(startAddress);
	}

	// @NotThreadSafe
	@Override
	public long getLong(long id, long offset) throws IOException {
		checkBounds(id, offset, Long.BYTES);
		long startAddress = getBlockAddress(id) + offset;
		ensureCapacity(startAddress + Long.BYTES);
		return pbuffer.getLong(startAddress);
	}

	// @NotThreadSafe
	@Override
	public InputStream getInputStream(long id, long offset, long length) throws IOException {
		checkBounds(id, offset, length);
		long startAddress = getBlockAddress(id)+offset;
		ensureCapacity(startAddress+length);
		return pbuffer.getInputStream(startAddress, length);
	}

	// @NotThreadSafe
	@Override
	public void put(long id, long offset, byte[] buff, int off, int len) throws IOException {
		checkBounds(id, offset, len);
		long startAddress = getBlockAddress(id)+offset;
		ensureCapacity(startAddress+len);
		pbuffer.put(startAddress, buff, off, len);
	}

	// @NotThreadSafe
	@Override
	public void putInt(long id, long offset, int value) throws IOException {
		checkBounds(id, offset, Integer.BYTES);
		long startAddress = getBlockAddress(id) + offset;
		ensureCapacity(startAddress + Integer.BYTES);
		pbuffer.putInt(startAddress, value);
	}

	// @NotThreadSafe
	@Override
	public void putLong(long id, long offset, long value) throws IOException {
		checkBounds(id, offset, Long.BYTES);
		long startAddress = getBlockAddress(id) + offset;
		ensureCapacity(startAddress + Long.BYTES);
		pbuffer.putLong(startAddress, value);
	}

	// @NotThreadSafe
	@Override
	public OutputStream getOutputStream(long id, long offset, long length) throws IOException {
		checkBounds(id, offset, length);
		long startAddress = getBlockAddress(id)+offset;
		ensureCapacity(startAddress+length);
		return pbuffer.getOutputStream(startAddress, length);
	}

	/**
	 * Gets the address of the block in the underlying persistent buffer.
	 * This should only be called for allocated blocks, implementations should
	 * check this with assertions.
	 */
	// @NotThreadSafe
	protected abstract long getBlockAddress(long id) throws IOException;

	/**
	 * Ensures the underlying persistent buffer is of adequate capacity.  Grows the
	 * underlying storage if needed.
	 */
	// @NotThreadSafe
	protected abstract void ensureCapacity(long capacity) throws IOException;
}
