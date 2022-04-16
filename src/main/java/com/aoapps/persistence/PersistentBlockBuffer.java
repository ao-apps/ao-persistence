/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.util.Iterator;

/**
 * A persistent set of blocks of arbitrary data.  Each block may be any
 * 64-bit size.  All implementations should have a relatively efficient
 * iteration in forward direction.  Some implementations may also have
 * efficient indexed access, and will implement the <code>RandomAccessPersistentBlockBuffer</code>
 * interface.
 *
 * @author  AO Industries, Inc.
 */
// TODO: @see  RandomAccessPersistentBlockBuffer
public interface PersistentBlockBuffer extends Closeable {

	/**
	 * Checks if this buffer is closed.
	 */
	boolean isClosed();

	/**
	 * Closes this buffer.
	 */
	@Override
	void close() throws IOException;

	/**
	 * Gets the protection level currently implemented by the buffer.
	 *
	 * @see  #barrier(boolean)
	 */
	ProtectionLevel getProtectionLevel();

	/**
	 * Ensures that all writes before this barrier occur before all writes after
	 * this barrier.  If <code>force</code> is <code>true</code>, will also
	 * commit to physical media synchronously before returning.  This request
	 * may be ignored or force downgraded to barrier-only depending on the current
	 * protection level.
	 *
	 * @see  #getProtectionLevel()
	 */
	void barrier(boolean force) throws IOException;

	/**
	 * <p>
	 * Iterates over the allocated block IDs in no specific order, with one
	 * exception: the first block allocated must be the first block iterated.
	 * This block may contain critical higher-level data structure meta data.
	 * If all blocks are deallocated, then the first one added has this same
	 * requirement.
	 * </p>
	 * <p>
	 * The <code>remove()</code> method may be used from the iterator in order
	 * to deallocate a block.  The block allocation should not be modified
	 * during the iteration through any means other than the iterator itself.
	 * An attempt will be made to throw <code>ConcurrentModificationException</code>
	 * in this case, but this is only intended to catch bugs.
	 * </p>
	 */
	Iterator<Long> iterateBlockIds() throws IOException;

	/**
	 * <p>
	 * Allocates a new block buffer that is at least as large as the requested space.
	 * The id should always be {@code >= 0}, higher level data structures may use the
	 * negative values for other purposes, such as indicating <code>null</code>
	 * with <code>-1</code>.
	 * </p>
	 * <p>
	 * In order to ensure the block allocation is completely in persistent storage,
	 * <code>{@link #barrier(boolean) barrier}</code> must be called.  This allows
	 * the contents of the block to be written and combined into a single <code>barrier</code>
	 * call.  If the system fails before <code>barrier</code> is called, the block
	 * may either be allocated or deallocated - it is up to higher-level data
	 * structures to determine which is the case.  In no event, however, will failing
	 * to call <code>barrier</code> after <code>allocate</code> cause corruption
	 * beyond that just described.
	 * </p>
	 * <p>
	 * This call may fail after the id is allocated and before the id is returned.
	 * This will manifest itself as an extra allocated block after recovery.
	 * </p>
	 */
	long allocate(long minimumSize) throws IOException;

	/**
	 * <p>
	 * Deallocates the block with the provided id.  The ids of other blocks
	 * will not be altered.  The space may later be reallocated with the same,
	 * or possibly different id.  The space may also be reclaimed.
	 * </p>
	 * <p>
	 * <code>{@link #barrier(boolean) barrier}</code> does not need to be called
	 * after a deallocation, but if not called previously deallocated blocks
	 * may reappear after a system failure.  It is up to higher-level data structures
	 * to detect this.  In no event, however, will failing to call <code>barrier</code>
	 * after <code>deallocate</code> cause corruption beyond that just described.
	 * </p>
	 *
	 * @throws IllegalStateException if the block is not allocated.
	 */
	void deallocate(long id) throws IOException, IllegalStateException;

	/**
	 * Gets the block size for the provided id.
	 */
	long getBlockSize(long id) throws IOException;

	/**
	 * Gets bytes from this block.  Bounds checking is performed only when assertions are enabled.
	 */
	void get(long id, long offset, byte[] buff, int off, int len) throws IOException;

	/**
	 * Gets an integer from this block.  Bounds checking is performed only when assertions are enabled.
	 */
	int getInt(long id, long offset) throws IOException;

	/**
	 * Gets a long from this block.  Bounds checking is performed only when assertions are enabled.
	 */
	long getLong(long id, long offset) throws IOException;

	/**
	 * Gets an input stream that reads from this buffer.  Bounds checking is performed only when assertions are enabled.
	 */
	InputStream getInputStream(long id, long offset, long length) throws IOException;

	/**
	 * Puts bytes to this block.  Bounds checking is performed only when assertions are enabled.
	 *
	 * @throws BufferOverflowException when out of range
	 */
	void put(long id, long offset, byte[] buff, int off, int len) throws IOException;

	/**
	 * Puts an integer to this block.  Bounds checking is performed only when assertions are enabled.
	 *
	 * @throws BufferOverflowException when out of range
	 */
	void putInt(long id, long offset, int value) throws IOException;

	/**
	 * Puts a long to this block.  Bounds checking is performed only when assertions are enabled.
	 *
	 * @throws BufferOverflowException when out of range
	 */
	void putLong(long id, long offset, long value) throws IOException;

	/**
	 * Gets an output stream that writes to this buffer.  Bounds checking is performed only when assertions are enabled.
	 *
	 * @throws BufferOverflowException when out of range
	 */
	OutputStream getOutputStream(long id, long offset, long length) throws IOException;
}
