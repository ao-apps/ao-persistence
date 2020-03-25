/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2013, 2016, 2017, 2019, 2020  AO Industries, Inc.
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
package com.aoindustries.persistence;

import com.aoindustries.collections.AoArrays;
import com.aoindustries.exception.WrappedException;
import com.aoindustries.io.IoUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractSequentialList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
// import org.checkthread.annotations.NotThreadSafe;

/**
 * <p>
 * Serializes and stores objects in a persistent buffer.  Unlike <code>FileList</code> which
 * is intended for efficient <code>RandomAccess</code>,
 * this is a linked list implementation and has the expected benefits and costs.
 * There are no size limits to the stored data.
 * </p>
 * <p>
 * This class is not thread-safe.  It is absolutely critical that external
 * synchronization be applied.
 * </p>
 * <p>
 * The objects are serialized using the standard Java serialization, unless a
 * <code>Serializer</code> is provided.  If an object that is not <code>Serializable</code>
 * is to be stored, a <code>Serializer</code> must be provided.  <code>Serializer</code>s
 * may also provide a more efficient or more compact representation of an object.
 * </p>
 * <p>
 * This class is intended for scalability and persistence, not for intra-process
 * or intra-thread shared data.
 * </p>
 * <p>
 * The first block allocated is a header:
 * </p>
 * <pre>
 *     Offset   Type  Description
 *      0- 3    ASCII "PLL\n"
 *      4- 7    int   version
 *      8-15    long  block id of the head or <code>END_PTR</code> if empty.
 *     16-23    long  block id of the tail or <code>END_PTR</code> if empty.
 * </pre>
 * <p>
 * Each entry consists of:
 * </p>
 * <pre>
 *     Offset   Name        Type     Description
 *       0- 7   next        long     block id of next, <code>END_PTR</code> for last element
 *       8-15   prev        long     block id of prev, <code>END_PTR</code> for first element
 *      16-23   dataSize    long     the size of the serialized data, <code>-1</code> means null element
 *      24+     data        data     the binary data
 * </pre>
 *
 * <pre>
 * TODO: Add corrupt flag, set on exceptions?  Cause immediate crash recovery?
 * TODO: Similar thing for the underlying block buffers and byte buffers?
 * </pre>
 *
 * @author  AO Industries, Inc.
 */
public class PersistentLinkedList<E> extends AbstractSequentialList<E> implements List<E>, Deque<E> {

	private static final Logger logger = Logger.getLogger(PersistentLinkedList.class.getName());

	private static final byte[] MAGIC={'P', 'L', 'L', '\n'};

	private static final int VERSION = 3;

	/**
	 * The value used to represent an ending pointer.
	 */
	private static final long END_PTR = -2;

	/**
	 * The constant location of the head pointer.
	 */
	private static final long HEAD_OFFSET = MAGIC.length+4;

	/**
	 * The constant location of the tail pointer.
	 */
	private static final long TAIL_OFFSET = HEAD_OFFSET+8;

	/**
	 * The total number of bytes in the header.
	 */
	private static final int HEADER_SIZE = (int)(TAIL_OFFSET + 8);

	/**
	 * The block offset for <code>next</code>.
	 */
	private static final int NEXT_OFFSET = 0;

	/**
	 * The block offset for <code>prev</code>.
	 */
	private static final int PREV_OFFSET = 8;

	/**
	 * The block offset for <code>dataSize</code>.
	 */
	private static final int DATA_SIZE_OFFSET = 16;

	/**
	 * The block offset for the beginning of the data.
	 */
	private static final int DATA_OFFSET = 24;

	/**
	 * Value used to indicate <code>null</code> data.
	 */
	private static final long DATA_SIZE_NULL = -1;

	final private Serializer<E> serializer;
	final private PersistentBlockBuffer blockBuffer;

	final private byte[] ioBuffer = new byte[Math.max(DATA_OFFSET, MAGIC.length)];

	// Cached for higher performance
	private long metaDataBlockId;
	private long _head;
	private long _tail;
	private long _size;

	// <editor-fold desc="Constructors">
	/**
	 * Constructs a list backed by a temporary file using standard serialization.
	 * The temporary file will be deleted at JVM shutdown.
	 * Operates in constant time.
	 *
	 * @see  PersistentCollections#getPersistentBuffer(long)
	 */
	public PersistentLinkedList(Class<E> type) throws IOException {
		serializer = PersistentCollections.getSerializer(type);
		blockBuffer = PersistentCollections.getPersistentBlockBuffer(
			serializer,
			PersistentCollections.getPersistentBuffer(Long.MAX_VALUE),
			Math.max(HEADER_SIZE, DATA_OFFSET)
		);
		checkConsistency(true, true);
	}

	/**
	 * Constructs a list with a temporary file using standard serialization containing all of the provided elements.
	 * The temporary file will be deleted at JVM shutdown.
	 * Operates in linear time.
	 */
	public PersistentLinkedList(Class<E> type, Collection<? extends E> c) throws IOException {
		this(type);
		addAll(c);
	}

	/**
	 * Constructs a list backed by the provided persistent buffer using the most efficient serialization
	 * for the provided type.
	 * Operates in linear time in order to cache the size.
	 *
	 * @see  PersistentCollections#getSerializer(java.lang.Class)
	 */
	public PersistentLinkedList(PersistentBuffer pbuffer, Class<E> type) throws IOException {
		this(pbuffer, PersistentCollections.getSerializer(type));
	}

	/**
	 * Constructs a list backed by the provided persistent buffer.
	 * Operates in linear time in order to cache the size and perform failure recovery.
	 */
	public PersistentLinkedList(PersistentBuffer pbuffer, Serializer<E> serializer) throws IOException {
		this.serializer = serializer;
		blockBuffer = PersistentCollections.getPersistentBlockBuffer(
			serializer,
			pbuffer,
			Math.max(HEADER_SIZE, DATA_OFFSET)
		);
		checkConsistency(blockBuffer.getProtectionLevel()!=ProtectionLevel.READ_ONLY, true);
	}
	// </editor-fold>

	// <editor-fold desc="Pointer Assertions">
	/**
	 * Checks that the ptr is in the valid address range.  It must be >=0 and
	 * not the metadata block pointer.
	 */
	// @NotThreadSafe
	private boolean isValidRange(long ptr) throws IOException {
		return ptr>=0 && ptr!=metaDataBlockId;
	}
	// </editor-fold>

	// <editor-fold desc="Pointer Management">
	/**
	 * Gets the head pointer or <code>TAIL_PTR</code> if the list is empty.
	 */
	// @NotThreadSafe
	private long getHead() {
		return _head;
	}

	/**
	 * Sets the head to the provided value.
	 */
	// @NotThreadSafe
	private void setHead(long head) throws IOException {
		if(PersistentCollections.ASSERT) assert head==END_PTR || isValidRange(head);
		blockBuffer.putLong(metaDataBlockId, HEAD_OFFSET, head);
		this._head = head;
	}

	// @NotThreadSafe
	private long getTail() {
		return _tail;
	}

	/**
	 * Sets the tail to the provided value.
	 */
	// @NotThreadSafe
	private void setTail(long tail) throws IOException {
		if(PersistentCollections.ASSERT) assert tail==END_PTR || isValidRange(tail);
		blockBuffer.putLong(metaDataBlockId, TAIL_OFFSET, tail);
		this._tail = tail;
	}

	/**
	 * Gets the next pointer for the entry at the provided location.
	 * The entry must have non-null next and prev.
	 */
	// @NotThreadSafe
	private long getNext(long ptr) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		return blockBuffer.getLong(ptr, NEXT_OFFSET);
	}

	/**
	 * Sets the next pointer for the entry at the provided location.
	 * The entry must have non-null next and prev.
	 */
	// @NotThreadSafe
	private void setNext(long ptr, long next) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		if(PersistentCollections.ASSERT) assert next==END_PTR || isValidRange(next);
		blockBuffer.putLong(ptr, NEXT_OFFSET, next);
	}

	/**
	 * Gets the prev pointer for the entry at the provided location.
	 * The entry must have non-null next and prev.
	 */
	// @NotThreadSafe
	private long getPrev(long ptr) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		return blockBuffer.getLong(ptr, PREV_OFFSET);
	}

	/**
	 * Sets the prev pointer for the entry at the provided location.
	 * The entry must have non-null next and prev.
	 */
	// @NotThreadSafe
	private void setPrev(long ptr, long prev) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		if(PersistentCollections.ASSERT) assert prev==END_PTR || isValidRange(prev);
		blockBuffer.putLong(ptr, PREV_OFFSET, prev);
	}

	/**
	 * Gets the size of the data for the entry at the provided location.
	 * This does not include the block header.
	 * The entry must have non-null next and prev.
	 */
	// @NotThreadSafe
	private long getDataSize(long ptr) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		return blockBuffer.getLong(ptr, DATA_SIZE_OFFSET);
	}

	/**
	 * Checks if the provided element is null.
	 * The entry must have non-null next and prev.
	 */
	// @NotThreadSafe
	private boolean isNull(long ptr) throws IOException {
		return getDataSize(ptr)==DATA_SIZE_NULL;
	}

	/**
	 * Gets the element for the entry at the provided location.
	 * The entry must have non-null next and prev.
	 */
	// @NotThreadSafe
	private E getElement(long ptr) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		long dataSize = getDataSize(ptr);
		if(dataSize==DATA_SIZE_NULL) return null;

		try (InputStream in = blockBuffer.getInputStream(ptr, DATA_OFFSET, dataSize)) {
			// Read the object
			return serializer.deserialize(in);
		}
	}
	// </editor-fold>

	// <editor-fold desc="Data Structure Management">
	/**
	 * Removes the entry at the provided location and restores it to an unallocated state.
	 * Operates in constant time.
	 * The entry must have non-null next and prev.
	 */
	// @NotThreadSafe
	private void remove(long ptr) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		if(PersistentCollections.ASSERT) assert _size>0;
		long prev = getPrev(ptr);
		long next = getNext(ptr);
		if(prev==END_PTR) {
			if(PersistentCollections.ASSERT) assert getHead()==ptr;
			setHead(next);
		} else {
			if(PersistentCollections.ASSERT) assert getNext(prev)==ptr;
			setNext(prev, next);
		}
		if(next==END_PTR) {
			if(PersistentCollections.ASSERT) assert getTail()==ptr;
			setTail(prev);
		} else {
			if(PersistentCollections.ASSERT) assert getPrev(next)==ptr;
			setPrev(next, prev);
		}
		// Barrier, to make sure always pointing to complete data
		blockBuffer.barrier(false);
		blockBuffer.deallocate(ptr);
		// Barrier to make sure removes one at a time.  This way the consistency
		// checker can assume only one possible unreferenced allocated block.  More
		// than that indicates a problem.  This helps avoid accidentally deallocating
		// a large part of the data during crash recovery of unrecoverably corrupted
		// data.
		blockBuffer.barrier(true);
		_size--;
	}

	/**
	 * Adds an entry.  Allocates, writes the header and data, barrier, link-in, barrier, _size++
	 * If the serializer is fixed size, will preallocate and serialize directly
	 * the block.  Otherwise, it serializes to a buffer, and then allocates the
	 * appropriate amount of space.
	 * Operates in constant time.
	 */
	// @NotThreadSafe
	private long addEntry(long next, long prev, E element) throws IOException {
		//System.err.println("DEBUG: addEntry: element="+element);
		if(PersistentCollections.ASSERT) assert next==END_PTR || isValidRange(next);
		if(PersistentCollections.ASSERT) assert prev==END_PTR || isValidRange(prev);
		if(_size==Long.MAX_VALUE) throw new IOException("List is full: _size==Long.MAX_VALUE");

		// Allocate and write new entry
		long newPtr;
		if(element==null) {
			newPtr = blockBuffer.allocate(DATA_OFFSET);
			IoUtils.longToBuffer(next, ioBuffer, NEXT_OFFSET);
			IoUtils.longToBuffer(prev, ioBuffer, PREV_OFFSET);
			IoUtils.longToBuffer(DATA_SIZE_NULL, ioBuffer, DATA_SIZE_OFFSET);
			blockBuffer.put(newPtr, 0, ioBuffer, 0, DATA_OFFSET);
		} else {
			long dataSize = serializer.getSerializedSize(element);
			newPtr = blockBuffer.allocate(DATA_OFFSET+dataSize);
			IoUtils.longToBuffer(next, ioBuffer, NEXT_OFFSET);
			IoUtils.longToBuffer(prev, ioBuffer, PREV_OFFSET);
			IoUtils.longToBuffer(dataSize, ioBuffer, DATA_SIZE_OFFSET);
			blockBuffer.put(newPtr, 0, ioBuffer, 0, DATA_OFFSET);
			try (OutputStream out = blockBuffer.getOutputStream(newPtr, DATA_OFFSET, dataSize)) {
				serializer.serialize(element, out);
			}
		}
		// Barrier, to make sure always pointing to complete data
		blockBuffer.barrier(false);
		// Update pointers
		if(prev==END_PTR) {
			if(PersistentCollections.ASSERT) assert getHead()==next;
			setHead(newPtr);
		} else {
			if(PersistentCollections.ASSERT) assert getNext(prev)==next;
			setNext(prev, newPtr);
		}
		if(next==END_PTR) {
			if(PersistentCollections.ASSERT) assert getTail()==prev;
			setTail(newPtr);
		} else {
			if(PersistentCollections.ASSERT) assert getPrev(next)==prev;
			setPrev(next, newPtr);
		}
		// Barrier, to make sure links are correct
		blockBuffer.barrier(true);
		// Increment size
		_size++;
		return newPtr;
	}

	/**
	 * Adds the first entry to the list.
	 */
	// @NotThreadSafe
	private void addFirstEntry(final E element) throws IOException {
		if(PersistentCollections.ASSERT) assert getHead()==END_PTR;
		if(PersistentCollections.ASSERT) assert getTail()==END_PTR;
		addEntry(END_PTR, END_PTR, element);
	}

	/**
	 * Adds the provided element before the element at the provided location.
	 */
	// @NotThreadSafe
	private void addBefore(final E element, final long ptr) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		addEntry(ptr, getPrev(ptr), element);
	}

	/**
	 * Adds the provided element after the element at the provided location.
	 */
	// @NotThreadSafe
	private void addAfter(final E element, final long ptr) throws IOException {
		if(PersistentCollections.ASSERT) assert isValidRange(ptr);
		addEntry(getNext(ptr), ptr, element);
	}
	// </editor-fold>

	// <editor-fold desc="Data Consistency Check">
	/**
	 * Performs a check that this linked list is in a consistent state, optionally
	 * correcting problems that may occur during an unclean shutdown.
	 * <ol>
	 *   <li>
	 *     Meta data block is present and complete:
	 *     <ol type="a">
	 *       <li>metaDataBlockId is the correct value</li>
	 *       <li>Magic value is correct</li>
	 *       <li>File version is supported</li>
	 *     </ol>
	 *   </li>
	 *   <li>_head is the correct value</li>
	 *   <li>_tail is the correct value</li>
	 *   <li>if _head==END_PTR then _tail==END_PTR</li>
	 *   <li>if _tail==END_PTR then _head==END_PTR</li>
	 *   <li>Makes sure all pointers point to allocated blocks</li>
	 *   <li>{@code _head == END_PTR || _head->prev == END_PTR}</li>
	 *   <li>{@code _tail == END_PTR || _tail->next == END_PTR}</li>
	 *   <li>For each node:
	 *     <ol type="a">
	 *       <li>Only seen once (detect loops)</li>
	 *       <li>{@code node.prev->next == node}</li>
	 *       <li>{@code node.next->prev == node}</li>
	 *     </ol>
	 *   </li>
	 *   <li>No unreferenced allocated blocks</li>
	 *   <li>_size matches the actual size</li>
	 * </ol>
	 *
	 * Assumptions used in this implementation:
	 * <ol>
	 *   <li>Blocks that are allocated but not referenced may contain incomplete data.</li>
	 *   <li>A block with any reference to it is both allocated and has complete data.</li>
	 *   <li>A block with any reference to it will be recovered or an exception will be thrown if unrecoverable.</li>
	 *   <li>Only one block may be allocated and not referenced due to barrier after each allocate or deallocate.</li>
	 * </ol>
	 *
	 * @param autoCorrect Will correct inconsistencies that arise from an unclean shutdown.
	 *                    Logs any corrections made to <code>logger</code> with level <code>INFO</code>.
	 *
	 * @exception  IOException if IO error occurs during check
	 * @exception  IllegalStateException when in an inconsistent state and, if autoCorrect, is uncorrectable
	 */
	// @NotThreadSafe
	protected void checkConsistency(boolean autoCorrect) throws IOException, IllegalStateException {
		checkConsistency(autoCorrect, false);
	}

	// @NotThreadSafe
	private void dumpPointer(long ptr) throws IOException {
		System.err.println("_head="+_head);
		if(_head!=END_PTR) System.err.println("  _head->next="+getNext(_head));
		if(_head!=END_PTR) System.err.println("  _head->prev="+getPrev(_head));
		System.err.println("ptr="+ptr);
		if(ptr!=END_PTR) {
			long next = getNext(ptr);
			System.err.println("  ptr.next="+next);
			if(next!=END_PTR) {
				System.err.println("    ptr->next.next="+getNext(next));
				System.err.println("    ptr->next.prev="+getPrev(next));
			}
			long prev = getPrev(ptr);
			System.err.println("  ptr.prev="+prev);
			if(prev!=END_PTR) {
				System.err.println("    ptr->prev.next="+getNext(prev));
				System.err.println("    ptr->prev.prev="+getPrev(prev));
			}
		}
		System.err.println("_tail="+_tail);
		if(_tail!=END_PTR) System.err.println("  _tail->next="+getNext(_tail));
		if(_tail!=END_PTR) System.err.println("  _tail->prev="+getPrev(_tail));
	}

	// @NotThreadSafe
	private void checkConsistency(boolean autoCorrect, boolean isInit) throws IOException, IllegalStateException {
		if(autoCorrect && blockBuffer.getProtectionLevel()==ProtectionLevel.READ_ONLY) throw new IllegalArgumentException("autoCorrect on read-only block buffer is not allowed");
		// Meta data block is present and complete
		Iterator<Long> ids = blockBuffer.iterateBlockIds();
		if(ids.hasNext()) {
			// metaDataBlockId is the correct value
			{
				long correctMetaDataBlockId = ids.next();
				if(metaDataBlockId!=correctMetaDataBlockId) {
					if(!isInit) {
						if(!autoCorrect) throw new IllegalStateException("metaDataBlockId!=correctMetaDataBlockId: "+metaDataBlockId+"!="+correctMetaDataBlockId);
						logger.info("metaDataBlockId!=correctMetaDataBlockId: "+metaDataBlockId+"!="+correctMetaDataBlockId+" - correcting");
					}
					metaDataBlockId = correctMetaDataBlockId;
				}
			}
			blockBuffer.get(metaDataBlockId, 0, ioBuffer, 0, MAGIC.length);
			// Magic value is correct
			if(!AoArrays.equals(ioBuffer, MAGIC, 0, MAGIC.length)) throw new IllegalStateException("File does not appear to be a PersistentLinkedList (MAGIC mismatch)");
			// File version is supported
			{
				int version = blockBuffer.getInt(metaDataBlockId, MAGIC.length);
				if(version!=VERSION) throw new IllegalStateException("Unsupported file version: "+version);
			}

			// Get the set of all allocated ids (except the meta data id).
			Map<Long,Boolean> allocatedIds = new HashMap<>();
			while(ids.hasNext()) allocatedIds.put(ids.next(), false);

			// _head is the correct value
			{
				long correctHead = blockBuffer.getLong(metaDataBlockId, HEAD_OFFSET);
				if(_head!=correctHead) {
					if(!isInit) {
						if(!autoCorrect) throw new IllegalStateException("_head!=correctHead: "+_head+"!="+correctHead);
						logger.info("_head!=correctMetaDataBlockId: "+_head+"!="+correctHead+" - correcting");
					}
					_head = correctHead;
				}
			}
			// Make sure head points to an allocated block.
			if(_head!=END_PTR && !allocatedIds.containsKey(_head)) throw new IllegalStateException("_head points to unallocated block: "+_head);
			// _tail is the correct value
			{
				long correctTail = blockBuffer.getLong(metaDataBlockId, TAIL_OFFSET);
				if(_tail!=correctTail) {
					if(!isInit) {
						if(!autoCorrect) throw new IllegalStateException("_tail!=correctTail: "+_tail+"!="+correctTail);
						logger.info("_tail!=correctMetaDataBlockId: "+_tail+"!="+correctTail+" - correcting");
					}
					_tail=correctTail;
				}
			}
			// Make sure tail points to an allocated block.
			if(_tail!=END_PTR && !allocatedIds.containsKey(_tail)) throw new IllegalStateException("_tail points to unallocated block: "+_tail);
			// if _head==END_PTR then _tail==END_PTR
			if(_head==END_PTR && _tail!=END_PTR) {
				if(!autoCorrect) throw new IllegalStateException("_head==END_PTR && _tail!=END_PTR: _tail="+_tail);
				// Partial delete or add, recover
				logger.info("_head==END_PTR && _tail!=END_PTR: _tail="+_tail+" - recovering partial add or remove");
				long prev = getPrev(_tail);
				if(prev!=END_PTR) throw new IllegalStateException("_tail->prev!=END_PTR: "+prev);
				long next = getNext(_tail);
				if(next!=END_PTR) throw new IllegalStateException("_tail->next!=END_PTR: "+next);
				setHead(_tail);
			}
			// if _tail==END_PTR then _head==END_PTR
			if(_tail==END_PTR && _head!=END_PTR) {
				if(!autoCorrect) throw new IllegalStateException("_tail==END_PTR && _head!=END_PTR: _head="+_head);
				// Partial delete or add, recover
				logger.info("_tail==END_PTR && _head!=END_PTR: _head="+_head+" - recovering partial add or remove");
				long prev = getPrev(_head);
				if(prev!=END_PTR) throw new IllegalStateException("_head->prev!=END_PTR: "+prev);
				long next = getNext(_head);
				if(next!=END_PTR) throw new IllegalStateException("_head->next!=END_PTR: "+next);
				setTail(_head);
			}
			if(_head!=END_PTR) {
				// _head->prev==END_PTR
				long prev = getPrev(_head);
				if(prev!=END_PTR) {
					if(!autoCorrect) throw new IllegalStateException("_head->prev!=END_PTR: _head="+_head+", _head->prev="+prev);
					if(!allocatedIds.containsKey(prev)) throw new IllegalStateException("_head->prev points to unallocated block: _head="+_head+", _head->prev="+prev);
					logger.info("_head->prev!=END_PTR: "+prev+" - recovering partial add or remove");
					// Recoverable if _head->prev.prev==END_PTR and _head->prev.next=_head
					long prevPrev = getPrev(prev);
					if(prevPrev!=END_PTR) throw new IllegalStateException("_head->prev!=END_PTR: _head="+_head+", _head->prev="+prev+" - unrecoverable because _head->prev.prev!=END_PTR: "+prevPrev);
					long prevNext = getNext(prev);
					if(prevNext!=_head) throw new IllegalStateException("_head->prev!=END_PTR: _head="+_head+", _head->prev="+prev+" - unrecoverable because _head->prev.next!=_head: "+prevNext);
					setHead(prev);
				}
			}
			if(_tail!=END_PTR) {
				// _tail->next==END_PTR
				long next = getNext(_tail);
				if(next!=END_PTR) {
					if(!autoCorrect) throw new IllegalStateException("_tail->next!=END_PTR: _tail="+_tail+", _tail->next="+next);
					if(!allocatedIds.containsKey(next)) throw new IllegalStateException("_tail->next points to unallocated block: _tail="+_tail+", _tail->next="+next);
					logger.info("_tail->next!=END_PTR: "+next+" - recovering partial add or remove");
					// Recoverable if _tail->next.next==END_PTR and _tail->next.prev=_tail
					long nextNext = getNext(next);
					if(nextNext!=END_PTR) throw new IllegalStateException("_tail->next!=END_PTR: _tail="+_tail+", _tail->next="+next+" - unrecoverable because _tail->next.next!=END_PTR: "+nextNext);
					long nextPrev = getPrev(next);
					if(nextPrev!=_tail) throw new IllegalStateException("_tail->next!=END_PTR: _tail="+_tail+", _tail->next="+next+" - unrecoverable because _tail->next.prev!=_tail: "+nextPrev);
					setTail(next);
				}
			}
			// For each node:
			long count = 0;
			long ptr = _head;
			while(ptr!=END_PTR) {
				// Points to allocated block
				Boolean seen = allocatedIds.get(ptr);
				if(seen==null) throw new IllegalStateException("ptr points to unallocated block: "+ptr);
				// Only seen once (detect loops)
				if(seen) throw new IllegalStateException("ptr seen more than once, loop in list: "+ptr);
				// Mark as seen
				allocatedIds.put(ptr, Boolean.TRUE);

				// Since checking from head to tail, make sure prev is correct before checking next
				long prev = getPrev(ptr);
				if(prev==END_PTR) {
					// head must point to this node
					if(_head!=ptr) {
						// TODO: Recovery?  Can this happen given our previous checks?
						dumpPointer(ptr);
						throw new IllegalStateException("ptr.prev==END_PTR while _head!=ptr: ptr="+ptr+", _head="+_head);
					}
				} else {
					// make sure ptr.prev is allocated
					if(!allocatedIds.containsKey(prev)) throw new IllegalStateException("ptr.prev points to unallocated block: ptr="+ptr+", ptr.prev="+prev);
					// node.prev->next==node
					long prevNext = getNext(prev);
					if(prevNext!=ptr) {
						// TODO: Recovery?  Can this happen given our previous checks?
						dumpPointer(ptr);
						throw new IllegalStateException("ptr.prev->next!=ptr: ptr="+ptr+", ptr.prev="+prev+", ptr.prev->next="+prevNext);
					}
				}

				long next = getNext(ptr);
				if(next==END_PTR) {
					// tail must point to this node
					if(_tail!=ptr) {
						if(!autoCorrect) throw new IllegalStateException("ptr.next==END_PTR while _tail!=ptr: ptr="+ptr+", _tail="+_tail);
						logger.info("ptr.next==END_PTR while _tail!=ptr: ptr="+ptr+", _tail="+_tail+" - recovering partial add or remove");
						if(_tail==END_PTR) throw new IllegalStateException("ptr.next==END_PTR while _tail!=ptr: ptr="+ptr+", _tail="+_tail+" - unrecoverable because _tail==END_PTR");
						long tailPrev = getPrev(_tail);
						if(tailPrev!=ptr) throw new IllegalStateException("ptr.next==END_PTR while _tail!=ptr: ptr="+ptr+", _tail="+_tail+" - unrecoverable because _tail->prev!=ptr: "+tailPrev);
						long tailNext = getNext(_tail);
						if(tailNext!=END_PTR) throw new IllegalStateException("ptr.next==END_PTR while _tail!=ptr: ptr="+ptr+", _tail="+_tail+" - unrecoverable because _tail->next!=END_PTR: "+tailNext);
						setNext(ptr, next=_tail);
					}
				} else {
					// make sure ptr.next is allocated
					if(!allocatedIds.containsKey(next)) throw new IllegalStateException("ptr.next points to unallocated block: ptr="+ptr+", ptr.next="+next);
					// node.next->prev==node
					long nextPrev = getPrev(next);
					if(nextPrev!=ptr) {
						if(!autoCorrect) throw new IllegalStateException("ptr.next->prev!=ptr: ptr="+ptr+", ptr.prev="+prev+", ptr.next="+next+", ptr.next->prev="+nextPrev);
						logger.info("ptr.next->prev!=ptr: ptr="+ptr+", ptr.prev="+prev+", ptr.next="+next+", ptr.next->prev="+nextPrev+" - recovering partial add or remove");
						if(nextPrev!=prev) throw new IllegalStateException("ptr.next->prev!=ptr: ptr="+ptr+", ptr.prev="+prev+", ptr.next="+next+", ptr.next->prev="+nextPrev+" - unrecoverable because ptr.next->prev!=ptr.prev");
						setPrev(next, ptr);
					}
				}

				ptr = next;
				count++;
			}
			// No unreferenced allocated blocks
			long firstUnreferencedBlockId = -1;
			long unreferencedCount = 0;
			for(Map.Entry<Long,Boolean> entry : allocatedIds.entrySet()) {
				if(!entry.getValue()) {
					if(firstUnreferencedBlockId==-1) firstUnreferencedBlockId = entry.getKey();
					unreferencedCount++;
				}
			}
			if(unreferencedCount>0) {
				// Should only need to deallocate one block - higher count may indicate a more serious problem
				if(unreferencedCount>1) throw new IllegalStateException("More than one block allocated but not referenced: firstUnreferencedBlockId="+firstUnreferencedBlockId+", unreferencedCount="+unreferencedCount);
				if(!autoCorrect) throw new IllegalStateException("Block allocated but not referenced: "+firstUnreferencedBlockId);
				logger.info("Block allocated but not referenced: "+firstUnreferencedBlockId+" - deallocating");
				blockBuffer.deallocate(firstUnreferencedBlockId);
			}
			// _size matches the actual size
			if(_size!=count) {
				if(!isInit) {
					if(!autoCorrect) throw new IllegalStateException("_size!=count: "+_size+"!="+count);
					logger.info("_size!=count: "+_size+"!="+count+" - correcting");
				}
				_size = count;
			}
		} else {
			if(!autoCorrect) throw new IllegalStateException("Block buffer is empty - no meta data block found.");
			if(!isInit) logger.info("Block buffer is empty - initializing meta data block.");
			metaDataBlockId = blockBuffer.allocate(HEADER_SIZE);
			blockBuffer.put(metaDataBlockId, 0, MAGIC, 0, MAGIC.length);
			blockBuffer.putInt(metaDataBlockId, MAGIC.length, VERSION);
			setHead(END_PTR);
			setTail(END_PTR);
			blockBuffer.barrier(true);
			_size = 0;
		}
	}
	// </editor-fold>

	// <editor-fold desc="Queue/Deque Implementation">
	/**
	 * Returns the first element in this list.
	 * Operates in constant time.
	 *
	 * @return the first element in this list
	 * @throws NoSuchElementException if this list is empty
	 */
	// @NotThreadSafe
	@Override
	public E getFirst() {
		long head=getHead();
		if(head==END_PTR) throw new NoSuchElementException();
		try {
			return getElement(head);
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	/**
	 * Returns the last element in this list.
	 * Operates in constant time.
	 *
	 * @return the last element in this list
	 * @throws NoSuchElementException if this list is empty
	 */
	// @NotThreadSafe
	@Override
	public E getLast()  {
		long tail=getTail();
		if(tail==END_PTR) throw new NoSuchElementException();
		try {
			return getElement(tail);
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	/**
	 * Removes and returns the first element from this list.
	 * Operates in constant time.
	 *
	 * @return the first element from this list
	 * @throws NoSuchElementException if this list is empty
	 */
	// @NotThreadSafe
	@Override
	public E removeFirst() {
		long head = getHead();
		if(head==END_PTR) throw new NoSuchElementException();
		try {
			modCount++;
			E element = getElement(head);
			remove(head);
			return element;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	/**
	 * Removes and returns the last element from this list.
	 * Operates in constant time.
	 *
	 * @return the last element from this list
	 * @throws NoSuchElementException if this list is empty
	 */
	// @NotThreadSafe
	@Override
	public E removeLast() {
		long tail = getTail();
		if(tail==END_PTR) throw new NoSuchElementException();
		try {
			modCount++;
			E element = getElement(tail);
			remove(tail);
			return element;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	/**
	 * Inserts the specified element at the beginning of this list.
	 * Operates in log time for free space.
	 *
	 * @param element the element to add
	 */
	// @NotThreadSafe
	@Override
	public void addFirst(E element) {
		try {
			modCount++;
			long head = getHead();
			if(head==END_PTR) addFirstEntry(element);
			else addBefore(element, head);
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	/**
	 * Appends the specified element to the end of this list.
	 * Operates in log time for free space.
	 *
	 * <p>This method is equivalent to {@link #add}.
	 *
	 * @param element the element to add
	 */
	// @NotThreadSafe
	@Override
	public void addLast(E element) {
		try {
			modCount++;
			long tail = getTail();
			if(tail==END_PTR) addFirstEntry(element);
			else addAfter(element, tail);
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public boolean contains(Object o) {
		return indexOf(o) != -1;
	}

	/**
	 * Gets the pointer for the provided index.
	 *
	 * This runs in linear time.
	 */
	// @NotThreadSafe
	private long getPointerForIndex(long index) throws IOException {
		if(PersistentCollections.ASSERT) assert _size>0;
		if(index<(_size >> 1)) {
			long ptr = getHead();
			if(PersistentCollections.ASSERT) assert ptr!=END_PTR;
			for(int i=0;i<index;i++) {
				ptr = getNext(ptr);
				if(PersistentCollections.ASSERT) assert ptr!=END_PTR;
			}
			return ptr;
		} else {
			// Search backwards
			long bptr = getTail();
			if(PersistentCollections.ASSERT) assert bptr!=END_PTR;
			for(long i=_size-1;i>index;i--) {
				bptr = getPrev(bptr);
				if(PersistentCollections.ASSERT) assert bptr!=END_PTR;
			}
			return bptr;
		}
		//if(ptr!=bptr) throw new AssertionError("ptr!=bptr: "+ptr+"!="+bptr);
		//return ptr;
	}

	// @NotThreadSafe
	@Override
	public boolean remove(Object o) {
		try {
			if(o==null) {
				for(long ptr = getHead(); ptr!=END_PTR; ptr = getNext(ptr)) {
					if(isNull(ptr)) {
						modCount++;
						remove(ptr);
						return true;
					}
				}
			} else {
				for(long ptr = getHead(); ptr!=END_PTR; ptr = getNext(ptr)) {
					if(o.equals(getElement(ptr))) {
						modCount++;
						remove(ptr);
						return true;
					}
				}
			}
			return false;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public boolean addAll(Collection<? extends E> c) {
		if(c.isEmpty()) return false;
		modCount++;
		for(E element : c) addLast(element);
		return true;
	}

	// @NotThreadSafe
	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		if(index==_size) return addAll(c);
		if(c.isEmpty()) return false;
		if (index < 0 || index > _size) throw new IndexOutOfBoundsException("Index: "+index+", Size: "+_size);
		modCount++;
		try {
			long ptr = getPointerForIndex(index);
			for(E element : c) addBefore(element, ptr);
			return true;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	/**
	 * Gets the number of elements in this list.
	 * Operates in constant time.
	 *
	 * @return the number of elements in this list
	 */
	// @NotThreadSafe
	@Override
	public int size() {
		return _size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)_size;
	}

	// @NotThreadSafe
	@Override
	public boolean add(E element) {
		addLast(element);
		return true;
	}
	// </editor-fold>

	/**
	 * Clears the list.
	 */
	// @NotThreadSafe
	@Override
	public void clear() {
		try {
			modCount++;
			Iterator<Long> ids = blockBuffer.iterateBlockIds();
			if(!ids.hasNext()) throw new AssertionError("Block buffer is empty - no meta data block found.");
			long firstId = ids.next();
			if(metaDataBlockId!=firstId) throw new AssertionError("metaDataBlockId!=firstId: "+metaDataBlockId+"!="+firstId);
			setHead(END_PTR);
			setTail(END_PTR);
			_size = 0;
			blockBuffer.barrier(false);
			// Deallocate all except first block
			while(ids.hasNext()) {
				ids.next();
				ids.remove();
			}
			blockBuffer.barrier(true);
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public E get(int index) {
		try {
			return getElement(getPointerForIndex(index));
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	/**
	 * Replaces the element at the specified position in this list with the
	 * specified element.
	 *
	 * TODO: First try to replace at the current position?  Impact on atomicity?
	 *
	 * @param index index of the element to replace
	 * @param element element to be stored at the specified position
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	// @NotThreadSafe
	private void setElement(long ptr, E element) {
		modCount++;
		try {
			long prev = getPrev(ptr);
			remove(ptr);
			if(prev==END_PTR) addFirst(element);
			else addAfter(element, prev);
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public E set(int index, E element) {
		try {
			long ptr = getPointerForIndex(index);
			E oldElement = getElement(ptr);
			setElement(ptr, element);
			return oldElement;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public void add(int index, E element) {
		modCount++;
		try {
			long ptr = getPointerForIndex(index);
			long prev = getPrev(ptr);
			if(prev==END_PTR) addFirst(element);
			else addAfter(element, prev);
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public E remove(int index) {
		modCount++;
		try {
			long ptr = getPointerForIndex(index);
			E oldElement = getElement(ptr);
			remove(ptr);
			return oldElement;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public int indexOf(Object o) {
		try {
			int index = 0;
			if(o==null) {
				for(long ptr = getHead(); ptr!=END_PTR; ptr = getNext(ptr)) {
					if(isNull(ptr)) return index;
					index++;
				}
			} else {
				for(long ptr = getHead(); ptr!=END_PTR; ptr = getNext(ptr)) {
					if(o.equals(getElement(ptr))) return index;
					index++;
				}
			}
			return -1;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public int lastIndexOf(Object o) {
		try {
			long index = _size;
			if(o==null) {
				for(long ptr = getTail(); ptr!=END_PTR; ptr = getPrev(ptr)) {
					--index;
					if(isNull(ptr)) {
						if(index>Integer.MAX_VALUE) throw new RuntimeException("Index too high to return from lastIndexOf: "+index);
						return (int)index;
					}
				}
			} else {
				for(long ptr = getTail(); ptr!=END_PTR; ptr = getPrev(ptr)) {
					--index;
					if(o.equals(getElement(ptr))) {
						if(index>Integer.MAX_VALUE) throw new RuntimeException("Index too high to return from lastIndexOf: "+index);
						return (int)index;
					}
				}
			}
			return -1;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public E peek() {
		if(_size==0) return null;
		return getFirst();
	}

	// @NotThreadSafe
	@Override
	public E element() {
		return getFirst();
	}

	// @NotThreadSafe
	@Override
	public E poll() {
		if(_size==0) return null;
		return removeFirst();
	}

	// @NotThreadSafe
	@Override
	public E remove() {
		return removeFirst();
	}

	// @NotThreadSafe
	@Override
	public boolean offer(E e) {
		return add(e);
	}

	// @NotThreadSafe
	@Override
	public boolean offerFirst(E e) {
		addFirst(e);
		return true;
	}

	// @NotThreadSafe
	@Override
	public boolean offerLast(E e) {
		addLast(e);
		return true;
	}

	// @NotThreadSafe
	@Override
	public E peekFirst() {
		if(_size==0)
			return null;
		return getFirst();
	}

	// @NotThreadSafe
	@Override
	public E peekLast() {
		if(_size==0)
			return null;
		return getLast();
	}

	// @NotThreadSafe
	@Override
	public E pollFirst() {
		if(_size==0)
			return null;
		return removeFirst();
	}

	// @NotThreadSafe
	@Override
	public E pollLast() {
		if (_size==0)
			return null;
		return removeLast();
	}

	// @NotThreadSafe
	@Override
	public void push(E e) {
		addFirst(e);
	}

	// @NotThreadSafe
	@Override
	public E pop() {
		return removeFirst();
	}

	// @NotThreadSafe
	@Override
	public boolean removeFirstOccurrence(Object o) {
		return remove(o);
	}

	// @NotThreadSafe
	@Override
	public boolean removeLastOccurrence(Object o) {
		try {
			if(o==null) {
				for(long ptr = getTail(); ptr!=END_PTR; ptr = getPrev(ptr)) {
					if(isNull(ptr)) {
						modCount++;
						remove(ptr);
						return true;
					}
				}
			} else {
				for(long ptr = getTail(); ptr!=END_PTR; ptr = getPrev(ptr)) {
					if(o.equals(getElement(ptr))) {
						modCount++;
						remove(ptr);
						return true;
					}
				}
			}
			return false;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	public ListIterator<E> listIterator(int index) {
		return new ListItr(index);
	}

	private class ListItr implements ListIterator<E> {
		private long lastReturned = END_PTR;
		private long nextPtr;
		private long nextIndex;
		private int expectedModCount = modCount;

		ListItr(long index) {
			if (index < 0 || index > _size) throw new IndexOutOfBoundsException("Index: "+index+", Size: "+_size);
			try {
				if(index==_size) {
					// Points one past the end
					nextPtr = END_PTR;
					nextIndex = _size;
				} else {
					// Points within the list
					nextPtr = getPointerForIndex(index);
					nextIndex = index;
				}
			} catch(IOException err) {
				throw new WrappedException(err);
			}
		}

		// @NotThreadSafe
		@Override
		public boolean hasNext() {
			return nextIndex != _size;
		}

		// @NotThreadSafe
		@Override
		public E next() {
			checkForComodification();
			if (nextIndex == _size) throw new NoSuchElementException();
			try {
				lastReturned = nextPtr;
				nextPtr = getNext(nextPtr);
				nextIndex++;
				assert (nextPtr==END_PTR && nextIndex==_size) || (nextPtr!=END_PTR && nextIndex<_size);
				return getElement(lastReturned);
			} catch(IOException err) {
				throw new WrappedException(err);
			}
		}

		// @NotThreadSafe
		@Override
		public boolean hasPrevious() {
			return nextIndex != 0;
		}

		// @NotThreadSafe
		@Override
		public E previous() {
			checkForComodification();
			if (nextIndex == 0) throw new NoSuchElementException();
			try {
				lastReturned = nextPtr = nextPtr==END_PTR ? getTail() : getPrev(nextPtr);
				nextIndex--;
				assert (nextPtr==END_PTR && nextIndex==_size) || (nextPtr!=END_PTR && nextIndex<_size);
				return getElement(lastReturned);
			} catch(IOException err) {
				throw new WrappedException(err);
			}
		}

		// @NotThreadSafe
		@Override
		public int nextIndex() {
			if(nextIndex>Integer.MAX_VALUE) throw new RuntimeException("Index too high to return from nextIndex: "+nextIndex);
			assert nextIndex>=0;
			return (int)nextIndex;
		}

		// @NotThreadSafe
		@Override
		public int previousIndex() {
			long prevIndex = nextIndex-1;
			if(prevIndex>Integer.MAX_VALUE) throw new RuntimeException("Index too high to return from previousIndex: "+prevIndex);
			assert prevIndex>=0;
			return (int)prevIndex;
		}

		// @NotThreadSafe
		@Override
		public void remove() {
			checkForComodification();
			try {
				long lastNext = getNext(lastReturned);
				try {
					PersistentLinkedList.this.remove(lastReturned);
				} catch (NoSuchElementException e) {
					throw new IllegalStateException();
				}
				if(nextPtr==lastReturned) nextPtr = lastNext;
				else nextIndex--;
				lastReturned = END_PTR;
				expectedModCount++;
				assert (nextPtr==END_PTR && nextIndex==_size) || (nextPtr!=END_PTR && nextIndex<_size);
			} catch(IOException err) {
				throw new WrappedException(err);
			}
		}

		// @NotThreadSafe
		@Override
		public void set(E e) {
			if (lastReturned == END_PTR) throw new IllegalStateException();
			checkForComodification();
			setElement(lastReturned, e);
			expectedModCount++;
		}

		// @NotThreadSafe
		@Override
		public void add(E e) {
			checkForComodification();
			try {
				lastReturned = END_PTR;
				modCount++;
				addBefore(e, nextPtr);
				nextIndex++;
				expectedModCount++;
			} catch(IOException err) {
				throw new WrappedException(err);
			}
		}

		// @NotThreadSafe
		final void checkForComodification() {
			if (modCount != expectedModCount)
			throw new ConcurrentModificationException();
		}
	}

	// @NotThreadSafe
	@Override
	public Iterator<E> descendingIterator() {
		return new DescendingIterator();
	}

	/** Adapter to provide descending iterators via ListItr.previous */
	private class DescendingIterator implements Iterator<E> {
		final ListItr itr = new ListItr(size());
		// @NotThreadSafe
		@Override
		public boolean hasNext() {
			return itr.hasPrevious();
		}
		// @NotThreadSafe
		@Override
		public E next() {
				return itr.previous();
			}
		// @NotThreadSafe
		@Override
		public void remove() {
			itr.remove();
		}
	}

	// @NotThreadSafe
	@Override
	public Object[] toArray() {
		try {
			if(_size>Integer.MAX_VALUE) throw new RuntimeException("Too many elements in list to create Object[]: "+_size);
			Object[] result = new Object[(int)_size];
			int i = 0;
			for (long ptr = getHead(); ptr != END_PTR; ptr = getNext(ptr)) result[i++] = getElement(ptr);
			return result;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	// @NotThreadSafe
	@Override
	@SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a) {
		if(_size>Integer.MAX_VALUE) throw new RuntimeException("Too many elements in list to fill or create array: "+_size);
		try {
			if (a.length < _size)
				a = (T[])java.lang.reflect.Array.newInstance(
									a.getClass().getComponentType(), (int)_size);
			int i = 0;
			Object[] result = a;
			for (long ptr = getHead(); ptr != END_PTR; ptr = getNext(ptr)) result[i++] = getElement(ptr);
			if (a.length > _size)
			a[(int)_size] = null;

			return a;
		} catch(IOException err) {
			throw new WrappedException(err);
		}
	}

	/**
     * @deprecated The finalization mechanism is inherently problematic.
	 */
    @Deprecated // Java 9: (since="9")
	// @NotThreadSafe
	@Override
	protected void finalize() throws Throwable {
		try {
			close();
		} finally {
			super.finalize();
		}
	}

	/**
	 * Closes the random access file backing this list.
	 */
	// @NotThreadSafe
	public void close() throws IOException {
		if(!blockBuffer.isClosed()) {
			// if(PersistentCollections.ASSERT) assert isConsistent();
			blockBuffer.close();
		}
	}
}
