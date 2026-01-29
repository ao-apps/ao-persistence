/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2013, 2016, 2017, 2019, 2020, 2021, 2022, 2024, 2025  AO Industries, Inc.
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
import com.aoapps.lang.io.IoUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
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
import java.util.RandomAccess;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Serializes and stores objects in a persistent buffer.  Unlike {@link com.aoapps.hodgepodge.io.FileList} which
 * is intended for efficient {@link RandomAccess},
 * this is a linked list implementation and has the expected benefits and costs.
 * There are no size limits to the stored data.
 *
 * <p>This class is not thread-safe.  It is absolutely critical that external
 * synchronization be applied.</p>
 *
 * <p>The objects are serialized using the standard Java serialization, unless a
 * {@link Serializer} is provided.  If an object that is not {@link Serializable}
 * is to be stored, a {@link Serializer} must be provided.  {@link Serializer Serializers}
 * may also provide a more efficient or more compact representation of an object.</p>
 *
 * <p>This class is intended for scalability and persistence, not for intra-process
 * or intra-thread shared data.</p>
 *
 * <p>The first block allocated is a header:</p>
 *
 * <pre>    Offset   Type  Description
 *      0- 3    ASCII "PLL\n"
 *      4- 7    int   version
 *      8-15    long  block id of the head or {@code END_PTR} if empty.
 *     16-23    long  block id of the tail or {@code END_PTR} if empty.</pre>
 *
 * <p>Each entry consists of:</p>
 *
 * <pre>    Offset   Name        Type     Description
 *       0- 7   next        long     block id of next, {@code END_PTR} for last element
 *       8-15   prev        long     block id of prev, {@code END_PTR} for first element
 *      16-23   dataSize    long     the size of the serialized data, {@code -1} means null element
 *      24+     data        data     the binary data</pre>
 *
 * <pre>TODO: Add corrupt flag, set on exceptions?  Cause immediate crash recovery?
 * TODO: Similar thing for the underlying block buffers and byte buffers?</pre>
 *
 * @author  AO Industries, Inc.
 */
public class PersistentLinkedList<E> extends AbstractSequentialList<E> implements List<E>, Deque<E>, Closeable {

  private static final Logger logger = Logger.getLogger(PersistentLinkedList.class.getName());

  private static final byte[] MAGIC = {'P', 'L', 'L', '\n'};

  private static final int VERSION = 3;

  private static final int VERSION_BYTES = Integer.BYTES;
  private static final int HEAD_BYTES = Long.BYTES;
  private static final int TAIL_BYTES = Long.BYTES;
  private static final int NEXT_BYTES = Long.BYTES;
  private static final int PREV_BYTES = Long.BYTES;
  private static final int DATA_SIZE_OFFSET_BYTES = Long.BYTES;

  /**
   * The value used to represent an ending pointer.
   */
  private static final long END_PTR = -2;

  /**
   * The constant location of the head pointer.
   */
  private static final long HEAD_OFFSET = (long) MAGIC.length + VERSION_BYTES;

  /**
   * The constant location of the tail pointer.
   */
  private static final long TAIL_OFFSET = HEAD_OFFSET + HEAD_BYTES;

  /**
   * The total number of bytes in the header.
   */
  private static final int HEADER_SIZE = (int) (TAIL_OFFSET + TAIL_BYTES);

  static {
    assert (TAIL_OFFSET + TAIL_BYTES) <= Integer.MAX_VALUE;
  }

  /**
   * The block offset for {@code next}.
   */
  private static final int NEXT_OFFSET = 0;

  /**
   * The block offset for {@code prev}.
   */
  private static final int PREV_OFFSET = NEXT_OFFSET + NEXT_BYTES;

  /**
   * The block offset for {@code dataSize}.
   */
  private static final int DATA_SIZE_OFFSET = PREV_OFFSET + PREV_BYTES;

  /**
   * The block offset for the beginning of the data.
   */
  private static final int DATA_OFFSET = DATA_SIZE_OFFSET + DATA_SIZE_OFFSET_BYTES;

  /**
   * Value used to indicate {@code null} data.
   */
  private static final long DATA_SIZE_NULL = -1;

  private final Serializer<E> serializer;
  private final PersistentBlockBuffer blockBuffer;

  private final byte[] ioBuffer = new byte[Math.max(DATA_OFFSET, MAGIC.length)];

  // Cached for higher performance
  private long metaDataBlockId;
  private long cachedHead;
  private long cachedTail;
  private long cachedSize;

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
  @SuppressWarnings("OverridableMethodCallInConstructor")
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
    checkConsistency(blockBuffer.getProtectionLevel() != ProtectionLevel.READ_ONLY, true);
  }

  // </editor-fold>

  // <editor-fold desc="Pointer Assertions">
  /**
   * Checks that the ptr is in the valid address range.  It must be >= 0 and
   * not the metadata block pointer.
   */
  private boolean isValidRange(long ptr) {
    return ptr >= 0 && ptr != metaDataBlockId;
  }

  // </editor-fold>

  // <editor-fold desc="Pointer Management">
  /**
   * Gets the head pointer or {@code TAIL_PTR} if the list is empty.
   */
  private long getHead() {
    return cachedHead;
  }

  /**
   * Sets the head to the provided value.
   */
  private void setHead(long head) throws IOException {
    assert head == END_PTR || isValidRange(head);
    blockBuffer.putLong(metaDataBlockId, HEAD_OFFSET, head);
    this.cachedHead = head;
  }

  private long getTail() {
    return cachedTail;
  }

  /**
   * Sets the tail to the provided value.
   */
  private void setTail(long tail) throws IOException {
    assert tail == END_PTR || isValidRange(tail);
    blockBuffer.putLong(metaDataBlockId, TAIL_OFFSET, tail);
    this.cachedTail = tail;
  }

  /**
   * Gets the next pointer for the entry at the provided location.
   * The entry must have non-null next and prev.
   */
  private long getNext(long ptr) throws IOException {
    assert isValidRange(ptr);
    return blockBuffer.getLong(ptr, NEXT_OFFSET);
  }

  /**
   * Sets the next pointer for the entry at the provided location.
   * The entry must have non-null next and prev.
   */
  private void setNext(long ptr, long next) throws IOException {
    assert isValidRange(ptr);
    assert next == END_PTR || isValidRange(next);
    blockBuffer.putLong(ptr, NEXT_OFFSET, next);
  }

  /**
   * Gets the prev pointer for the entry at the provided location.
   * The entry must have non-null next and prev.
   */
  private long getPrev(long ptr) throws IOException {
    assert isValidRange(ptr);
    return blockBuffer.getLong(ptr, PREV_OFFSET);
  }

  /**
   * Sets the prev pointer for the entry at the provided location.
   * The entry must have non-null next and prev.
   */
  private void setPrev(long ptr, long prev) throws IOException {
    assert isValidRange(ptr);
    assert prev == END_PTR || isValidRange(prev);
    blockBuffer.putLong(ptr, PREV_OFFSET, prev);
  }

  /**
   * Gets the size of the data for the entry at the provided location.
   * This does not include the block header.
   * The entry must have non-null next and prev.
   */
  private long getDataSize(long ptr) throws IOException {
    assert isValidRange(ptr);
    return blockBuffer.getLong(ptr, DATA_SIZE_OFFSET);
  }

  /**
   * Checks if the provided element is null.
   * The entry must have non-null next and prev.
   */
  private boolean isNull(long ptr) throws IOException {
    return getDataSize(ptr) == DATA_SIZE_NULL;
  }

  /**
   * Gets the element for the entry at the provided location.
   * The entry must have non-null next and prev.
   */
  private E getElement(long ptr) throws IOException {
    assert isValidRange(ptr);
    long dataSize = getDataSize(ptr);
    if (dataSize == DATA_SIZE_NULL) {
      return null;
    }

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
  private void remove(long ptr) throws IOException {
    assert isValidRange(ptr);
    assert cachedSize > 0;
    long prev = getPrev(ptr);
    long next = getNext(ptr);
    if (prev == END_PTR) {
      assert getHead() == ptr;
      setHead(next);
    } else {
      assert getNext(prev) == ptr;
      setNext(prev, next);
    }
    if (next == END_PTR) {
      assert getTail() == ptr;
      setTail(prev);
    } else {
      assert getPrev(next) == ptr;
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
    cachedSize--;
  }

  /**
   * Adds an entry.  Allocates, writes the header and data, barrier, link-in, barrier, cachedSize++
   * If the serializer is fixed size, will preallocate and serialize directly
   * the block.  Otherwise, it serializes to a buffer, and then allocates the
   * appropriate amount of space.
   * Operates in constant time.
   */
  private long addEntry(long next, long prev, E element) throws IOException {
    // System.err.println("DEBUG: addEntry: element="+element);
    assert next == END_PTR || isValidRange(next);
    assert prev == END_PTR || isValidRange(prev);
    if (cachedSize == Long.MAX_VALUE) {
      throw new IOException("List is full: cachedSize == Long.MAX_VALUE");
    }

    // Allocate and write new entry
    long newPtr;
    if (element == null) {
      newPtr = blockBuffer.allocate(DATA_OFFSET);
      IoUtils.longToBuffer(next, ioBuffer, NEXT_OFFSET);
      IoUtils.longToBuffer(prev, ioBuffer, PREV_OFFSET);
      IoUtils.longToBuffer(DATA_SIZE_NULL, ioBuffer, DATA_SIZE_OFFSET);
      blockBuffer.put(newPtr, 0, ioBuffer, 0, DATA_OFFSET);
    } else {
      long dataSize = serializer.getSerializedSize(element);
      newPtr = blockBuffer.allocate(DATA_OFFSET + dataSize);
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
    if (prev == END_PTR) {
      assert getHead() == next;
      setHead(newPtr);
    } else {
      assert getNext(prev) == next;
      setNext(prev, newPtr);
    }
    if (next == END_PTR) {
      assert getTail() == prev;
      setTail(newPtr);
    } else {
      assert getPrev(next) == prev;
      setPrev(next, newPtr);
    }
    // Barrier, to make sure links are correct
    blockBuffer.barrier(true);
    // Increment size
    cachedSize++;
    return newPtr;
  }

  /**
   * Adds the first entry to the list.
   */
  private void addFirstEntry(final E element) throws IOException {
    assert getHead() == END_PTR;
    assert getTail() == END_PTR;
    addEntry(END_PTR, END_PTR, element);
  }

  /**
   * Adds the provided element before the element at the provided location.
   */
  private void addBefore(final E element, final long ptr) throws IOException {
    assert isValidRange(ptr);
    addEntry(ptr, getPrev(ptr), element);
  }

  /**
   * Adds the provided element after the element at the provided location.
   */
  private void addAfter(final E element, final long ptr) throws IOException {
    assert isValidRange(ptr);
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
   *   <li>cachedHead is the correct value</li>
   *   <li>cachedTail is the correct value</li>
   *   <li>if cachedHead == END_PTR then cachedTail == END_PTR</li>
   *   <li>if cachedTail == END_PTR then cachedHead == END_PTR</li>
   *   <li>Makes sure all pointers point to allocated blocks</li>
   *   <li>{@code cachedHead == END_PTR || cachedHead->prev == END_PTR}</li>
   *   <li>{@code cachedTail == END_PTR || cachedTail->next == END_PTR}</li>
   *   <li>For each node:
   *     <ol type="a">
   *       <li>Only seen once (detect loops)</li>
   *       <li>{@code node.prev->next == node}</li>
   *       <li>{@code node.next->prev == node}</li>
   *     </ol>
   *   </li>
   *   <li>No unreferenced allocated blocks</li>
   *   <li>cachedSize matches the actual size</li>
   * </ol>
   *
   * <p>Assumptions used in this implementation:</p>
   *
   * <ol>
   *   <li>Blocks that are allocated but not referenced may contain incomplete data.</li>
   *   <li>A block with any reference to it is both allocated and has complete data.</li>
   *   <li>A block with any reference to it will be recovered or an exception will be thrown if unrecoverable.</li>
   *   <li>Only one block may be allocated and not referenced due to barrier after each allocate or deallocate.</li>
   * </ol>
   *
   * @param autoCorrect Will correct inconsistencies that arise from an unclean shutdown.
   *                    Logs any corrections made to {@link PersistentLinkedList#logger} with level {@link Level#INFO}.
   *
   * @exception  IOException if IO error occurs during check
   * @exception  IllegalStateException when in an inconsistent state and, if autoCorrect, is uncorrectable
   */
  protected void checkConsistency(boolean autoCorrect) throws IOException, IllegalStateException {
    checkConsistency(autoCorrect, false);
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  private void dumpPointer(long ptr) throws IOException {
    System.err.println("cachedHead=" + cachedHead);
    if (cachedHead != END_PTR) {
      System.err.println("  cachedHead->next=" + getNext(cachedHead));
    }
    if (cachedHead != END_PTR) {
      System.err.println("  cachedHead->prev=" + getPrev(cachedHead));
    }
    System.err.println("ptr=" + ptr);
    if (ptr != END_PTR) {
      long next = getNext(ptr);
      System.err.println("  ptr.next=" + next);
      if (next != END_PTR) {
        System.err.println("    ptr->next.next=" + getNext(next));
        System.err.println("    ptr->next.prev=" + getPrev(next));
      }
      long prev = getPrev(ptr);
      System.err.println("  ptr.prev=" + prev);
      if (prev != END_PTR) {
        System.err.println("    ptr->prev.next=" + getNext(prev));
        System.err.println("    ptr->prev.prev=" + getPrev(prev));
      }
    }
    System.err.println("cachedTail=" + cachedTail);
    if (cachedTail != END_PTR) {
      System.err.println("  cachedTail->next=" + getNext(cachedTail));
    }
    if (cachedTail != END_PTR) {
      System.err.println("  cachedTail->prev=" + getPrev(cachedTail));
    }
  }

  private void checkConsistency(boolean autoCorrect, boolean isInit) throws IOException, IllegalStateException {
    if (autoCorrect && blockBuffer.getProtectionLevel() == ProtectionLevel.READ_ONLY) {
      throw new IllegalArgumentException("autoCorrect on read-only block buffer is not allowed");
    }
    // Meta data block is present and complete
    Iterator<Long> ids = blockBuffer.iterateBlockIds();
    if (ids.hasNext()) {
      // metaDataBlockId is the correct value
      {
        long correctMetaDataBlockId = ids.next();
        if (metaDataBlockId != correctMetaDataBlockId) {
          if (!isInit) {
            if (!autoCorrect) {
              throw new IllegalStateException("metaDataBlockId != correctMetaDataBlockId: " + metaDataBlockId + " != " + correctMetaDataBlockId);
            }
            logger.info("metaDataBlockId != correctMetaDataBlockId: " + metaDataBlockId + " != " + correctMetaDataBlockId + " - correcting");
          }
          metaDataBlockId = correctMetaDataBlockId;
        }
      }
      blockBuffer.get(metaDataBlockId, 0, ioBuffer, 0, MAGIC.length);
      // Magic value is correct
      if (!AoArrays.equals(ioBuffer, MAGIC, 0, MAGIC.length)) {
        throw new IllegalStateException("File does not appear to be a PersistentLinkedList (MAGIC mismatch)");
      }
      // File version is supported
      {
        int version = blockBuffer.getInt(metaDataBlockId, MAGIC.length);
        if (version != VERSION) {
          throw new IllegalStateException("Unsupported file version: " + version);
        }
      }

      // Get the set of all allocated ids (except the meta data id).
      Map<Long, Boolean> allocatedIds = new HashMap<>();
      while (ids.hasNext()) {
        allocatedIds.put(ids.next(), false);
      }

      // cachedHead is the correct value
      {
        long correctHead = blockBuffer.getLong(metaDataBlockId, HEAD_OFFSET);
        if (cachedHead != correctHead) {
          if (!isInit) {
            if (!autoCorrect) {
              throw new IllegalStateException("cachedHead != correctHead: " + cachedHead + " != " + correctHead);
            }
            logger.info("cachedHead != correctMetaDataBlockId: " + cachedHead + " != " + correctHead + " - correcting");
          }
          cachedHead = correctHead;
        }
      }
      // Make sure head points to an allocated block.
      if (cachedHead != END_PTR && !allocatedIds.containsKey(cachedHead)) {
        throw new IllegalStateException("cachedHead points to unallocated block: " + cachedHead);
      }
      // cachedTail is the correct value
      {
        long correctTail = blockBuffer.getLong(metaDataBlockId, TAIL_OFFSET);
        if (cachedTail != correctTail) {
          if (!isInit) {
            if (!autoCorrect) {
              throw new IllegalStateException("cachedTail != correctTail: " + cachedTail + " != " + correctTail);
            }
            logger.info("cachedTail != correctMetaDataBlockId: " + cachedTail + " != " + correctTail + " - correcting");
          }
          cachedTail = correctTail;
        }
      }
      // Make sure tail points to an allocated block.
      if (cachedTail != END_PTR && !allocatedIds.containsKey(cachedTail)) {
        throw new IllegalStateException("cachedTail points to unallocated block: " + cachedTail);
      }
      // if cachedHead == END_PTR then cachedTail == END_PTR
      if (cachedHead == END_PTR && cachedTail != END_PTR) {
        if (!autoCorrect) {
          throw new IllegalStateException("cachedHead == END_PTR && cachedTail != END_PTR: cachedTail=" + cachedTail);
        }
        // Partial delete or add, recover
        logger.info("cachedHead == END_PTR && cachedTail != END_PTR: cachedTail=" + cachedTail + " - recovering partial add or remove");
        long prev = getPrev(cachedTail);
        if (prev != END_PTR) {
          throw new IllegalStateException("cachedTail->prev != END_PTR: " + prev);
        }
        long next = getNext(cachedTail);
        if (next != END_PTR) {
          throw new IllegalStateException("cachedTail->next != END_PTR: " + next);
        }
        setHead(cachedTail);
      }
      // if cachedTail == END_PTR then cachedHead == END_PTR
      if (cachedTail == END_PTR && cachedHead != END_PTR) {
        if (!autoCorrect) {
          throw new IllegalStateException("cachedTail == END_PTR && cachedHead != END_PTR: cachedHead=" + cachedHead);
        }
        // Partial delete or add, recover
        logger.info("cachedTail == END_PTR && cachedHead != END_PTR: cachedHead=" + cachedHead + " - recovering partial add or remove");
        long prev = getPrev(cachedHead);
        if (prev != END_PTR) {
          throw new IllegalStateException("cachedHead->prev != END_PTR: " + prev);
        }
        long next = getNext(cachedHead);
        if (next != END_PTR) {
          throw new IllegalStateException("cachedHead->next != END_PTR: " + next);
        }
        setTail(cachedHead);
      }
      if (cachedHead != END_PTR) {
        // cachedHead->prev == END_PTR
        long prev = getPrev(cachedHead);
        if (prev != END_PTR) {
          if (!autoCorrect) {
            throw new IllegalStateException("cachedHead->prev != END_PTR: cachedHead=" + cachedHead + ", cachedHead->prev=" + prev);
          }
          if (!allocatedIds.containsKey(prev)) {
            throw new IllegalStateException("cachedHead->prev points to unallocated block: cachedHead=" + cachedHead + ", cachedHead->prev=" + prev);
          }
          logger.info("cachedHead->prev != END_PTR: " + prev + " - recovering partial add or remove");
          // Recoverable if cachedHead->prev.prev == END_PTR and cachedHead->prev.next=cachedHead
          long prevPrev = getPrev(prev);
          if (prevPrev != END_PTR) {
            throw new IllegalStateException("cachedHead->prev != END_PTR: cachedHead=" + cachedHead + ", cachedHead->prev=" + prev
                + " - unrecoverable because cachedHead->prev.prev != END_PTR: " + prevPrev);
          }
          long prevNext = getNext(prev);
          if (prevNext != cachedHead) {
            throw new IllegalStateException("cachedHead->prev != END_PTR: cachedHead=" + cachedHead + ", cachedHead->prev=" + prev
                + " - unrecoverable because cachedHead->prev.next != cachedHead: " + prevNext);
          }
          setHead(prev);
        }
      }
      if (cachedTail != END_PTR) {
        // cachedTail->next == END_PTR
        long next = getNext(cachedTail);
        if (next != END_PTR) {
          if (!autoCorrect) {
            throw new IllegalStateException("cachedTail->next != END_PTR: cachedTail=" + cachedTail + ", cachedTail->next=" + next);
          }
          if (!allocatedIds.containsKey(next)) {
            throw new IllegalStateException("cachedTail->next points to unallocated block: cachedTail=" + cachedTail + ", cachedTail->next=" + next);
          }
          logger.info("cachedTail->next != END_PTR: " + next + " - recovering partial add or remove");
          // Recoverable if cachedTail->next.next == END_PTR and cachedTail->next.prev=cachedTail
          long nextNext = getNext(next);
          if (nextNext != END_PTR) {
            throw new IllegalStateException("cachedTail->next != END_PTR: cachedTail=" + cachedTail + ", cachedTail->next=" + next
                + " - unrecoverable because cachedTail->next.next != END_PTR: " + nextNext);
          }
          long nextPrev = getPrev(next);
          if (nextPrev != cachedTail) {
            throw new IllegalStateException("cachedTail->next != END_PTR: cachedTail=" + cachedTail + ", cachedTail->next=" + next
                + " - unrecoverable because cachedTail->next.prev != cachedTail: " + nextPrev);
          }
          setTail(next);
        }
      }
      // For each node:
      long count = 0;
      long ptr = cachedHead;
      while (ptr != END_PTR) {
        // Points to allocated block
        Boolean seen = allocatedIds.get(ptr);
        if (seen == null) {
          throw new IllegalStateException("ptr points to unallocated block: " + ptr);
        }
        // Only seen once (detect loops)
        if (seen) {
          throw new IllegalStateException("ptr seen more than once, loop in list: " + ptr);
        }
        // Mark as seen
        allocatedIds.put(ptr, Boolean.TRUE);

        // Since checking from head to tail, make sure prev is correct before checking next
        long prev = getPrev(ptr);
        if (prev == END_PTR) {
          // head must point to this node
          if (cachedHead != ptr) {
            // TODO: Recovery?  Can this happen given our previous checks?
            dumpPointer(ptr);
            throw new IllegalStateException("ptr.prev == END_PTR while cachedHead != ptr: ptr=" + ptr + ", cachedHead=" + cachedHead);
          }
        } else {
          // make sure ptr.prev is allocated
          if (!allocatedIds.containsKey(prev)) {
            throw new IllegalStateException("ptr.prev points to unallocated block: ptr=" + ptr + ", ptr.prev=" + prev);
          }
          // node.prev->next == node
          long prevNext = getNext(prev);
          if (prevNext != ptr) {
            // TODO: Recovery?  Can this happen given our previous checks?
            dumpPointer(ptr);
            throw new IllegalStateException("ptr.prev->next != ptr: ptr=" + ptr + ", ptr.prev=" + prev + ", ptr.prev->next=" + prevNext);
          }
        }

        long next = getNext(ptr);
        if (next == END_PTR) {
          // tail must point to this node
          if (cachedTail != ptr) {
            if (!autoCorrect) {
              throw new IllegalStateException("ptr.next == END_PTR while cachedTail != ptr: ptr=" + ptr + ", cachedTail=" + cachedTail);
            }
            logger.info("ptr.next == END_PTR while cachedTail != ptr: ptr=" + ptr + ", cachedTail=" + cachedTail + " - recovering partial add or remove");
            if (cachedTail == END_PTR) {
              throw new IllegalStateException("ptr.next == END_PTR while cachedTail != ptr: ptr=" + ptr + ", cachedTail=" + cachedTail
                  + " - unrecoverable because cachedTail == END_PTR");
            }
            long tailPrev = getPrev(cachedTail);
            if (tailPrev != ptr) {
              throw new IllegalStateException("ptr.next == END_PTR while cachedTail != ptr: ptr=" + ptr + ", cachedTail=" + cachedTail
                  + " - unrecoverable because cachedTail->prev != ptr: " + tailPrev);
            }
            long tailNext = getNext(cachedTail);
            if (tailNext != END_PTR) {
              throw new IllegalStateException("ptr.next == END_PTR while cachedTail != ptr: ptr=" + ptr + ", cachedTail=" + cachedTail
                  + " - unrecoverable because cachedTail->next != END_PTR: " + tailNext);
            }
            setNext(ptr, next = cachedTail);
          }
        } else {
          // make sure ptr.next is allocated
          if (!allocatedIds.containsKey(next)) {
            throw new IllegalStateException("ptr.next points to unallocated block: ptr=" + ptr + ", ptr.next=" + next);
          }
          // node.next->prev == node
          long nextPrev = getPrev(next);
          if (nextPrev != ptr) {
            if (!autoCorrect) {
              throw new IllegalStateException("ptr.next->prev != ptr: ptr=" + ptr + ", ptr.prev=" + prev
                  + ", ptr.next=" + next + ", ptr.next->prev=" + nextPrev);
            }
            logger.info("ptr.next->prev != ptr: ptr=" + ptr + ", ptr.prev=" + prev + ", ptr.next=" + next
                + ", ptr.next->prev=" + nextPrev + " - recovering partial add or remove");
            if (nextPrev != prev) {
              throw new IllegalStateException("ptr.next->prev != ptr: ptr=" + ptr + ", ptr.prev=" + prev
                  + ", ptr.next=" + next + ", ptr.next->prev=" + nextPrev
                  + " - unrecoverable because ptr.next->prev != ptr.prev");
            }
            setPrev(next, ptr);
          }
        }

        ptr = next;
        count++;
      }
      // No unreferenced allocated blocks
      long firstUnreferencedBlockId = -1;
      long unreferencedCount = 0;
      for (Map.Entry<Long, Boolean> entry : allocatedIds.entrySet()) {
        if (!entry.getValue()) {
          if (firstUnreferencedBlockId == -1) {
            firstUnreferencedBlockId = entry.getKey();
          }
          unreferencedCount++;
        }
      }
      if (unreferencedCount > 0) {
        // Should only need to deallocate one block - higher count may indicate a more serious problem
        if (unreferencedCount > 1) {
          throw new IllegalStateException("More than one block allocated but not referenced: firstUnreferencedBlockId="
              + firstUnreferencedBlockId + ", unreferencedCount=" + unreferencedCount);
        }
        if (!autoCorrect) {
          throw new IllegalStateException("Block allocated but not referenced: " + firstUnreferencedBlockId);
        }
        logger.info("Block allocated but not referenced: " + firstUnreferencedBlockId + " - deallocating");
        blockBuffer.deallocate(firstUnreferencedBlockId);
      }
      // cachedSize matches the actual size
      if (cachedSize != count) {
        if (!isInit) {
          if (!autoCorrect) {
            throw new IllegalStateException("cachedSize != count: " + cachedSize + " != " + count);
          }
          logger.info("cachedSize != count: " + cachedSize + " != " + count + " - correcting");
        }
        cachedSize = count;
      }
    } else {
      if (!autoCorrect) {
        throw new IllegalStateException("Block buffer is empty - no meta data block found.");
      }
      if (!isInit) {
        logger.info("Block buffer is empty - initializing meta data block.");
      }
      metaDataBlockId = blockBuffer.allocate(HEADER_SIZE);
      blockBuffer.put(metaDataBlockId, 0, MAGIC, 0, MAGIC.length);
      blockBuffer.putInt(metaDataBlockId, MAGIC.length, VERSION);
      setHead(END_PTR);
      setTail(END_PTR);
      blockBuffer.barrier(true);
      cachedSize = 0;
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
  @Override
  public E getFirst() {
    long head = getHead();
    if (head == END_PTR) {
      throw new NoSuchElementException();
    }
    try {
      return getElement(head);
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  /**
   * Returns the last element in this list.
   * Operates in constant time.
   *
   * @return the last element in this list
   * @throws NoSuchElementException if this list is empty
   */
  @Override
  public E getLast()  {
    long tail = getTail();
    if (tail == END_PTR) {
      throw new NoSuchElementException();
    }
    try {
      return getElement(tail);
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  /**
   * Removes and returns the first element from this list.
   * Operates in constant time.
   *
   * @return the first element from this list
   * @throws NoSuchElementException if this list is empty
   */
  @Override
  public E removeFirst() {
    long head = getHead();
    if (head == END_PTR) {
      throw new NoSuchElementException();
    }
    try {
      modCount++;
      E element = getElement(head);
      remove(head);
      return element;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  /**
   * Removes and returns the last element from this list.
   * Operates in constant time.
   *
   * @return the last element from this list
   * @throws NoSuchElementException if this list is empty
   */
  @Override
  public E removeLast() {
    long tail = getTail();
    if (tail == END_PTR) {
      throw new NoSuchElementException();
    }
    try {
      modCount++;
      E element = getElement(tail);
      remove(tail);
      return element;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  /**
   * Inserts the specified element at the beginning of this list.
   * Operates in log time for free space.
   *
   * @param element the element to add
   */
  @Override
  public void addFirst(E element) {
    try {
      modCount++;
      long head = getHead();
      if (head == END_PTR) {
        addFirstEntry(element);
      } else {
        addBefore(element, head);
      }
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  /**
   * Appends the specified element to the end of this list.
   * Operates in log time for free space.
   *
   * <p>This method is equivalent to {@link PersistentLinkedList#add}.
   *
   * @param element the element to add
   */
  @Override
  public void addLast(E element) {
    try {
      modCount++;
      long tail = getTail();
      if (tail == END_PTR) {
        addFirstEntry(element);
      } else {
        addAfter(element, tail);
      }
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) != -1;
  }

  /**
   * Gets the pointer for the provided index.
   *
   * <p>This runs in linear time.</p>
   */
  private long getPointerForIndex(long index) throws IOException {
    assert cachedSize > 0;
    if (index < (cachedSize >> 1)) {
      long ptr = getHead();
      assert ptr != END_PTR;
      for (int i = 0; i < index; i++) {
        ptr = getNext(ptr);
        assert ptr != END_PTR;
      }
      return ptr;
    } else {
      // Search backwards
      long bptr = getTail();
      assert bptr != END_PTR;
      for (long i = cachedSize - 1; i > index; i--) {
        bptr = getPrev(bptr);
        assert bptr != END_PTR;
      }
      return bptr;
    }
    // if (ptr != bptr) {
    //   throw new AssertionError("ptr != bptr: "+ptr+" != "+bptr);
    // }
    // return ptr;
  }

  @Override
  public boolean remove(Object o) {
    try {
      if (o == null) {
        for (long ptr = getHead(); ptr != END_PTR; ptr = getNext(ptr)) {
          if (isNull(ptr)) {
            modCount++;
            remove(ptr);
            return true;
          }
        }
      } else {
        for (long ptr = getHead(); ptr != END_PTR; ptr = getNext(ptr)) {
          if (o.equals(getElement(ptr))) {
            modCount++;
            remove(ptr);
            return true;
          }
        }
      }
      return false;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    if (c.isEmpty()) {
      return false;
    }
    modCount++;
    for (E element : c) {
      addLast(element);
    }
    return true;
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    if (index == cachedSize) {
      return addAll(c);
    }
    if (c.isEmpty()) {
      return false;
    }
    if (index < 0 || index > cachedSize) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + cachedSize);
    }
    modCount++;
    try {
      long ptr = getPointerForIndex(index);
      for (E element : c) {
        addBefore(element, ptr);
      }
      return true;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  /**
   * Gets the number of elements in this list.
   * Operates in constant time.
   *
   * @return the number of elements in this list
   */
  @Override
  public int size() {
    return cachedSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) cachedSize;
  }

  @Override
  public boolean add(E element) {
    addLast(element);
    return true;
  }

  // </editor-fold>

  /**
   * Clears the list.
   */
  @Override
  public void clear() {
    try {
      modCount++;
      Iterator<Long> ids = blockBuffer.iterateBlockIds();
      if (!ids.hasNext()) {
        throw new AssertionError("Block buffer is empty - no meta data block found.");
      }
      long firstId = ids.next();
      if (metaDataBlockId != firstId) {
        throw new AssertionError("metaDataBlockId != firstId: " + metaDataBlockId + " != " + firstId);
      }
      setHead(END_PTR);
      setTail(END_PTR);
      cachedSize = 0;
      blockBuffer.barrier(false);
      // Deallocate all except first block
      while (ids.hasNext()) {
        ids.next();
        ids.remove();
      }
      blockBuffer.barrier(true);
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public E get(int index) {
    try {
      return getElement(getPointerForIndex(index));
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  /**
   * Replaces the element at the specified position in this list with the
   * specified element.
   *
   * <p>TODO: First try to replace at the current position?  Impact on atomicity?</p>
   *
   * @param index index of the element to replace
   * @param element element to be stored at the specified position
   */
  private void setElement(long ptr, E element) {
    modCount++;
    try {
      long prev = getPrev(ptr);
      remove(ptr);
      if (prev == END_PTR) {
        addFirst(element);
      } else {
        addAfter(element, prev);
      }
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public E set(int index, E element) {
    try {
      long ptr = getPointerForIndex(index);
      E oldElement = getElement(ptr);
      setElement(ptr, element);
      return oldElement;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public void add(int index, E element) {
    modCount++;
    try {
      long ptr = getPointerForIndex(index);
      long prev = getPrev(ptr);
      if (prev == END_PTR) {
        addFirst(element);
      } else {
        addAfter(element, prev);
      }
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public E remove(int index) {
    modCount++;
    try {
      long ptr = getPointerForIndex(index);
      E oldElement = getElement(ptr);
      remove(ptr);
      return oldElement;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public int indexOf(Object o) {
    try {
      int index = 0;
      if (o == null) {
        for (long ptr = getHead(); ptr != END_PTR; ptr = getNext(ptr)) {
          if (isNull(ptr)) {
            return index;
          }
          index++;
        }
      } else {
        for (long ptr = getHead(); ptr != END_PTR; ptr = getNext(ptr)) {
          if (o.equals(getElement(ptr))) {
            return index;
          }
          index++;
        }
      }
      return -1;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public int lastIndexOf(Object o) {
    try {
      long index = cachedSize;
      if (o == null) {
        for (long ptr = getTail(); ptr != END_PTR; ptr = getPrev(ptr)) {
          --index;
          if (isNull(ptr)) {
            if (index > Integer.MAX_VALUE) {
              throw new RuntimeException("Index too high to return from lastIndexOf: " + index);
            }
            return (int) index;
          }
        }
      } else {
        for (long ptr = getTail(); ptr != END_PTR; ptr = getPrev(ptr)) {
          --index;
          if (o.equals(getElement(ptr))) {
            if (index > Integer.MAX_VALUE) {
              throw new RuntimeException("Index too high to return from lastIndexOf: " + index);
            }
            return (int) index;
          }
        }
      }
      return -1;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  public E peek() {
    if (cachedSize == 0) {
      return null;
    }
    return getFirst();
  }

  @Override
  public E element() {
    return getFirst();
  }

  @Override
  public E poll() {
    if (cachedSize == 0) {
      return null;
    }
    return removeFirst();
  }

  @Override
  public E remove() {
    return removeFirst();
  }

  @Override
  public boolean offer(E e) {
    return add(e);
  }

  @Override
  public boolean offerFirst(E e) {
    addFirst(e);
    return true;
  }

  @Override
  public boolean offerLast(E e) {
    addLast(e);
    return true;
  }

  @Override
  public E peekFirst() {
    if (cachedSize == 0) {
      return null;
    }
    return getFirst();
  }

  @Override
  public E peekLast() {
    if (cachedSize == 0) {
      return null;
    }
    return getLast();
  }

  @Override
  public E pollFirst() {
    if (cachedSize == 0) {
      return null;
    }
    return removeFirst();
  }

  @Override
  public E pollLast() {
    if (cachedSize == 0) {
      return null;
    }
    return removeLast();
  }

  @Override
  public void push(E e) {
    addFirst(e);
  }

  @Override
  public E pop() {
    return removeFirst();
  }

  @Override
  @SuppressWarnings("element-type-mismatch")
  public boolean removeFirstOccurrence(Object o) {
    return remove(o);
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    try {
      if (o == null) {
        for (long ptr = getTail(); ptr != END_PTR; ptr = getPrev(ptr)) {
          if (isNull(ptr)) {
            modCount++;
            remove(ptr);
            return true;
          }
        }
      } else {
        for (long ptr = getTail(); ptr != END_PTR; ptr = getPrev(ptr)) {
          if (o.equals(getElement(ptr))) {
            modCount++;
            remove(ptr);
            return true;
          }
        }
      }
      return false;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

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
      if (index < 0 || index > cachedSize) {
        throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + cachedSize);
      }
      try {
        if (index == cachedSize) {
          // Points one past the end
          nextPtr = END_PTR;
          nextIndex = cachedSize;
        } else {
          // Points within the list
          nextPtr = getPointerForIndex(index);
          nextIndex = index;
        }
      } catch (IOException err) {
        throw new UncheckedIOException(err);
      }
    }

    @Override
    public boolean hasNext() {
      return nextIndex != cachedSize;
    }

    @Override
    public E next() throws NoSuchElementException {
      checkForComodification();
      if (nextIndex == cachedSize) {
        throw new NoSuchElementException();
      }
      try {
        lastReturned = nextPtr;
        nextPtr = getNext(nextPtr);
        nextIndex++;
        assert (nextPtr == END_PTR && nextIndex == cachedSize) || (nextPtr != END_PTR && nextIndex < cachedSize);
        return getElement(lastReturned);
      } catch (IOException err) {
        throw new UncheckedIOException(err);
      }
    }

    @Override
    public boolean hasPrevious() {
      return nextIndex != 0;
    }

    @Override
    public E previous() throws NoSuchElementException {
      checkForComodification();
      if (nextIndex == 0) {
        throw new NoSuchElementException();
      }
      try {
        lastReturned = nextPtr = nextPtr == END_PTR ? getTail() : getPrev(nextPtr);
        nextIndex--;
        assert (nextPtr == END_PTR && nextIndex == cachedSize) || (nextPtr != END_PTR && nextIndex < cachedSize);
        return getElement(lastReturned);
      } catch (IOException err) {
        throw new UncheckedIOException(err);
      }
    }

    @Override
    public int nextIndex() {
      if (nextIndex > Integer.MAX_VALUE) {
        throw new RuntimeException("Index too high to return from nextIndex: " + nextIndex);
      }
      assert nextIndex >= 0;
      return (int) nextIndex;
    }

    @Override
    public int previousIndex() {
      long prevIndex = nextIndex - 1;
      if (prevIndex > Integer.MAX_VALUE) {
        throw new RuntimeException("Index too high to return from previousIndex: " + prevIndex);
      }
      assert prevIndex >= 0;
      return (int) prevIndex;
    }

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
        if (nextPtr == lastReturned) {
          nextPtr = lastNext;
        } else {
          nextIndex--;
        }
        lastReturned = END_PTR;
        expectedModCount++;
        assert (nextPtr == END_PTR && nextIndex == cachedSize) || (nextPtr != END_PTR && nextIndex < cachedSize);
      } catch (IOException err) {
        throw new UncheckedIOException(err);
      }
    }

    @Override
    public void set(E e) {
      if (lastReturned == END_PTR) {
        throw new IllegalStateException();
      }
      checkForComodification();
      setElement(lastReturned, e);
      expectedModCount++;
    }

    @Override
    public void add(E e) {
      checkForComodification();
      try {
        lastReturned = END_PTR;
        modCount++;
        addBefore(e, nextPtr);
        nextIndex++;
        expectedModCount++;
      } catch (IOException err) {
        throw new UncheckedIOException(err);
      }
    }

    final void checkForComodification() {
      if (modCount != expectedModCount) {
        throw new ConcurrentModificationException();
      }
    }
  }

  @Override
  public Iterator<E> descendingIterator() {
    return new DescendingIterator();
  }

  /** Adapter to provide descending iterators via ListItr.previous. */
  private class DescendingIterator implements Iterator<E> {
    final ListItr itr = new ListItr(size());

    @Override
    public boolean hasNext() {
      return itr.hasPrevious();
    }

    @Override
    public E next() throws NoSuchElementException {
      return itr.previous();
    }

    @Override
    public void remove() {
      itr.remove();
    }
  }

  @Override
  public Object[] toArray() {
    try {
      if (cachedSize > Integer.MAX_VALUE) {
        throw new RuntimeException("Too many elements in list to create Object[]: " + cachedSize);
      }
      Object[] result = new Object[(int) cachedSize];
      int i = 0;
      for (long ptr = getHead(); ptr != END_PTR; ptr = getNext(ptr)) {
        result[i++] = getElement(ptr);
      }
      return result;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] a) {
    if (cachedSize > Integer.MAX_VALUE) {
      throw new RuntimeException("Too many elements in list to fill or create array: " + cachedSize);
    }
    try {
      if (a.length < cachedSize) {
        a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), (int) cachedSize);
      }
      int i = 0;
      @SuppressWarnings("MismatchedReadAndWriteOfArray")
      Object[] result = a;
      for (long ptr = getHead(); ptr != END_PTR; ptr = getNext(ptr)) {
        result[i++] = getElement(ptr);
      }
      if (a.length > cachedSize) {
        a[(int) cachedSize] = null;
      }

      return a;
    } catch (IOException err) {
      throw new UncheckedIOException(err);
    }
  }

  /**
   * Closes the {@linkplain PersistentBlockBuffer block buffer} backing this list.
   */
  @Override
  public void close() throws IOException {
    if (!blockBuffer.isClosed()) {
      // assert isConsistent();
      blockBuffer.close();
    }
  }
}
