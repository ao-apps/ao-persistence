/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2013, 2016, 2019, 2020, 2021  AO Industries, Inc.
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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;
// import org.checkthread.annotations.NotThreadSafe;
// import org.checkthread.annotations.ThreadSafe;

/**
 * <p>
 * Treats a <code>PersistentBuffer</code> as a set of allocatable blocks.
 * Each block is stored in a 2^n area of the buffer, where the usable
 * space is 2^n-1 (the first byte of that area of the buffer indicates
 * the block size and allocated status).
 * </p>
 * <p>
 * Free space maps are generated upon instantiation.  This means that startup
 * costs can be fairly high.  This class is designed for long-lifetime situations.
 * </p>
 * <p>
 * Blocks that are allocated take no space in memory, while blocks that are deallocated
 * consume space.  Adjacent blocks are automatically merged into a larger free block of
 * twice the size.  Blocks are also split into smaller blocks before allocating additional
 * space.
 * </p>
 * <p>
 * Fragmentation may occur in the file over time, but is minimized by the use of
 * per-block size free space maps along with block merging and splitting.  There
 * is currently no compaction tool.
 * </p>
 * <p>
 * Each entry has a one-byte header:
 *     bits 0-5: maxBits     (0-63) the power of two of the size of this block (max data size is <code>(2^maxBits)-1</code>).
 *     bit  6:   reserved, should be 0
 *     bit  7:   allocated flag
 * </p>
 * <p>
 * This class is not thread-safe.
 * </p>
 *
 * @author  AO Industries, Inc.
 */
public class DynamicPersistentBlockBuffer extends AbstractPersistentBlockBuffer {

	private static final Logger logger = Logger.getLogger(DynamicPersistentBlockBuffer.class.getName());

	/**
	 * Tracks free space on a per power-of-two basis.
	 */
	private final List<SortedSet<Long>> freeSpaceMaps = new ArrayList<>(64);

	/**
	 * Keeps track of modification counts to try to detect concurrent modifications.
	 */
	private int modCount;

	/**
	 * Creates a buffer.
	 */
	public DynamicPersistentBlockBuffer(PersistentBuffer pbuffer) throws IOException {
		super(pbuffer);
		for(int c=0;c<64;c++) {
			freeSpaceMaps.add(null);
		}
		// Build the free space maps and expand to end on an even block
		long capacity = pbuffer.capacity();
		long id = 0;
		while(id<capacity) {
			assert isValidRange(id);
			byte header = pbuffer.get(id);
			int blockSizeBits = getBlockSizeBits(header);
			if(!isBlockAligned(id, blockSizeBits)) throw new IOException("Block not aligned: "+id);
			// Auto-expand if underlying buffer doesn't end on a block boundary, this may be the result of a partial increase in size
			long blockEnd = id + getBlockSize(blockSizeBits);
			if(blockEnd>capacity) {
				// Could remove last block is unallocated, but the safer choice may be to grow the capacity and leave data intact
				logger.warning("Expanding capacity to match block end: capacity="+capacity+", blockEnd="+blockEnd);
				pbuffer.setCapacity(capacity=blockEnd);
			}
			if(!isAllocated(header)) {
				addFreeSpaceMap(id, blockSizeBits, capacity, true);
				//SortedSet<Long> fsm = freeSpaceMaps.get(blockSizeBits);
				//if(fsm==null) freeSpaceMaps.set(blockSizeBits, fsm = new TreeSet<Long>());
				//if(!fsm.add(id)) throw new AssertionError("Free space map already contains entry: "+id);
			}
			id = blockEnd;
		}
		assert id==capacity : "id!=capacity: "+id+"!="+capacity;
	}

	// <editor-fold desc="Bit Manipulation">
	/**
	 * Space will always be allocated to align with this page size.
	 */
	private static final long PAGE_SIZE = 0x1000L; // Must be a power of two.
	private static final long PAGE_OFFSET_MASK = PAGE_SIZE-1;
	private static final long PAGE_MASK = -PAGE_SIZE;

	// @ThreadSafe
	private static boolean isAllocated(byte header) {
		return (header&0x80)!=0;
	}

	// @ThreadSafe
	private static int getBlockSizeBits(byte header) {
		return header&0x3f;
	}

	/**
	 * Gets the block size given the block size bits.
	 */
	// @ThreadSafe
	private static long getBlockSize(int blockSizeBits) {
		return 1L << blockSizeBits;
	}

	/**
	 * Gets the offset of an id within a page.
	 */
	// @ThreadSafe
	private static long getPageOffset(long id) {
		return id&PAGE_OFFSET_MASK;
	}

	/**
	 * Gets the nearest page boundary, rounding up if necessary.
	 */
	// @ThreadSafe
	private static long getNearestPage(long id) {
		if(getPageOffset(id)!=0) id = (id & PAGE_MASK)+PAGE_SIZE;
		return id;
	}
	// </editor-fold>

	// <editor-fold desc="Assertions and Data Consistency">
	/**
	 * Checks that is a valid blockSizeBits.
	 */
	// @ThreadSafe
	private static boolean isValidBlockSizeBits(int blockSizeBits) {
		return blockSizeBits>=0 && blockSizeBits<=0x3f;
	}

	/**
	 * Makes sure the id is in the valid range: <code>0 &lt;= id &lt; capacity</code>
	 */
	// @NotThreadSafe
	private boolean isValidRange(long id) throws IOException {
		return id>=0 && id<pbuffer.capacity();
	}

	/**
	 * Each block should always be aligned based on its size.  This means that
	 * all bits for its location less than its size should be zero.
	 */
	// @NotThreadSafe
	private boolean isBlockAligned(long id, int blockSizeBits) throws IOException {
		assert isValidRange(id);
		assert isValidBlockSizeBits(blockSizeBits);
		return ((getBlockSize(blockSizeBits)-1)&id)==0;
	}

	/**
	 * Makes sure a block is complete: <code>(id + blockSize) &lt;= capacity</code>
	 */
	// @NotThreadSafe
	private boolean isBlockComplete(long id, int blockSizeBits) throws IOException {
		assert isValidRange(id);
		assert isValidBlockSizeBits(blockSizeBits);
		return (id+getBlockSize(blockSizeBits))<=pbuffer.capacity();
	}

	/**
	 * Checks if the provided block is allocated.  This is for debugging only
	 * to be used in assertions.  It is not a reliable mechanism to use because
	 * block combining and splitting can cause this method to check arbitrary
	 * data.  The only true way to check if allocated is to sequentially scan
	 * from the beginning of the file considering each block size.
	 */
	// @NotThreadSafe
	private boolean isAllocated(long id) throws IOException {
		assert isValidRange(id);
		byte header = pbuffer.get(id);
		assert isBlockAligned(id, getBlockSizeBits(header)) : "Block not aligned: "+id;
		assert isBlockComplete(id, getBlockSizeBits(header)) : "Block is incomplete: "+id;
		return isAllocated(header);
	}
	// </editor-fold>

	// <editor-fold desc="Allocation and Deallocation">
	/**
	 * Adds the block at the provided location to the free space maps.
	 *
	 * @param  groupEnabled   enables grouping with adjacent and aligned free blocks
	 * @param  groupPrevOnly  Indicates that this should only group with previous entries.  This
	 *                        is used when initially populating the free space maps or when increasing
	 *                        the capacity.
	 */
	// @NotThreadSafe
	private void addFreeSpaceMap(long id, int blockSizeBits, long capacity, boolean groupPrevOnly) throws IOException {
		assert isValidRange(id);
		assert isValidBlockSizeBits(blockSizeBits);
		assert blockSizeBits==getBlockSizeBits(pbuffer.get(id));
		assert capacity>=0;
		assert !isAllocated(id);
		// Group as much as possible within the same power-of-two block
		boolean blockSizeBitsUpdated = false;
		while(blockSizeBits<0x3f) {
			assert isBlockAligned(id, blockSizeBits) : "Block not aligned: "+id;
			assert isBlockComplete(id, blockSizeBits) : "Block is incomplete: "+id;
			long blockSize = getBlockSize(blockSizeBits);
			long blockOffsetMask = blockSize-1;
			// Only allow grouping if id is aligned with the current number of bits
			if((id&blockOffsetMask)!=0) break;
			long biggerBlockSize = blockSize << 1;
			long biggerBlockMask = -biggerBlockSize;
			long idBiggerBlockMask = id&biggerBlockMask;
			long prevId = id - blockSize;
			if(prevId>=0 && (prevId&biggerBlockMask)==idBiggerBlockMask) {
				// The block to the left must be the same number of bits and be unallocated
				byte prevHeader = pbuffer.get(prevId);
				if(isAllocated(prevHeader) || blockSizeBits!=getBlockSizeBits(prevHeader)) {
					// Block to the left is allocated or a different size, stop grouping
					break;
				} else {
					id = prevId;
					// Remove prev from FSM
					SortedSet<Long> fsm = freeSpaceMaps.get(blockSizeBits);
					if(fsm==null) throw new AssertionError("fsm is null for bits="+blockSizeBits);
					if(!fsm.remove(prevId)) throw new AssertionError("fsm for bits="+blockSizeBits+" did not contain prevId="+prevId);
					blockSizeBits++;
					blockSizeBitsUpdated = true;
				}
			} else {
				// Stop if groupPrevOnly or only group right if the pbuffer has room for the bigger parent
				if(groupPrevOnly || (id+biggerBlockSize)>capacity) {
					// Stop grouping
					break;
				} else {
					long nextId = id + blockSize;
					assert (nextId&biggerBlockMask)==idBiggerBlockMask;
					// The block to the right must be the same number of bits and be unallocated
					byte nextHeader = pbuffer.get(nextId);
					if(isAllocated(nextHeader) || blockSizeBits!=getBlockSizeBits(nextHeader)) {
						// Block to the right is allocated or a different size, stop grouping
						break;
					} else {
						// Remove next from FSM
						SortedSet<Long> fsm = freeSpaceMaps.get(blockSizeBits);
						if(fsm==null) throw new AssertionError("fsm is null for bits="+blockSizeBits);
						if(!fsm.remove(nextId)) throw new AssertionError("fsm for bits="+blockSizeBits+" did not contain nextId="+nextId);
						blockSizeBits++;
						blockSizeBitsUpdated = true;
					}
				}
			}
		}
		if(blockSizeBitsUpdated) {
			// Redo the same assertions above because id and blockSizeBits may have changed
			assert isValidRange(id);
			assert isValidBlockSizeBits(blockSizeBits);
			assert isBlockAligned(id, blockSizeBits) : "Block not aligned: "+id;
			assert isBlockComplete(id, blockSizeBits) : "Block is incomplete: "+id;
			assert !isAllocated(pbuffer.get(id)) : "Block is allocated: "+id;
			assert pbuffer.get(id)!=blockSizeBits;
			pbuffer.put(id, (byte)blockSizeBits);
			//pbuffer.barrier(false); // Not required because if this write fails either side will still be consistent?
		}
		SortedSet<Long> fsm = freeSpaceMaps.get(blockSizeBits);
		if(fsm==null) freeSpaceMaps.set(blockSizeBits, fsm = new TreeSet<>());
		if(!fsm.add(id)) throw new AssertionError("Free space map already contains entry: "+id);
		//System.out.println("DEBUG: Added to fsm: fsm["+blockSizeBits+"].size()="+fsm.size());
	}

	/**
	 * Tries to find existing free space for the provided block size.  If the free space
	 * is available, will return the available space.  Otherwise,
	 * it will look for a larger free space and split it into two smaller pieces, returning
	 * the first of the two pieces.
	 *
	 * @return  the address of the block or <code>-1</code> if no free space can be found
	 */
	// @NotThreadSafe
	private long splitAllocate(int blockSizeBits, long capacity) throws IOException {
		return splitAllocate(blockSizeBits, capacity, 0);
	}
	private long splitAllocate(int blockSizeBits, long capacity, int recursionDepth) throws IOException {
		assert isValidBlockSizeBits(blockSizeBits);
		assert capacity>=0;
		SortedSet<Long> fsm = freeSpaceMaps.get(blockSizeBits);
		if(fsm!=null && !fsm.isEmpty()) {
			// No split needed
			Long id = fsm.first();
			assert isBlockAligned(id, blockSizeBits) : "Block not aligned: "+id;
			assert isBlockComplete(id, blockSizeBits) : "Block is incomplete: "+id;
			fsm.remove(id);
			return id;
		} else {
			// End recursion
			if(blockSizeBits==0x3f) return -1;
			long blockSize = getBlockSize(blockSizeBits);
			if(blockSize>capacity) return -1;
			// Try split
			long biggerAvailableId = splitAllocate(blockSizeBits+1, capacity, recursionDepth+1);
			// No bigger available
			if(biggerAvailableId==-1) return -1;
			assert isBlockAligned(biggerAvailableId, blockSizeBits+1) : "Block not aligned: "+biggerAvailableId;
			assert isBlockComplete(biggerAvailableId, blockSizeBits+1) : "Block is incomplete: "+biggerAvailableId;
			// Split the bigger one, adding the right half to the free space map
			long nextId = biggerAvailableId+blockSize;
			assert isBlockAligned(nextId, blockSizeBits) : "Block not aligned: "+nextId;
			assert isBlockComplete(nextId, blockSizeBits) : "Block is incomplete: "+nextId;
			if(pbuffer.get(nextId)!=blockSizeBits) {
				pbuffer.put(nextId, (byte)blockSizeBits);
				if(recursionDepth==0) barrier(false); // When splitting, the right side must have appropriate size headers before left side is updated
			}
			if(fsm==null) freeSpaceMaps.set(blockSizeBits, fsm = new TreeSet<>());
			fsm.add(nextId);
			assert pbuffer.get(biggerAvailableId)!=(byte)blockSizeBits;
			pbuffer.put(biggerAvailableId, (byte)blockSizeBits);
			// pbuffer.barrier(false); // Not required because writes will be constrained to the returned block, and if the header is not updated it will remain unallocated at its previous size
			return biggerAvailableId;
		}
	}

	/**
	 * Adds newly allocated space to the free space maps.
	 */
	// @NotThreadSafe
	private void configureNewAllocation(long start, long capacity) throws IOException {
		//System.out.println("DEBUG: start="+start+", capacity="+capacity+", capacity/start="+((float)capacity/(float)start));
		//long iterations = 0;
		// TODO: Do not combine to the first block of size blockSizeBits and return it directly, avoiding call to addFSM and splitAllocate?
		while(start<capacity) {
			// Find the largest power of two block that aligns with the start and fits between start and end
			int bits = 1;
			while(bits<0x3f) {
				if((start&((1L << bits)-1))!=0) {
					// Not aligned
					break;
				}
				long blockEnd = start + (1L << bits);
				if(blockEnd<0 || blockEnd>capacity) {
					// Outside capacity
					break;
				}
				bits++;
			}
			bits--;
			assert isBlockAligned(start, bits) : "Block not aligned: "+start;
			assert isBlockComplete(start, bits) : "Block is incomplete: "+start;
			if(bits>0) {
				assert pbuffer.get(start)!=(byte)bits;
				pbuffer.put(start, (byte)bits);
				//pbuffer.barrier(false); // Not necessary because free space will be combined an recovery for TIGHT.  BALANCED will combine when needed, and FAST allocates minimally
			}
			addFreeSpaceMap(start, bits, capacity, true);
			start += 1L << bits;
			//iterations++;
		}
		//System.out.println("DEBUG: Completed in "+iterations+" iterations");
		assert start==capacity;
	}

	/**
	 * This will call <code>barrier</code> as necessary during block splitting.
	 */
	// @NotThreadSafe
	@Override
	public long allocate(long minimumSize) throws IOException {
		if(minimumSize<0) throw new IllegalArgumentException("minimumSize<0: "+minimumSize);
		modCount++;
		// Determine the min block size for the provided input
		int blockSizeBits = 64 - Long.numberOfLeadingZeros(minimumSize);
		long capacity = pbuffer.capacity();
		long id = splitAllocate(blockSizeBits, capacity);
		if(id!=-1) {
			assert isValidRange(id);
			assert blockSizeBits==getBlockSizeBits(pbuffer.get(id));
			assert isBlockAligned(id, blockSizeBits) : "Block not aligned: "+id;
			assert isBlockComplete(id, blockSizeBits) : "Block is incomplete: "+id;
			assert !isAllocated(pbuffer.get(id)) : "Block is allocated: "+id;
			assert pbuffer.get(id)!=(byte)(0x80 | blockSizeBits);
			pbuffer.put(id, (byte)(0x80 | blockSizeBits));
			//pbuffer.barrier(false); // TODO: necessary?
		} else {
			// No block available and no blocks may be combined to fulfill allocation, increase capacity
			long blockSize = getBlockSize(blockSizeBits);
			assert blockSize>minimumSize; // Must have one byte extra for the header
			long blockMask = blockSize - 1;
			assert blockMask>=0;
			// Align new block
			long blockStart = capacity;
			long blockOffset = blockStart & blockMask;
			if(blockOffset!=0) {
				// Expanding existing allocation to the right to align the current block
				long expandBytes = blockSize - blockOffset;
				assert expandBytes>0 && expandBytes<blockSize;
				blockStart += expandBytes;
			}
			assert (blockStart & blockMask)==0;
			long newCapacity = blockStart + blockSize;
			// Grow the file by at least 25% its previous size
			long percentCapacity = capacity + (capacity >> 2);
			if(percentCapacity>newCapacity) newCapacity = percentCapacity;
			// Align with page
			newCapacity = getNearestPage(newCapacity);
			assert getPageOffset(newCapacity)==0;
			// Expand and initialize new space
			pbuffer.setCapacity(newCapacity);
			configureNewAllocation(capacity, newCapacity);
			// The expansion must have caused free space that can fulfill this allocation.
			id = splitAllocate(blockSizeBits, newCapacity);
			if(id==-1) throw new AssertionError("Free space not available after expansion: capacity="+capacity+", newCapacity="+newCapacity);
			assert isValidRange(id);
			assert blockSizeBits==getBlockSizeBits(pbuffer.get(id));
			assert isBlockAligned(id, blockSizeBits) : "Block not aligned: "+id;
			assert isBlockComplete(id, blockSizeBits) : "Block is incomplete: "+id;
			assert !isAllocated(pbuffer.get(id)) : "Block is allocated: "+id;
			assert pbuffer.get(id)!=(byte)(0x80 | blockSizeBits);
			pbuffer.put(id, (byte)(0x80 | blockSizeBits));
			//pbuffer.barrier(false); // TODO: necessary?
		}
		// These assertions cause a failure that is unexpected
		assert isValidRange(id);
		assert blockSizeBits==getBlockSizeBits(pbuffer.get(id));
		assert isBlockAligned(id, blockSizeBits) : "Block not aligned: "+id;
		assert isBlockComplete(id, blockSizeBits) : "Block is incomplete: "+id;
		assert isAllocated(pbuffer.get(id)) : "Block not allocated: "+id;
		assert freeSpaceMaps.get(blockSizeBits)==null || !freeSpaceMaps.get(blockSizeBits).contains(id) : "Block still in free space maps: "+id;
		return id;
	}

	// @NotThreadSafe
	@Override
	public void deallocate(long id) throws IOException, IllegalStateException {
		assert isValidRange(id);
		byte header = pbuffer.get(id);
		int blockSizeBits = getBlockSizeBits(header);
		assert isBlockAligned(id, blockSizeBits) : "Block not aligned: "+id;
		assert isBlockComplete(id, blockSizeBits) : "Block is incomplete: "+id;
		if(!isAllocated(header)) throw new AssertionError("Block not allocated: "+id);
		modCount++;
		assert pbuffer.get(id)!=(byte)(header&0x7f);
		pbuffer.put(id, (byte)(header&0x7f));
		//pbuffer.barrier(false); // TODO: necessary?
		addFreeSpaceMap(id, blockSizeBits, pbuffer.capacity(), false);
	}
	// </editor-fold>

	// <editor-fold desc="PersistentBlockBuffer Implementation">
	// @NotThreadSafe
	@Override
	public Iterator<Long> iterateBlockIds() throws IOException {
		return new Iterator<Long>() {
			private int expectedModCount = modCount;
			private long lastId = -1;
			private long nextId = 0;

			// @NotThreadSafe
			@Override
			public boolean hasNext() {
				if(expectedModCount!=modCount) throw new ConcurrentModificationException();
				try {
					long capacity = pbuffer.capacity();
					while(nextId<capacity) {
						byte header = pbuffer.get(nextId);
						int blockSizeBits = getBlockSizeBits(header);
						assert isBlockAligned(nextId, blockSizeBits) : "Block not aligned: "+nextId;
						assert isBlockComplete(nextId, blockSizeBits) : "Block is incomplete: "+nextId;
						if(isAllocated(header)) return true;
						nextId += getBlockSize(blockSizeBits);
					}
					return false;
				} catch(IOException err) {
					throw new UncheckedIOException(err);
				}
			}

			// @NotThreadSafe
			@Override
			public Long next() {
				if(expectedModCount!=modCount) throw new ConcurrentModificationException();
				try {
					long capacity = pbuffer.capacity();
					while(nextId<capacity) {
						byte header = pbuffer.get(nextId);
						int blockSizeBits = getBlockSizeBits(header);
						assert isBlockAligned(nextId, blockSizeBits) : "Block not aligned: "+nextId;
						assert isBlockComplete(nextId, blockSizeBits) : "Block is incomplete: "+nextId;
						long ptr = nextId;
						nextId += getBlockSize(blockSizeBits);
						if(isAllocated(header)) return lastId = ptr;
					}
					throw new NoSuchElementException();
				} catch(IOException err) {
					throw new UncheckedIOException(err);
				}
			}

			// @NotThreadSafe
			@Override
			public void remove() {
				try {
					if(expectedModCount!=modCount) throw new ConcurrentModificationException();
					if(lastId==-1) throw new IllegalStateException();
					deallocate(lastId);
					expectedModCount++;
					lastId = -1;
				} catch(IOException err) {
					throw new UncheckedIOException(err);
				}
			}
		};
	}

	/**
	 * Gets the maximum amount of data that may be stored in the entry.  This
	 * is the underlying power-of-two block size minus one.  May only check
	 * the block size of allocated blocks (not necessarily enforced, it is up
	 * to the caller to ensure this).
	 */
	// @NotThreadSafe
	@Override
	public long getBlockSize(long id) throws IOException {
		assert isValidRange(id);
		byte header = pbuffer.get(id);
		int blockSizeBits = getBlockSizeBits(header);
		assert isBlockAligned(id, blockSizeBits) : "Block not aligned: "+id;
		assert isBlockComplete(id, blockSizeBits) : "Block is incomplete: "+id;
		if(!isAllocated(header)) throw new AssertionError("Block not allocated: "+id);
		return getBlockSize(blockSizeBits) - 1;
	}

	// The block data starts one byte past the block header
	// @NotThreadSafe
	@Override
	protected long getBlockAddress(long id) throws IOException {
		assert isAllocated(id) : "Block not allocated: "+id;
		return id + 1;
	}

	/**
	 * The capacity should always be enough because the capacity ensured here is
	 * constrained to a single block, and blocks are always allocated fully.
	 * This merely asserts this fact.
	 */
	// @NotThreadSafe
	@Override
	protected void ensureCapacity(long capacity) throws IOException {
		assert pbuffer.capacity()>=capacity: "pbuffer.capacity()<capacity";
	}
	// </editor-fold>
}
