/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2013, 2016, 2019, 2020  AO Industries, Inc.
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

import com.aoindustries.exception.WrappedException;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
// import org.checkthread.annotations.NotThreadSafe;
// import org.checkthread.annotations.ThreadSafe;

/**
 * <p>
 * Treats a <code>PersistentBuffer</code> as a set of allocatable blocks.
 * Each block is stored in a block of fixed size.  The first block has an id
 * of zero, and the blocks go up by ones.  The storage address is determined
 * from this logical id on the fly.  The storage address and the block address
 * are not the same.
 * </p>
 * <p>
 * One block of free space map preceeds a set of blocks.  However, with exceptionally
 * large block sizes, the free space map will be smaller.  The free space map is the
 * maximum size of the block size and 2^n, where n+(ceil(log(2)(blockSize)))=63.
 * The result is that the minimum free space map size will be used to address the
 * entire 2^63-1 address space.
 * </p>
 * <p>
 * The switchover point is at one gigabyte block size (2^30).  The total of 2^63
 * address space may contain a maximum of 2^33 of these blocks.  Each block
 * consumes one bit of the space, the 2^30 bitmap block may contain a total of
 * 2^33 bits, an exact match for the number of blocks.
 * </p>
 * <p>
 * A final example is for a 16 GB block size (2^34).  The total of 2^63
 * address space may contain a maximum of 2^29 of these blocks.  Each block
 * consumes one bit of the space, thus the free space map only needs to contain
 * 2^26 bytes to cover the entire address space.
 * </p>
 *
 * @author  AO Industries, Inc.
 */
public class FixedPersistentBlockBuffer extends AbstractPersistentBlockBuffer /*implements RandomAccessPersistentBlockBuffer*/ {

	private final long blockSize;
	private final boolean singleBitmap;

	/**
	 * The number of bytes in the bitmap.
	 */
	private final long bitmapSize;

	/**
	 * Keeps track of modification counts to try to detect concurrent modifications.
	 */
	private int modCount;

	/**
	 * Creates a persistent buffer with the provided block size.  There may be
	 * performance advantages to block sizes that match or are multiples of the
	 * system page size.  For smaller block sizes, there may also be reliability
	 * advantages to block sizes that are fractions of the system page size
	 * or the physical media block size.  A good overall approach would be to
	 * select even powers of two (1, 2, 4, 8, ...).
	 */
	public FixedPersistentBlockBuffer(PersistentBuffer pbuffer, long blockSize) throws IOException {
		super(pbuffer);
		if(blockSize<=0) throw new IllegalArgumentException("blockSize<=0: "+blockSize);
		this.blockSize = blockSize;

		// Initialize the first bitmap
		int numZeros = Long.numberOfLeadingZeros(blockSize);
		if(numZeros<=3) {
			singleBitmap = true;
			bitmapSize = 1;
		} else {
			long smallestPowerOfTwo = 1L << (64-1-numZeros);
			if(PersistentCollections.ASSERT) assert smallestPowerOfTwo==Long.highestOneBit(blockSize);
			if(smallestPowerOfTwo!=blockSize) {
				//smallestPowerOfTwo <<= 1;
				numZeros--;
			}
			if(numZeros<=(64-1-30)) {
				// Only a single bit map at the beginning of the file
				singleBitmap = true;
				bitmapSize = 1L << (numZeros-3);
			} else {
				// Multiple bit maps spread throughout the file
				singleBitmap = false;
				bitmapSize = blockSize;
			}
		}
	}

	/**
	 * Gets the address of the byte that stores the bitmap for the provided id.
	 * This is algorithmic and may be beyond the end of the buffer capacity.
	 */
	// @ThreadSafe // singleBitmap, bitmapSize, and blockSize are all final
	private long getBitMapBitsAddress(long id) {
		if(singleBitmap) {
			return id >>> 3;
		} else {
			// Find which block to use
			long bitsPerBitmap = bitmapSize << 3;
			long bitmapNum = id / bitsPerBitmap;
			long bitmapStart = bitmapNum * (bitmapSize + blockSize * bitsPerBitmap);
			return bitmapStart + ((id % bitsPerBitmap) >>> 3);
		}
	}

	/**
	 * Gets the address that stores the beginning of the block with the provided id.
	 * This is algorithmic and may be beyond the end of the buffer capacity.
	 */
	// @ThreadSafe // singleBitmap, bitmapSize, and blockSize are all final
	@Override
	protected long getBlockAddress(long id) {
		if(singleBitmap) {
			return bitmapSize + id * blockSize;
		} else {
			long bitsPerBitmap = bitmapSize << 3;
			long bitmapNum = id / bitsPerBitmap;
			long bitmapStart = bitmapNum * (bitmapSize + blockSize * bitsPerBitmap);
			return bitmapStart + bitmapSize + (id % bitsPerBitmap) * blockSize;
		}
	}

	private long lowestFreeId = 0; // One direction scan used before knownFreeIds is populated
	private final SortedSet<Long> knownFreeIds = new TreeSet<>();

	/**
	 * Allocates a block.
	 * This does not directly cause any <code>barrier</code>s.
	 */
	// @NotThreadSafe
	@Override
	public long allocate(long minimumSize) throws IOException {
		if(minimumSize>blockSize) throw new IOException("minimumSize>blockSize: "+minimumSize+">"+blockSize);
		// Check known first
		if(!knownFreeIds.isEmpty()) {
			Long freeIdL = knownFreeIds.first();
			knownFreeIds.remove(freeIdL);
			long freeId = freeIdL;
			long bitmapBitsAddress = getBitMapBitsAddress(freeId);
			byte bits = pbuffer.get(bitmapBitsAddress);
			int bit = 1 << (freeId&7);
			modCount++;
			pbuffer.put(bitmapBitsAddress, (byte)(bits | bit));
			return freeId;
		}
		long bitmapBitsAddress = getBitMapBitsAddress(lowestFreeId);
		long capacity = pbuffer.capacity();
		while(bitmapBitsAddress<capacity) {
			byte bits = pbuffer.get(bitmapBitsAddress);
			if(bits!=-1) {
				// Check as many bits as possible
				for(int bit = 1 << (lowestFreeId&7); bit!=0x100; bit <<= 1) {
					if((bits&bit)==0) {
						modCount++;
						pbuffer.put(bitmapBitsAddress, (byte)(bits | bit));
						return lowestFreeId++;
					}
					lowestFreeId++;
				}
			} else {
				// All ones, go to the next byte
				lowestFreeId = (lowestFreeId & 0xfffffffffffffff8L)+8L;
			}
			bitmapBitsAddress = getBitMapBitsAddress(lowestFreeId);
		}
		// Grow the underlying storage to make room for the bitmap space.
		if(PersistentCollections.ASSERT) assert (lowestFreeId&7)==0 : "lowestFreeId must be the beginning of a byte";
		modCount++;
		expandCapacity(capacity, bitmapBitsAddress+1);
		pbuffer.put(bitmapBitsAddress, (byte)1);
		return lowestFreeId++;
	}

	/**
	 * Deallocates the block for the provided id.
	 * This does not directly cause any <code>barrier</code>s.
	 */
	// @NotThreadSafe
	@Override
	public void deallocate(long id) throws IOException {
		long bitmapBitsAddress = getBitMapBitsAddress(id);
		byte bits = pbuffer.get(bitmapBitsAddress);
		int bit = 1 << (id & 7);
		if((bits&bit)==0) throw new IllegalStateException("Block already deallocated: "+id);
		knownFreeIds.add(id);
		// else if(id < lowestFreeId) lowestFreeId = id;
		modCount++;
		pbuffer.put(bitmapBitsAddress, (byte)(bits ^ bit));
	}

	// @NotThreadSafe
	@Override
	public Iterator<Long> iterateBlockIds() {
		return new Iterator<Long>() {
			private int expectedModCount = modCount;
			private long lastId = -1;
			private long nextId = 0;
			// @NotThreadSafe
			@Override
			public boolean hasNext() {
				if(expectedModCount!=modCount) throw new ConcurrentModificationException();
				try {
					long bitmapBitsAddress = getBitMapBitsAddress(nextId);
					long capacity = pbuffer.capacity();
					while(bitmapBitsAddress<capacity) {
						byte bits = pbuffer.get(bitmapBitsAddress);
						if(bits!=0) {
							// Check as many bits as possible
							for(int bit = 1 << (nextId&7); bit!=0x100; bit <<= 1) {
								if((bits&bit)!=0) return true;
								nextId++;
							}
						} else {
							// All zero, go to the next byte
							nextId = (nextId & 0xfffffffffffffff8L)+8L;
						}
						bitmapBitsAddress = getBitMapBitsAddress(nextId);
					}
					return false;
				} catch(IOException err) {
					throw new WrappedException(err);
				}
			}
			// @NotThreadSafe
			@Override
			public Long next() {
				if(expectedModCount!=modCount) throw new ConcurrentModificationException();
				try {
					long bitmapBitsAddress = getBitMapBitsAddress(nextId);
					long capacity = pbuffer.capacity();
					while(bitmapBitsAddress<capacity) {
						byte bits = pbuffer.get(bitmapBitsAddress);
						if(bits!=0) {
							// Check as many bits as possible
							for(int bit = 1 << (nextId&7); bit!=0x100; bit <<= 1) {
								if((bits&bit)!=0) return lastId = nextId++;
								nextId++;
							}
						} else {
							// All zero, go to the next byte
							nextId = (nextId & 0xfffffffffffffff8L)+8L;
						}
						bitmapBitsAddress = getBitMapBitsAddress(nextId);
					}
					throw new NoSuchElementException();
				} catch(IOException err) {
					throw new WrappedException(err);
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
					throw new WrappedException(err);
				}
			}
		};
	}

	// @NotThreadSafe // blockSize is final
	@Override
	public long getBlockSize(long id) {
		return blockSize;
	}

	// @NotThreadSafe
	protected void expandCapacity(long oldCapacity, long newCapacity) throws IOException {
		// Grow the file by at least 25% its previous size
		long percentCapacity = oldCapacity + (oldCapacity >> 2);
		if(percentCapacity>newCapacity) newCapacity = percentCapacity;
		// Align with page
		if((newCapacity&0xfff)!=0) newCapacity = (newCapacity & 0xfffffffffffff000L)+4096L;
		//System.out.println("DEBUG: newCapacity="+newCapacity);
		pbuffer.setCapacity(newCapacity);
	}

	/**
	 * This class takes a lazy approach on allocating buffer space.  It will allocate
	 * additional space as needed here, rounding up to the next 4096-byte boundary.
	 */
	@Override
	// @NotThreadSafe
	protected void ensureCapacity(long capacity) throws IOException {
		long curCapacity = pbuffer.capacity();
		if(curCapacity<capacity) expandCapacity(curCapacity, capacity);
	}

	/*
	// @NotThreadSafe
	public long getBlockCount() throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	// @NotThreadSafe
	public long getBlockId(long index) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}*/
}
