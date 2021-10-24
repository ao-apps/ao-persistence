/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2012, 2013, 2016, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.collections.AoArrays;
import com.aoapps.lang.io.FileUtils;
import com.aoapps.lang.io.IoUtils;
import com.aoapps.tempfiles.TempFileContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.NotImplementedException;
// import org.checkthread.annotations.NotThreadSafe;
// import org.checkthread.annotations.ThreadSafe;

/**
 * <p>
 * Java does not support write barriers without a complete <code>force</code> call,
 * this class works-around this issue by maintaining two copies of the file and
 * updating the older copy to be the newer copy occasionally on <code>barrier(false)</code>
 * and immediately on <code>barrier(true)</code> (if protectionLevel is high enough).
 * </p>
 * <p>
 * All instances also share a <code>{@link Timer Timer}</code> to perform automatic
 * background flushing of caches.  Automatic flushing is single-threaded to favor
 * low load averages over timely flushes.
 * </p>
 * <p>
 * This class also acts as a write cache that may batch or delay writes for potentially
 * long periods of time.  This is useful for media such as flash memory where write
 * throughput is quite limited and the number of writes on the media is also limited.
 * This also turns many random writes into fewer, sequential writes.  This may help
 * spinning platter-based media.
 * </p>
 * <p>
 * Since this class is intended for flash-based media, it will not rewrite the
 * same data to a section of a file.  Even if this means that it has to read the
 * section of the file, compare the values, and then write only when changed.  This
 * is an attempt to help the wear leveling technology built into the media by
 * dispatching a minimum number of writes.
 * </p>
 * <p>
 * An additional benefit may be that reads from cached data are performed directly
 * from the write cache, although this is not the purpose of this buffer.  Writes that
 * would not change anything, however, are not cached and would not help as a
 * read cache.
 * </p>
 * <p>
 * Two copies of the file are maintained.  The most recent version of the file
 * will normally be named with the regular name, but other versions will exist
 * with .old or .new appended to the filename.  The exact order of update is:
 * <ol>
 *   <li>Rename <code><i>filename</i>.old</code> to <code><i>filename</i>.new</code></li>
 *   <li>Write new version of all data to <code><i>filename</i>.new</code></li>
 *   <li>Rename <code><i>filename</i></code> to <code><i>filename</i>.old</code></li>
 *   <li>Rename <code><i>filename</i>.new</code> to <code><i>filename</i></code></li>
 * </ol>
 * <p>
 * The filename states, in order:
 * </p>
 * <pre>
 *          Complete      Complete Old  Partial
 * Normal:  filename      filename.old
 *          filename                    filename.new
 *          filename.new  filename.old
 * Normal:  filename      filename.old
 *
 * </pre>
 * <p>
 * This implementation assumes an atomic file rename for correct recovery.
 * </p>
 * <p>
 * To reduce the chance of data loss, this registers a JVM shutdown hook to flush
 * all caches on JVM shutdown.  If it takes a long time to flush the data this
 * can cause significant delays when stopping a JVM.
 * </p>
 * <p>
 * TODO: Should we run on top of RandomAccessBuffer/LargeMappedPersistentBuffer/MappedPersistentBuffer for memory-mapped read performance?
 * </p>
 *
 * @author  AO Industries, Inc.
 */
public class TwoCopyBarrierBuffer extends AbstractPersistentBuffer {

	/**
	 * The default number of bytes per sector.  This should match the block
	 * size of the underlying filesystem for best results.
	 */
	private static final int DEFAULT_SECTOR_SIZE = 4096;

	/**
	 * The default delay (in milliseconds) before committing changes in the background.  This errors
	 * on the side of safety with a short 5 second timeout.
	 */
	private static final long DEFAULT_ASYNCHRONOUS_COMMIT_DELAY = 5L * 1000L;

	/**
	 * The default delay (in milliseconds) before committing changes in the foreground.  This
	 * defaults to one minute.
	 */
	private static final long DEFAULT_SYNCHRONOUS_COMMIT_DELAY = 60L * 1000L;

	private static final Logger logger = Logger.getLogger(TwoCopyBarrierBuffer.class.getName());

	private static final Timer asynchronousCommitTimer = new Timer("TwoCopyBarrierBuffer.asynchronousCommitTimer");

	private static final Set<TwoCopyBarrierBuffer> shutdownBuffers = new HashSet<>();

	private static class FieldLock {}

	/**
	 * TODO: Is there a way we can combine the force calls between all buffers?
	 * 1) Use recursion to get lock on all individual buffers - or use newer locks
	 * 2) Write new versions of all files.
	 * 3) Perform a single sync (will this take as long as individual fsync's?)
	 * 4) Rename all files.
	 * Deadlock concerns?  Performance benefits?
	 *
	 * Could the background commit thread take a similar strategy?
	 */
	@SuppressWarnings({"UseSpecificCatch", "TooBroadCatch"})
	private static final Thread shutdownHook = new Thread("TwoCopyBarrierBuffer.shutdownHook") {
		@Override
		public void run() {
			// Get a snapshot of all buffers
			List<TwoCopyBarrierBuffer> toClose;
			synchronized(shutdownBuffers) {
				toClose = new ArrayList<>(shutdownBuffers);
				shutdownBuffers.clear();
			}
			if(!toClose.isEmpty()) {
				// These fields are shared by the invoked threads
				final FieldLock fieldLock = new FieldLock();
				final int[] counter = {1};
				final long[] startTime = {System.currentTimeMillis() - 55000};
				final boolean[] wrote = {false};
				final int size = toClose.size();
				// The maximum number of threads will be 100 or 1/20th of the number of buffers, whichever is larger
				int maxNumThreads = Math.max(100, size/20);
				int numThreads = Math.min(maxNumThreads, size);
				// TODO: This concurrency really didn't help much :(
				ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
				try {
					for(int c=0;c<size;c++) {
						final TwoCopyBarrierBuffer buffer = toClose.get(c);
						executorService.submit(() -> {
							synchronized(fieldLock) {
								long currentTime = System.currentTimeMillis();
								long timeSince = currentTime - startTime[0];
								if(timeSince<=-60000 || timeSince>=60000) {
									logger.info(size==1 ? "Closing the TwoCopyBarrierBuffer." : "Closing TwoCopyBarrierBuffer "+counter[0]+" of "+size+".");
									wrote[0] = true;
									startTime[0] = currentTime;
								}
								counter[0]++;
							}
							try {
								buffer.close();
							} catch(ThreadDeath td) {
								throw td;
							} catch(Throwable t) {
								logger.log(Level.WARNING, null, t);
							}
						});
					}
				} finally {
					executorService.shutdown();
					boolean terminated = false;
					while(!terminated) {
						try {
							terminated = executorService.awaitTermination(3600, TimeUnit.SECONDS);
						} catch(InterruptedException err) {
							logger.log(Level.WARNING, null, err);
						}
						if(!terminated) logger.info(size==1 ? "Waiting for the TwoCopyBarrierBuffer to close." : "Waiting for all "+size+" TwoCopyBarrierBuffers to close.");
					}
				}
				synchronized(fieldLock) {
					if(wrote[0]) logger.info(size==1 ? "Finished closing the TwoCopyBarrierBuffer." : "Finished closing all "+size+" TwoCopyBarrierBuffers.");
				}
			}
		}
	};
	static {
		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}

	private final TempFileContext tempFileContext;
	private final File file;
	private final File newFile;
	private final File oldFile;
	private final int sectorSize;
	private final long asynchronousCommitDelay;
	private final long synchronousCommitDelay;
	private static class CacheLock {}
	private final CacheLock cacheLock = new CacheLock();

	// All modifiable fields are protected by cacheLock

	/**
	 * <p>
	 * Keeps track of the last version of all sectors that have been written.  Each
	 * entry will be <code>sectorSize</code> in length, even if at the end of the
	 * capacity.
	 * </p>
	 * <p>
	 * Each copy of the file has its own write cache, which is flushed separately.
	 * This is done to avoid unnecessary reads while still keeping the files
	 * synchronized.  However, the actual <code>byte[]</code> of cached data
	 * is shared between the two maps.
	 * </p>
	 */
	private SortedMap<Long, byte[]>
		// Changes since the current (most up-to-date) file was last updated
		currentWriteCache = new TreeMap<>(),
		// Changes since the old (previous version) file was last updated.  This
		// is a superset of <code>currentWriteCache</code>.
		oldWriteCache = new TreeMap<>()
	;
	private long capacity; // The underlying storage is not extended until commit time.
	private RandomAccessFile raf; // Reads on non-cached data are read from here (this is the current file) - this is read-only
	private boolean isClosed = false;
	private long firstWriteTime = -1; // The time the first cached entry was written since the last commit
	private TimerTask asynchronousCommitTimerTask;

	/**
	 * Creates a read-write buffer backed by temporary files.  The protection level
	 * is set to <code>NONE</code>.  The temporary file will be deleted when this
	 * buffer is closed or on JVM shutdown.
	 * Uses default sectorSize of 4096, asynchronous commit delay of 5 seconds, and synchronous commit delay of 60 seconds.
	 * A shutdown hook is not registered.
	 */
	public TwoCopyBarrierBuffer() throws IOException {
		super(ProtectionLevel.NONE);
		tempFileContext = new TempFileContext();
		file = tempFileContext.createTempFile("TwoCopyBarrierBuffer_").getFile();
		String prefix = file.getName() + '_';
		newFile = tempFileContext.createTempFile(prefix, ".new").getFile();
		Files.delete(newFile.toPath());
		oldFile = tempFileContext.createTempFile(prefix, ".old").getFile();
		sectorSize = DEFAULT_SECTOR_SIZE;
		asynchronousCommitDelay = DEFAULT_ASYNCHRONOUS_COMMIT_DELAY;
		synchronousCommitDelay = DEFAULT_SYNCHRONOUS_COMMIT_DELAY;
		raf = new RandomAccessFile(file, "r");
	}

	/**
	 * Creates a read-write buffer with <code>BARRIER</code> protection level.
	 * Uses default sectorSize of 4096, asynchronous commit delay of 5 seconds, and synchronous commit delay of 60 seconds.
	 */
	public TwoCopyBarrierBuffer(String name) throws IOException {
		this(new File(name), ProtectionLevel.BARRIER, DEFAULT_SECTOR_SIZE, DEFAULT_ASYNCHRONOUS_COMMIT_DELAY, DEFAULT_SYNCHRONOUS_COMMIT_DELAY);
	}

	/**
	 * Creates a buffer.
	 * Uses default sectorSize of 4096, asynchronous commit delay of 5 seconds, and synchronous commit delay of 60 seconds.
	 */
	public TwoCopyBarrierBuffer(String name, ProtectionLevel protectionLevel) throws IOException {
		this(new File(name), protectionLevel, DEFAULT_SECTOR_SIZE, DEFAULT_ASYNCHRONOUS_COMMIT_DELAY, DEFAULT_SYNCHRONOUS_COMMIT_DELAY);
	}

	/**
	 * Creates a read-write buffer with <code>BARRIER</code> protection level.
	 * Uses default sectorSize of 4096, asynchronous commit delay of 5 seconds, and synchronous commit delay of 60 seconds.
	 */
	public TwoCopyBarrierBuffer(File file) throws IOException {
		this(file, ProtectionLevel.BARRIER, DEFAULT_SECTOR_SIZE, DEFAULT_ASYNCHRONOUS_COMMIT_DELAY, DEFAULT_SYNCHRONOUS_COMMIT_DELAY);
	}

	/**
	 * Creates a buffer.
	 * Uses default sectorSize of 4096, asynchronous commit delay of 5 seconds, and synchronous commit delay of 60 seconds.
	 */
	public TwoCopyBarrierBuffer(File file, ProtectionLevel protectionLevel) throws IOException {
		this(file, protectionLevel, DEFAULT_SECTOR_SIZE, DEFAULT_ASYNCHRONOUS_COMMIT_DELAY, DEFAULT_SYNCHRONOUS_COMMIT_DELAY);
	}

	/**
	 * Creates a buffer.  Synchronizes the two copies of the file.  Populates the <code>oldWriteCache</code> with
	 * any data the doesn't match the newer version of the file.  This means that both files are read completely
	 * at start-up in order to provide the most efficient synchronization later.
	 *
	 * @param file              The base filename, will be appended with ".old" and ".new" while committing changes.
	 * @param protectionLevel   The protection level for this buffer.
	 * @param sectorSize        The size of the sectors cached and written.  For best results this should
	 *                          match the underlying filesystem block size.  Must be a power of two &gt;= 1.
	 * @param asynchronousCommitDelay The number of milliseconds before a background thread syncs uncommitted data to
	 *                                the underlying storage.  A value of <code>Long.MAX_VALUE</code> will avoid any
	 *                                overhead of background thread management.
	 * @param synchronousCommitDelay  The number of milliseconds before a the calling thread syncs uncommitted data to
	 *                                the underlying storage.
	 */
	@SuppressWarnings("LeakingThisInConstructor")
	public TwoCopyBarrierBuffer(File file, ProtectionLevel protectionLevel, int sectorSize, long asynchronousCommitDelay, long synchronousCommitDelay) throws IOException {
		super(protectionLevel);
		if(Integer.bitCount(sectorSize)!=1) throw new IllegalArgumentException("sectorSize is not a power of two: "+sectorSize);
		if(sectorSize<1) throw new IllegalArgumentException("sectorSize<1: "+sectorSize);
		if(asynchronousCommitDelay<0) throw new IllegalArgumentException("asynchronousCommitDelay<0: "+asynchronousCommitDelay);
		if(synchronousCommitDelay<0) throw new IllegalArgumentException("synchronousCommitDelay<0: "+synchronousCommitDelay);
		this.tempFileContext = null;
		this.file = file;
		newFile = new File(file.getPath() + ".new");
		oldFile = new File(file.getPath() + ".old");
		this.sectorSize = sectorSize;
		this.asynchronousCommitDelay = asynchronousCommitDelay;
		this.synchronousCommitDelay = synchronousCommitDelay;
		// Find file(s) and rename if necessary
		// filename and filename.old
		if(file.exists()) {
			if(newFile.exists()) {
				if(oldFile.exists()) {
					throw new IOException("file, newFile, and oldFile all exist");
				} else {
					FileUtils.rename(newFile, oldFile);
				}
			} else {
				if(oldFile.exists()) {
					// This is the normal state
				} else {
					new FileOutputStream(oldFile).close();
				}
			}
		} else {
			if(newFile.exists()) {
				if(oldFile.exists()) {
					FileUtils.rename(newFile, file);
				} else {
					throw new IOException("newFile exists without either file or oldFile");
				}
			} else {
				if(oldFile.exists()) {
					throw new IOException("oldFile exists without either file or newFile");
				} else {
					new FileOutputStream(file).close();
					new FileOutputStream(oldFile).close();
				}
			}
		}
		// Populate oldWriteCache by comparing the files
		raf = new RandomAccessFile(file, "r");
		capacity = raf.length();
		long oldCapacity = oldFile.length();
		try (InputStream oldIn = new FileInputStream(oldFile)) {
			byte[] buff = new byte[sectorSize];
			byte[] oldBuff = new byte[sectorSize];
			for(long sector=0; sector<capacity; sector+=sectorSize) {
				long sectorEnd = sector+sectorSize;
				if(sectorEnd>capacity) sectorEnd = capacity;
				int inBytes = (int)(sectorEnd - sector);
				assert inBytes>0;
				// Read current bytes
				raf.readFully(buff, 0, inBytes);
				if(sectorEnd>oldCapacity) {
					// Old capacity too small, add to oldWriteCache if can't assume all zeros
					if(sector<oldCapacity || !AoArrays.allEquals(buff, 0, inBytes, (byte)0)) {
						if(inBytes<sectorSize) Arrays.fill(buff, sectorSize-inBytes, sectorSize, (byte)0);
						oldWriteCache.put(sector, buff);
						buff = new byte[sectorSize];
					}
				} else {
					// Read old bytes
					IoUtils.readFully(oldIn, oldBuff, 0, inBytes);
					if(!AoArrays.equals(buff, oldBuff, 0, inBytes)) {
						// Not equal, add to oldWriteCache
						if(inBytes<sectorSize) Arrays.fill(buff, sectorSize-inBytes, sectorSize, (byte)0);
						oldWriteCache.put(sector, buff);
						buff = new byte[sectorSize];
					}
				}
			}
		}
		synchronized(shutdownBuffers) {
			shutdownBuffers.add(this);
		}
	}

	/**
	 * Writes any modified data to the older copy of the file and makes it the new copy.
	 *
	 * @param isClosing  when <code>true</code>, will not reopen raf.
	 */
	// @NotThreadSafe
	private void flushWriteCache(boolean isClosing) throws IOException {
		assert Thread.holdsLock(cacheLock);
		if(!currentWriteCache.isEmpty()) {
			if(protectionLevel==ProtectionLevel.READ_ONLY) throw new IOException("protectionLevel==ProtectionLevel.READ_ONLY");
			FileUtils.rename(oldFile, newFile);
			try (RandomAccessFile newRaf = new RandomAccessFile(newFile, "rw")) {
				long oldLength = newRaf.length();
				if(capacity!=oldLength) {
					newRaf.setLength(capacity);
					if(capacity>oldLength) {
						// Ensure zero-filled
						PersistentCollections.ensureZeros(newRaf, oldLength, capacity - oldLength);
					}
				}
				// Write only those sectors that have been modified
				for(Map.Entry<Long, byte[]> entry : oldWriteCache.entrySet()) {
					long sector = entry.getKey();
					assert sector < capacity;
					assert (sector & (sectorSize-1))==0 : "Sector not aligned";
					long sectorEnd = sector + sectorSize;
					if(sectorEnd>capacity) sectorEnd = capacity;
					newRaf.seek(sector);
					newRaf.write(entry.getValue(), 0, (int)(sectorEnd - sector));
				}
				if(protectionLevel.compareTo(ProtectionLevel.BARRIER)>=0) newRaf.getChannel().force(false);
			}
			raf.close();
			FileUtils.rename(file, oldFile);
			// Swap caches
			oldWriteCache.clear();
			SortedMap<Long, byte[]> temp = currentWriteCache;
			currentWriteCache = oldWriteCache;
			oldWriteCache = temp;
			FileUtils.rename(newFile, file);
			if(!isClosing) raf = new RandomAccessFile(file, "r"); // Read-only during normal operation because using write caches
			clearFirstWriteTime();
		} else {
			if(isClosing) raf.close();
		}
	}

	// @ThreadSafe
	@Override
	public boolean isClosed() {
		synchronized(cacheLock) {
			return isClosed;
		}
	}

	// @ThreadSafe
	@Override
	public void close() throws IOException {
		synchronized(shutdownBuffers) {
			shutdownBuffers.remove(this);
		}
		synchronized(cacheLock) {
			flushWriteCache(true);
			isClosed = true;
			//raf.close(); // Now closed by flushWriteCache
			if(tempFileContext != null) tempFileContext.close();
		}
	}

	/**
	 * Checks if closed and throws IOException if so.
	 */
	// @NotThreadSafe
	private void checkClosed() throws IOException {
		assert Thread.holdsLock(cacheLock);
		if(isClosed) throw new IOException("TwoCopyBarrierBuffer(\""+file.getPath()+"\") is closed");
	}

	// @ThreadSafe
	@Override
	public long capacity() throws IOException {
		synchronized(cacheLock) {
			checkClosed();
			return capacity;
		}
	}

	/**
	 * Clears the starting time for the write cache.  Also cancels and removes the asynchronous timer, if exists.
	 */
	// @NotThreadSafe
	private void clearFirstWriteTime() {
		assert Thread.holdsLock(cacheLock);
		firstWriteTime = -1;
		if(asynchronousCommitTimerTask!=null) {
			asynchronousCommitTimerTask.cancel();
			asynchronousCommitTimerTask = null;
		}
	}

	/**
	 * Marks the starting time for the write cache.  Also starts the asynchronous timer, if not yet started.
	 */
	// @NotThreadSafe
	private void markFirstWriteTime() {
		assert Thread.holdsLock(cacheLock);
		// Mark as needing flush
		if(firstWriteTime==-1) firstWriteTime = System.currentTimeMillis();
		if(asynchronousCommitDelay!=Long.MAX_VALUE && asynchronousCommitTimerTask==null) {
			asynchronousCommitTimerTask = new TimerTask() {
				@Override
				public void run() {
					synchronized(cacheLock) {
						if(asynchronousCommitTimerTask==this) { // Ignore if canceled
							asynchronousCommitTimerTask = null;
							if(firstWriteTime!=-1) { // Nothing to write?
								// Only flush after asynchronousCommitDelay milliseconds have passed
								long timeSince = System.currentTimeMillis() - firstWriteTime;
								if(timeSince<=(-asynchronousCommitDelay) || timeSince>=asynchronousCommitDelay) {
									try {
										flushWriteCache(false);
									} catch(IOException err) {
										logger.log(Level.SEVERE, null, err);
									}
								} else {
									// Resubmit timer task
									markFirstWriteTime();
								}
							}
						}
					}
				}
			};
			long delay = (firstWriteTime + asynchronousCommitDelay) - System.currentTimeMillis();
			if(delay<0) delay = 0;
			asynchronousCommitTimer.schedule(asynchronousCommitTimerTask, delay);
		}
	}

	// @ThreadSafe
	@Override
	public void setCapacity(long newCapacity) throws IOException {
		synchronized(cacheLock) {
			checkClosed();
			if(newCapacity!=capacity) {
				Iterator<Map.Entry<Long, byte[]>> oldEntries = oldWriteCache.entrySet().iterator();
				while(oldEntries.hasNext()) {
					Map.Entry<Long, byte[]> entry = oldEntries.next();
					Long sector = entry.getKey();
					if(sector>=newCapacity) {
						// Remove any cached writes that start >= newCapacity
						oldEntries.remove();
						currentWriteCache.remove(sector);
					} else {
						long sectorEnd = sector+sectorSize;
						if(newCapacity>=sector && newCapacity<sectorEnd) {
							// Also, zero-out any part of the last sector (beyond newCapacity) if it is a cached write
							Arrays.fill(entry.getValue(), (int)(newCapacity-sector), sectorSize, (byte)0);
						}
					}
				}
				this.capacity = newCapacity;
				markFirstWriteTime();
			}
		}
	}

	// @ThreadSafe
	@Override
	public byte get(long position) throws IOException {
		synchronized(cacheLock) {
			checkClosed();
			if(position<0) throw new IllegalArgumentException("position<0: "+position);
			assert position<capacity;
			long sector = position&(-sectorSize);
			assert (sector&(sectorSize-1))==0 : "Sector not aligned";
			byte[] cached = oldWriteCache.get(sector);
			if(cached!=null) {
				return cached[(int)(position-sector)];
			} else {
				long rafLength = raf.length();
				if(position<rafLength) {
					raf.seek(position);
					return raf.readByte();
				} else {
					// Extended past end of raf, assume zeros that will be written during commit
					return (byte)0;
				}
			}
		}
	}

	// @ThreadSafe
	@Override
	public int getSome(long position, final byte[] buff, int off, int len) throws IOException {
		synchronized(cacheLock) {
			checkClosed();
			if(position<0) throw new IllegalArgumentException("position<0: "+position);
			if(off<0) throw new IllegalArgumentException("off<0: "+off);
			if(len<0) throw new IllegalArgumentException("len<0: "+len);
			final long end = position+len;
			assert end<=capacity;
			int bytesRead = 0;
			while(position<end) {
				long sector = position&(-sectorSize);
				assert (sector&(sectorSize-1))==0 : "Sector not aligned";
				int buffEnd = off + (sectorSize+(int)(sector-position));
				if(buffEnd>(off+len)) buffEnd = off+len;
				int bytesToRead = buffEnd-off;
				assert bytesToRead <= len;
				byte[] cached = oldWriteCache.get(sector);
				int count;
				if(cached!=null) {
					System.arraycopy(cached, (int)(position-sector), buff, off, bytesToRead);
					count = bytesToRead;
				} else {
					long rafLength = raf.length();
					if(position<rafLength) {
						raf.seek(position);
						count = raf.read(buff, off, bytesToRead);
						if(count==-1) throw new BufferUnderflowException();
					} else {
						// Extended past end of raf, assume zeros that will be written during commit
						Arrays.fill(buff, off, buffEnd, (byte)0);
						count = bytesToRead;
					}
				}
				bytesRead += count;
				if(count<bytesToRead) break;
				position += count;
				off += count;
				len -= count;
			}
			return bytesRead;
		}
	}

	@Override
	// @ThreadSafe
	public void put(long position, byte value) throws IOException {
		synchronized(cacheLock) {
			checkClosed();
			if(position<0) throw new IllegalArgumentException("position<0: "+position);
			assert position<capacity;
			final long sector = position&(-sectorSize);
			assert (sector&(sectorSize-1))==0 : "Sector not aligned";
			byte[] oldCached = oldWriteCache.get(sector);
			if(oldCached!=null) {
				if(currentWriteCache.containsKey(sector)) {
					// Already in current cache, update always because only dirty sectors are in the current cache.
					// Update cache only (do not write-through)
					oldCached[(int)(position-sector)] = value;
				} else {
					// Only add to current cache when data changed (save flash writes)
					if(oldCached[(int)(position-sector)] != value) {
						markFirstWriteTime();
						currentWriteCache.put(sector, oldCached); // Shares the byte[] buffer
						// Update cache only (do not write-through)
						oldCached[(int)(position-sector)] = value;
					}
				}
			} else {
				// Read the entire sector from underlying storage, assuming zeros past end of file
				long rafLength = raf.length();
				// Only add to caches when data changed (save flash writes)
				byte curValue;
				if(position<rafLength) {
					raf.seek(position);
					curValue = raf.readByte();
				} else {
					// Past end, assume zero
					curValue = (byte)0;
				}
				if(curValue!=value) {
					byte[] readBuff = new byte[sectorSize];
					// Scoping block
					{
						int offset = 0;
						int bytesLeft = sectorSize;
						while(bytesLeft>0) {
							long seek = sector+offset;
							if(seek<rafLength) {
								raf.seek(seek);
								long readEnd = seek+bytesLeft;
								if(readEnd>rafLength) readEnd = rafLength;
								int readLen = (int)(readEnd - seek);
								raf.readFully(readBuff, offset, readLen);
								offset+=readLen;
								bytesLeft-=readLen;
							} else {
								// Assume zeros that are already in readBuff
								offset+=bytesLeft;
								bytesLeft=0;
							}
						}
					}
					markFirstWriteTime();
					currentWriteCache.put(sector, readBuff); // Shares the byte[] buffer
					oldWriteCache.put(sector, readBuff); // Shares the byte[] buffer
					// Update cache only (do not write-through)
					readBuff[(int)(position-sector)] = value;
				}
			}
		}
	}

	@Override
	// @ThreadSafe
	public void ensureZeros(long position, long len) throws IOException {
		throw new NotImplementedException("TODO: Implement by using PersistentCollection.zero, passing to put sector aligned");
	}

	// @ThreadSafe
	@Override
	public void put(long position, byte[] buff, int off, int len) throws IOException {
		synchronized(cacheLock) {
			checkClosed();
			if(position<0) throw new IllegalArgumentException("position<0: "+position);
			if(off<0) throw new IllegalArgumentException("off<0: "+off);
			if(len<0) throw new IllegalArgumentException("len<0: "+len);
			long rafLength = -1;
			final long end = position+len;
			assert end<=capacity;
			byte[] readBuff = null;
			while(position<end) {
				final long sector = position&(-sectorSize);
				assert (sector&(sectorSize-1))==0 : "Sector not aligned";
				int buffEnd = off + (sectorSize+(int)(sector-position));
				if(buffEnd>(off+len)) buffEnd = off+len;
				int bytesToWrite = buffEnd-off;
				byte[] oldCached = oldWriteCache.get(sector);
				if(oldCached!=null) {
					if(currentWriteCache.containsKey(sector)) {
						// Already in current cache, update always because only dirty sectors are in the current cache.
						// Update cache only (do not write-through)
						System.arraycopy(buff, off, oldCached, (int)(position-sector), bytesToWrite);
					} else {
						// Only add to current cache when data changed (save flash writes)
						if(!AoArrays.equals(buff, off, oldCached, (int)(position-sector), bytesToWrite)) {
							markFirstWriteTime();
							currentWriteCache.put(sector, oldCached); // Shares the byte[] buffer
							// Update cache only (do not write-through)
							System.arraycopy(buff, off, oldCached, (int)(position-sector), bytesToWrite);
						}
					}
				} else {
					// Read the entire sector from underlying storage, assuming zeros past end of file
					if(rafLength==-1) rafLength = raf.length();
					boolean isNewBuff = readBuff==null;
					if(isNewBuff) readBuff = new byte[sectorSize];
					// Scoping block
					{
						int offset = 0;
						int bytesLeft = sectorSize;
						while(bytesLeft>0) {
							long seek = sector+offset;
							if(seek<rafLength) {
								raf.seek(seek);
								long readEnd = seek+bytesLeft;
								if(readEnd>rafLength) readEnd = rafLength;
								int readLen = (int)(readEnd - seek);
								raf.readFully(readBuff, offset, readLen);
								offset+=readLen;
								bytesLeft-=readLen;
							} else {
								// Assume zeros
								if(!isNewBuff) Arrays.fill(readBuff, offset, sectorSize, (byte)0);
								offset+=bytesLeft;
								bytesLeft=0;
							}
						}
					}
					// Only add to caches when data changed (save flash writes)
					if(!AoArrays.equals(buff, off, readBuff, (int)(position-sector), bytesToWrite)) {
						markFirstWriteTime();
						currentWriteCache.put(sector, readBuff); // Shares the byte[] buffer
						oldWriteCache.put(sector, readBuff); // Shares the byte[] buffer
						// Update cache only (do not write-through)
						System.arraycopy(buff, off, readBuff, (int)(position-sector), bytesToWrite);
						readBuff = null; // Create new array next time needed
					}
				}
				position += bytesToWrite;
				off += bytesToWrite;
				len -= bytesToWrite;
			}
		}
	}

	// @ThreadSafe
	@Override
	public void barrier(boolean force) throws IOException {
		synchronized(cacheLock) {
			checkClosed();
			// Downgrade to barrier-only if not using FORCE protection level
			if(force && protectionLevel.compareTo(ProtectionLevel.FORCE)>=0) {
				// Flush always when forced
				flushWriteCache(false);
			} else {
				// Only flush after synchronousCommitDelay milliseconds have passed
				if(firstWriteTime!=-1) {
					long timeSince = System.currentTimeMillis() - firstWriteTime;
					if(timeSince<=(-synchronousCommitDelay) || timeSince>=synchronousCommitDelay) flushWriteCache(false);
				}
			}
		}
	}
}
