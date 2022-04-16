/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2013, 2016, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
import com.aoapps.tempfiles.TempFile;
import com.aoapps.tempfiles.TempFileContext;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import junit.framework.TestCase;

/**
 * @author  AO Industries, Inc.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public abstract class BlockBufferTestParent extends TestCase {

	protected BlockBufferTestParent(String testName) {
		super(testName);
	}

	private static final int TEST_LOOPS = 2; // 20;
	private static final int TEST_REPORT_INTERVAL = 1; //5;

	/**
	 * A fast pseudo-random number generator for non-cryptographic purposes.
	 */
	static final Random fastRandom = new Random(IoUtils.bufferToLong(new SecureRandom().generateSeed(Long.BYTES)));

	public abstract PersistentBuffer getBuffer(File tempFile, ProtectionLevel protectionLevel) throws IOException;
	public abstract PersistentBlockBuffer getBlockBuffer(PersistentBuffer pbuffer) throws IOException;
	public abstract long getAllocationSize() throws IOException;

	public void testAllocateDeallocate() throws Exception {
		try (
			TempFileContext tempFileContext = new TempFileContext();
			TempFile tempFile = tempFileContext.createTempFile("BlockBufferTestParent_")
		) {
			try (PersistentBlockBuffer blockBuffer = getBlockBuffer(getBuffer(tempFile.getFile(), ProtectionLevel.NONE))) {
				Set<Long> allocatedIds = new HashSet<>();
				for(int c=0;c<TEST_LOOPS;c++) {
					if(((c+1)%TEST_REPORT_INTERVAL)==0) System.out.println(getClass()+": testAllocateDeallocate: Test loop "+(c+1)+" of " + TEST_LOOPS);
					// Allocate some blocks, must not return duplicate ids.
					for(int d = 0; d < 1000; d++) {
						long id = blockBuffer.allocate(getAllocationSize());
						assertTrue("Block id allocated twice: "+id, allocatedIds.add(id));
					}

					// Iterate the block ids.  Each must be allocated.  All allocated must
					// be returned once and only once.
					Set<Long> notReturnedIds = new HashSet<>(allocatedIds);
					Iterator<Long> iter = blockBuffer.iterateBlockIds();
					while(iter.hasNext()) {
						Long id = iter.next();
						assertTrue(notReturnedIds.remove(id));
					}
					assertTrue(notReturnedIds.isEmpty());

					// Randomly deallocate 900 of the entries
					List<Long> ids = new ArrayList<>(allocatedIds);
					Collections.shuffle(ids, fastRandom);
					for(int d=0;d<500;d++) {
						long id = ids.get(d);
						blockBuffer.deallocate(id);
						allocatedIds.remove(id);
					}
				}
			} finally {
				File newFile = new File(tempFile.getFile().getPath() + ".new");
				if(newFile.exists()) Files.delete(newFile.toPath());
				File oldFile = new File(tempFile.getFile().getPath() + ".old");
				if(oldFile.exists()) Files.delete(oldFile.toPath());
			}
		}
	}

	/**
	 * <ol>
	 *   <li>Everything in recoveredIds must be in allocatedIds, if not add to allocatedIds.</li>
	 *   <li>Everything in allocatedIds must be in either recoveredIds or partialIds, if in partialIds remove from allocatedIds.</li>
	 * </ol>
	 */
	private static void compareAllocatedIds(SortedSet<Long> allocatedIds, SortedSet<Long> recoveredIds, SortedSet<Long> partialIds) throws Exception {
		for(Long id : recoveredIds) {
			if(!allocatedIds.contains(id)) {
				//System.out.println("DEBUG: Adding "+id+" to allocatedIds");
				allocatedIds.add(id);
			}
			//assertTrue("allocatedIds.contains("+id+")==false", allocatedIds.contains(id));
		}
		Iterator<Long> ids = allocatedIds.iterator();
		while(ids.hasNext()) {
			Long id = ids.next();
			boolean recoveryContains = recoveredIds.contains(id);
			boolean partialContains = partialIds.contains(id);
			if(!recoveryContains && partialContains) {
				//System.out.println("DEBUG: Removing "+id+" from allocatedIds");
				ids.remove();
			}
			assertTrue("partialIds.size()="+partialIds.size()+", recoveredIds.contains("+id+")="+recoveryContains+", partialIds.contains("+id+")="+partialContains, recoveryContains || partialContains);
		}
	}

	/**
	 * <ol>
	 *   <li>Everything in allocatedIds must be in recoveredIds.</li>
	 *   <li>Everything in recoveredIds must be in allocatedIds or partialIds, if in partialIds add to allocatedIds.</li>
	 * </ol>
	 */
	private static void compareDeallocatedIds(SortedSet<Long> allocatedIds, SortedSet<Long> recoveredIds, SortedSet<Long> partialIds) throws Exception {
		for(Long id : allocatedIds) {
			assertTrue(recoveredIds.contains(id));
		}
		for(Long id : recoveredIds) {
			boolean allocatedContains = allocatedIds.contains(id);
			boolean partialContains = partialIds.contains(id);
			if(!allocatedContains && partialContains) {
				//System.out.println("DEBUG: Adding "+id+" to allocatedIds");
				allocatedIds.add(id);
			}
			assertTrue("partialIds.size()="+partialIds.size()+", allocatedIds.contains("+id+")="+allocatedContains+", partialIds.contains("+id+")="+partialContains, allocatedContains || partialContains);
		}
	}

	private void doTestFailureRecovery(ProtectionLevel protectionLevel) throws Exception {
		try (
			TempFileContext tempFileContext = new TempFileContext();
			TempFile tempFile = tempFileContext.createTempFile("BlockBufferTestParent_")
		) {
			try {
				SortedSet<Long> allocatedIds = new TreeSet<>();
				SortedSet<Long> partialIds = new TreeSet<>(); // The ids that are added in this batch
				final int iterations = TEST_LOOPS;
				for(int c=0;c<iterations;c++) {
					long startNanos = System.nanoTime();
					partialIds.clear();
					try {
						try {
							try (PersistentBlockBuffer failingBlockBuffer = getBlockBuffer(new RandomFailBuffer(getBuffer(tempFile.getFile(), protectionLevel), true))) {
								int batchSize = fastRandom.nextInt(100)+1;
								for(int d = 0; d < batchSize; d++) {
									long id = failingBlockBuffer.allocate(getAllocationSize());
									partialIds.add(id);
									allocatedIds.add(id);
								}
								failingBlockBuffer.barrier(false);
								partialIds.clear();
							}
						} catch(UncheckedIOException err) {
							IOException cause = err.getCause();
							if(cause != null) throw cause;
							throw err;
						}
					} catch(IOException err) {
						System.out.println(protectionLevel+": "+(c+1)+" of "+iterations+": Allocate: Caught failure: "+err.toString());
					}
					// With failure or not, the allocation should be consistent
					PersistentBlockBuffer recoveredBlockBuffer = getBlockBuffer(getBuffer(tempFile.getFile(), protectionLevel));
					try {
						SortedSet<Long> recoveredIds = new TreeSet<>();
						Iterator<Long> ids = recoveredBlockBuffer.iterateBlockIds();
						while(ids.hasNext()) {
							long recoveredId = ids.next();
							recoveredIds.add(recoveredId);
							//System.out.println("recoveredId="+recoveredId);
						}
						compareAllocatedIds(allocatedIds, recoveredIds, partialIds);
					} finally {
						recoveredBlockBuffer.close();
					}
					// Deallocate some with similar check as allocation above
					if(!allocatedIds.isEmpty()) {
						List<Long> randomizedIds = new ArrayList<>(allocatedIds);
						Collections.sort(randomizedIds);
						partialIds.clear();
						try {
							try {
								try (PersistentBlockBuffer failingBlockBuffer = getBlockBuffer(new RandomFailBuffer(getBuffer(tempFile.getFile(), protectionLevel), true))) {
									int batchSize = fastRandom.nextInt(50) + 1;
									if(batchSize>randomizedIds.size()) batchSize=randomizedIds.size();
									for(int d=0;d<batchSize;d++) {
										Long id = randomizedIds.get(d);
										assertTrue(partialIds.add(id));
										assertTrue(allocatedIds.remove(id));
										failingBlockBuffer.deallocate(id);
									}
									failingBlockBuffer.barrier(false);
									partialIds.clear();
								}
							} catch(UncheckedIOException err) {
								IOException cause = err.getCause();
								if(cause != null) throw cause;
								throw err;
							}
						} catch(IOException err) {
							System.out.println(protectionLevel+": "+(c+1)+" of "+iterations+": Deallocate: Caught failure: "+err.toString());
						}
					}
					// With failure or not, the allocation should be consistent
					recoveredBlockBuffer = getBlockBuffer(getBuffer(tempFile.getFile(), protectionLevel));
					try {
						SortedSet<Long> recoveredIds = new TreeSet<>();
						Iterator<Long> ids = recoveredBlockBuffer.iterateBlockIds();
						while(ids.hasNext()) {
							long recoveredId = ids.next();
							recoveredIds.add(recoveredId);
							//System.out.println("recoveredId="+recoveredId);
						}
						compareDeallocatedIds(allocatedIds, recoveredIds, partialIds);
					} finally {
						recoveredBlockBuffer.close();
					}

					long endNanos = System.nanoTime();
					if(((c+1)%TEST_REPORT_INTERVAL)==0) System.out.println(protectionLevel+": "+(c+1)+" of "+iterations+": Tested block buffer failure recovery in "+BigDecimal.valueOf((endNanos-startNanos)/1000, 3)+" ms");
				}
			} finally {
				File newFile = new File(tempFile.getFile().getPath() + ".new");
				if(newFile.exists()) Files.delete(newFile.toPath());
				File oldFile = new File(tempFile.getFile().getPath() + ".old");
				if(oldFile.exists()) Files.delete(oldFile.toPath());
			}
		}
	}

	public void testFailureRecoveryBarrier() throws Exception {
		doTestFailureRecovery(ProtectionLevel.BARRIER);
	}

	public void testFailureRecoveryForce() throws Exception {
		doTestFailureRecovery(ProtectionLevel.FORCE);
	}
}
