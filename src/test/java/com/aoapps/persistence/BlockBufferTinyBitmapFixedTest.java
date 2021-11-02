/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2013, 2016, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.tempfiles.TempFile;
import com.aoapps.tempfiles.TempFileContext;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author  AO Industries, Inc.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class BlockBufferTinyBitmapFixedTest extends BlockBufferTestParent {

	public static Test suite() {
		TestSuite suite = new TestSuite(BlockBufferTinyBitmapFixedTest.class);
		return suite;
	}

	public BlockBufferTinyBitmapFixedTest(String testName) {
		super(testName);
	}

	@Override
	public PersistentBuffer getBuffer(File tempFile, ProtectionLevel protectionLevel) throws IOException {
		return new MappedPersistentBuffer(tempFile, protectionLevel);
	}

	@Override
	public PersistentBlockBuffer getBlockBuffer(PersistentBuffer pbuffer) throws IOException {
		return new FixedPersistentBlockBuffer(pbuffer, 1);
	}

	@Override
	public long getAllocationSize() throws IOException {
		return fastRandom.nextBoolean() ? 1 : 0;
	}

	public void testAllocateOneMillion() throws Exception {
		try (
			TempFileContext tempFileContext = new TempFileContext();
			TempFile tempFile = tempFileContext.createTempFile("BlockBufferTinyBitmapFixedTest_");
			PersistentBlockBuffer blockBuffer = getBlockBuffer(getBuffer(tempFile.getFile(), ProtectionLevel.NONE))
		) {
			for(int c = 0; c < 1000000; c++) {
				blockBuffer.allocate(1);
			}
		}
	}

	public void testAllocateDeallocateOneMillion() throws Exception {
		try (
			TempFileContext tempFileContext = new TempFileContext();
			TempFile tempFile = tempFileContext.createTempFile("BlockBufferTinyBitmapFixedTest_");
			PersistentBlockBuffer blockBuffer = getBlockBuffer(getBuffer(tempFile.getFile(), ProtectionLevel.NONE))
		) {
			final int numAdd = 1000000;
			List<Long> ids = new ArrayList<>(numAdd);
			long startNanos = System.nanoTime();
			for(int c = 0; c < numAdd; c++) {
				ids.add(blockBuffer.allocate(1));
			}
			long endNanos = System.nanoTime();
			System.out.println("BlockBufferTinyBitmapFixedTest: testAllocateDeallocateOneMillion: Allocating "+numAdd+" blocks in "+BigDecimal.valueOf((endNanos-startNanos)/1000, 3)+" ms");
			//System.out.println("BlockBufferTinyBitmapFixedTest: testAllocateDeallocateOneMillion: Getting "+numAdd+" ids.");
			//Iterator<Long> iter = blockBuffer.iterateBlockIds();
			//int count = 0;
			//while(iter.hasNext()) {
			//    ids.add(iter.next());
			//    count++;
			//}
			//assertEquals(numAdd, count);
			long deallocCount = 0;
			long deallocTime = 0;
			long allocCount = 0;
			long allocTime = 0;
			for(int c=0;c<100;c++) {
				// Remove random items
				int numRemove = fastRandom.nextInt(Math.min(10000, ids.size()));
				List<Long> removeList = new ArrayList<>(numRemove);
				for(int d=0;d<numRemove;d++) {
					int index = fastRandom.nextInt(ids.size());
					removeList.add(ids.get(index));
					ids.set(index, ids.get(ids.size()-1));
					ids.remove(ids.size()-1);
				}
				//System.out.println("BlockBufferTinyBitmapFixedTest: testAllocateDeallocateOneMillion: Shuffling.");
				//Collections.shuffle(ids, fastRandom);
				startNanos = System.nanoTime();
				for(Long id : removeList) {
					blockBuffer.deallocate(id);
				}
				deallocCount += numRemove;
				deallocTime += System.nanoTime() - startNanos;
				int numAddBack = fastRandom.nextInt(10000);
				startNanos = System.nanoTime();
				for(int d = 0; d < numAddBack; d++) {
					ids.add(blockBuffer.allocate(1));
				}
				allocCount += numAddBack;
				allocTime += System.nanoTime() - startNanos;
			}
			System.out.println("BlockBufferTinyBitmapFixedTest: testAllocateDeallocateOneMillion: Deallocated "+deallocCount+" blocks in "+BigDecimal.valueOf(deallocTime/1000, 3)+" ms");
			System.out.println("BlockBufferTinyBitmapFixedTest: testAllocateDeallocateOneMillion: Allocated "+allocCount+" blocks in "+BigDecimal.valueOf(allocTime/1000, 3)+" ms");
		}
	}
}
