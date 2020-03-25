/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2013, 2016, 2019, 2020  AO Industries, Inc.
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

import com.aoindustries.io.FileUtils;
import com.aoindustries.util.Sequence;
import com.aoindustries.util.UnsynchronizedSequence;
import com.aoindustries.exception.WrappedException;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import junit.framework.TestCase;

/**
 * Tests the <code>LinkedFileList</code> against the standard <code>LinkedList</code>
 * by performing equal, random actions on each and ensuring equal results.
 *
 * @author  AO Industries, Inc.
 */
abstract public class PersistentLinkedListTestParent extends TestCase {

	private static final int TEST_LOOPS = 10;

	public PersistentLinkedListTestParent(String testName) {
		super(testName);
	}

	private static String getRandomString(Random random, boolean allowNull) {
		int len;
		if(allowNull) {
			len = random.nextInt(130)-1;
			if(len==-1) return null;
		} else {
			len = random.nextInt(129);
		}
		StringBuilder SB = new StringBuilder(len);
		for(int d=0;d<len;d++) SB.append((char)random.nextInt(Character.MAX_VALUE+1));
		return SB.toString();
	}

	private final Random secureRandom = new SecureRandom();
	private final Random random = new Random(secureRandom.nextLong());

	protected abstract PersistentBuffer getPersistentBuffer(File tempFile, ProtectionLevel protectionLevel) throws Exception;

	private void doTestCorrectnessString(int numElements) throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), String.class);
		LinkedList<String> linkedList = new LinkedList<>();
		try {
			// Populate the list
			for(int c=0;c<numElements;c++) {
				String s = getRandomString(random, true);
				assertEquals(linkedFileList.add(s), linkedList.add(s));
			}
			// Check size match
			assertEquals(linkedFileList.size(), linkedList.size());
			if(numElements>0) {
				// Check first
				assertEquals(linkedFileList.getFirst(), linkedList.getFirst());
				// Check last
				assertEquals(linkedFileList.getLast(), linkedList.getLast());
				// Update random locations to random values
				for(int c=0;c<numElements;c++) {
					int index = random.nextInt(numElements);
					String newVal = getRandomString(random, true);
					assertEquals(linkedFileList.set(index, newVal), linkedList.set(index, newVal));
				}
			}
			// Check equality
			assertEquals(linkedFileList, linkedList);
			// Remove random indexes
			if(numElements>0) {
				int numRemove = random.nextInt(numElements);
				for(int c=0;c<numRemove;c++) {
					assertEquals(linkedFileList.size(), linkedList.size());
					int index = random.nextInt(linkedFileList.size());
					assertEquals(
						linkedFileList.remove(index),
						linkedList.remove(index)
					);
				}
			}
			// Add random values to end
			if(numElements>0) {
				int numAdd = random.nextInt(numElements);
				for(int c=0;c<numAdd;c++) {
					assertEquals(linkedFileList.size(), linkedList.size());
					String newVal = getRandomString(random, true);
					assertEquals(linkedFileList.add(newVal), linkedList.add(newVal));
				}
			}
			// Check equality
			assertEquals(linkedFileList, linkedList);
			// Add random values in middle
			if(numElements>0) {
				int numAdd = random.nextInt(numElements);
				for(int c=0;c<numAdd;c++) {
					assertEquals(linkedFileList.size(), linkedList.size());
					int index = random.nextInt(linkedFileList.size());
					String newVal = getRandomString(random, true);
					linkedFileList.add(index, newVal);
					linkedList.add(index, newVal);
					assertEquals(
						linkedFileList.remove(index),
						linkedList.remove(index)
					);
				}
			}
			// Check equality
			assertEquals(linkedFileList, linkedList);
			// Save and restore, checking matches
			linkedFileList.close();
			PersistentLinkedList<String> newFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.READ_ONLY), String.class);
			try {
				assertEquals(newFileList, linkedList);
			} finally {
				newFileList.close();
			}
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Tests for correctness comparing to standard LinkedList implementation.
	 */
	public void testCorrectnessString() throws Exception {
		doTestCorrectnessString(0);
		doTestCorrectnessString(1);
		for(int c=0; c<10; c++) doTestCorrectnessString(100 + random.nextInt(101));
	}

	private void doTestCorrectnessInteger(int numElements) throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), Integer.class);
		LinkedList<Integer> linkedList = new LinkedList<>();
		try {
			// Populate the list
			for(int c=0;c<numElements;c++) {
				Integer I = random.nextInt();
				assertEquals(linkedFileList.add(I), linkedList.add(I));
			}
			// Check size match
			assertEquals(linkedFileList.size(), linkedList.size());
			if(numElements>0) {
				// Check first
				assertEquals(linkedFileList.getFirst(), linkedList.getFirst());
				// Check last
				assertEquals(linkedFileList.getLast(), linkedList.getLast());
				// Update random locations to random values
				for(int c=0;c<numElements;c++) {
					int index = random.nextInt(numElements);
					Integer newVal = random.nextInt();
					assertEquals(linkedFileList.set(index, newVal), linkedList.set(index, newVal));
				}
			}
			// Check equality
			assertEquals(linkedFileList, linkedList);
			// Remove random indexes
			if(numElements>0) {
				int numRemove = random.nextInt(numElements);
				for(int c=0;c<numRemove;c++) {
					assertEquals(linkedFileList.size(), linkedList.size());
					int index = random.nextInt(linkedFileList.size());
					assertEquals(
						linkedFileList.remove(index),
						linkedList.remove(index)
					);
				}
			}
			// Add random values to end
			if(numElements>0) {
				int numAdd = random.nextInt(numElements);
				for(int c=0;c<numAdd;c++) {
					assertEquals(linkedFileList.size(), linkedList.size());
					Integer newVal = random.nextInt();
					assertEquals(linkedFileList.add(newVal), linkedList.add(newVal));
				}
			}
			// Check equality
			assertEquals(linkedFileList, linkedList);
			// Add random values in middle
			if(numElements>0) {
				int numAdd = random.nextInt(numElements);
				for(int c=0;c<numAdd;c++) {
					assertEquals(linkedFileList.size(), linkedList.size());
					int index = random.nextInt(linkedFileList.size());
					Integer newVal = random.nextInt();
					linkedFileList.add(index, newVal);
					linkedList.add(index, newVal);
					assertEquals(
						linkedFileList.remove(index),
						linkedList.remove(index)
					);
				}
			}
			// Check equality
			assertEquals(linkedFileList, linkedList);
			// Save and restore, checking matches
			linkedFileList.close();
			PersistentLinkedList<Integer> newFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.READ_ONLY), Integer.class);
			try {
				assertEquals(newFileList, linkedList);
			} finally {
				newFileList.close();
			}
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Tests for correctness comparing to standard LinkedList implementation.
	 */
	public void testCorrectnessInteger() throws Exception {
		doTestCorrectnessInteger(0);
		doTestCorrectnessInteger(1);
		for(int c=0; c<10; c++) doTestCorrectnessInteger(100 + random.nextInt(101));
	}

	/**
	 * Tests the time complexity by adding many elements and making sure the time stays near linear
	 */
	public void testAddRandomStrings() throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), String.class);
		try {
			// Add in groups of 1000, timing the add
			String[] toAdd = new String[1000];
			for(int d=0;d<1000;d++) toAdd[d] = getRandomString(random, true);
			for(int c=0;c<TEST_LOOPS;c++) {
				long startNanos = System.nanoTime();
				for(int d=0;d<1000;d++) linkedFileList.add(toAdd[d]);
				long endNanos = System.nanoTime();
				System.out.println((c+1)+" of "+TEST_LOOPS+": Added 1000 random strings in "+BigDecimal.valueOf((endNanos-startNanos)/1000, 3)+" ms");
			}
			// Calculate the mean and standard deviation, compare for linear
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Tests the time complexity by adding many elements and making sure the time stays near linear
	 */
	public void testAddRandomIntegers() throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), Integer.class);
		try {
			// Add in groups of 1000, timing the add
			Integer[] toAdd = new Integer[1000];
			for(int d=0;d<1000;d++) toAdd[d] = random.nextInt();
			for(int c=0;c<TEST_LOOPS;c++) {
				long startNanos = System.nanoTime();
				for(int d=0;d<1000;d++) linkedFileList.add(toAdd[d]);
				long endNanos = System.nanoTime();
				System.out.println((c+1)+" of "+TEST_LOOPS+": Added 1000 random integers in "+BigDecimal.valueOf((endNanos-startNanos)/1000, 3)+" ms");
			}
			// Calculate the mean and standard deviation, compare for linear
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Tests the time complexity for integers (all null to avoid serialization)
	 */
	public void testAddNull() throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), Integer.class);
		try {
			// Add in groups of 1000, timing the add
			for(int c=0;c<TEST_LOOPS;c++) {
				long startNanos = System.nanoTime();
				for(int d=0;d<1000;d++) linkedFileList.add(null);
				long endNanos = System.nanoTime();
				System.out.println((c+1)+" of "+TEST_LOOPS+": Added 1000 null Integer in "+BigDecimal.valueOf((endNanos-startNanos)/1000, 3)+" ms");
			}
			// Calculate the mean and standard deviation, compare for linear
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Test iteration performance.
	 */
	public void testStringIterationPerformance() throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), String.class);
		try {
			for(int c=0;c<=10;c++) {
				if(c>0) for(int d=0;d<1000;d++) linkedFileList.add(getRandomString(random, true));
				long startNanos = System.nanoTime();
				for(String value : linkedFileList) {
					// Do nothing
				}
				long endNanos = System.nanoTime();
				System.out.println("Iterated "+linkedFileList.size()+" random strings in "+BigDecimal.valueOf((endNanos-startNanos)/1000, 3)+" ms");
			}
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Test iteration performance.
	 */
	public void testIntegerIterationPerformance() throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), Integer.class);
		try {
			for(int c=0;c<=10;c++) {
				if(c>0) for(int d=0;d<1000;d++) linkedFileList.add(random.nextInt());
				long startNanos = System.nanoTime();
				for(Integer value : linkedFileList) {
					// Do nothing
				}
				long endNanos = System.nanoTime();
				System.out.println("Iterated "+linkedFileList.size()+" random integers in "+BigDecimal.valueOf((endNanos-startNanos)/1000, 3)+" ms");
			}
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Test circular list performance.
	 */
	public void testStringCircularListPerformance() throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), String.class);
		try {
			for(int c=0;c<100000;c++) {
				String newValue = getRandomString(random, true);
				String oldValue = null;
				if(linkedFileList.size()>=1000) oldValue = linkedFileList.removeLast();
				linkedFileList.addFirst(newValue);
			}
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Test circular list performance.
	 */
	public void testIntegerCircularListPerformance() throws Exception {
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, ProtectionLevel.NONE), Integer.class);
		try {
			for(int c=0;c<100000;c++) {
				Integer newValue = random.nextInt();
				Integer oldValue = null;
				if(linkedFileList.size()>=1000) oldValue = linkedFileList.removeLast();
				linkedFileList.addFirst(newValue);
			}
		} finally {
			linkedFileList.close();
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Ensures that the fileList is not corrupt after a simulated failure.
	 * <ol>
	 *   <li>The fileList must be the same size as heapList or one smaller (for incomplete add/remove).</li>
	 *   <li>
	 *     The fileList may only be missing one item that is in heapList,
	 *     and this is only acceptable when partial is not null and the missing item
	 *     equals partial.  When this is found, heapList is updated.  At this point
	 *     the two lists should precisely match.
	 *   </li>
	 *   <li>The fileList may not have any entry that is not in heapList.</li>
	 *   <li>The lists must be the same size.</li>
	 *   <li>The lists must iterate both forwards and backwards with identical elements.</li>
	 * </ol>
	 */
	private static void checkRecoveryConsistency(LinkedList<String> heapList, PersistentLinkedList<String> fileList, String partial) {
		// The fileList must be the same size as heapList or one smaller (for incomplete add).
		assertTrue("Size mismatch: heapList.size()="+heapList.size()+", fileList.size()="+fileList.size(), heapList.size()==fileList.size() || (heapList.size()-1)==fileList.size());
		Iterator<String> heapIter = heapList.iterator();
		Iterator<String> fileIter = fileList.iterator();
		boolean removedPartial = false;
		while(heapIter.hasNext() || fileIter.hasNext()) {
			// The persistent list may not have any entry that is not in linkedList.
			String heapValue = heapIter.hasNext() ? heapIter.next() : null;
			String fileValue = fileIter.hasNext() ? fileIter.next() : null;
			if(heapValue!=null) {
				//System.err.println("DEBUG: heapValue="+heapValue+", fileValue="+fileValue);
				if(!heapValue.equals(fileValue)) {
					assertTrue("Must be an exact match when partial is null: heapValue="+heapValue+", fileValue="+fileValue, partial!=null);
					if(fileValue!=null) assertTrue("Value found in fileList that is not found in heapList: "+fileValue, heapList.contains(fileValue));
					assertTrue("The only value that may be in the heap but not in the fileList is partial: partial="+partial+", fileValue="+fileValue, heapValue.equals(partial));
					assertFalse("Refusing to remove partial twice", removedPartial);
					heapIter.remove();
					heapValue = heapIter.hasNext() ? heapIter.next() : null;
					assertEquals(heapValue, fileValue);
					removedPartial = true;
				}
			} else {
				if(fileValue!=null) {
					// The fileList may not have any entry that is not in heapList.
					throw new AssertionError("Value found in fileList that is not found in heapList: "+fileValue);
				} else {
					throw new AssertionError("heapValue and fileValue should not both be null");
				}
			}
		}
		// The lists must be the same size.
		assertTrue("Post-correction size mismatch: heapList.size()="+heapList.size()+", fileList.size()="+fileList.size(), heapList.size()==fileList.size());
		// The lists must iterate forwards with identical elements.
		heapIter = heapList.iterator();
		fileIter = fileList.iterator();
		while(heapIter.hasNext()) {
			assertTrue("Forward iteration mismatch", fileIter.hasNext());
			assertEquals("Forward iteration mismatch", heapIter.next(), fileIter.next());
		}
		assertFalse("Forward iteration mismatch", fileIter.hasNext());
		// The lists must iterate backwards with identical elements.
		heapIter = heapList.descendingIterator();
		fileIter = fileList.descendingIterator();
		while(heapIter.hasNext()) {
			assertTrue("Backward iteration mismatch", fileIter.hasNext());
			assertEquals("Backward iteration mismatch", heapIter.next(), fileIter.next());
		}
		assertFalse("Backward iteration mismatch", fileIter.hasNext());
	}

	private void doTestFailureRecovery(ProtectionLevel protectionLevel) throws Exception {
		boolean allowFailure = true;
		Sequence sequence = new UnsynchronizedSequence();
		File tempFile = File.createTempFile("PersistentLinkedListTest", null);
		tempFile.deleteOnExit();
		try {
			LinkedList<String> heapList = new LinkedList<>();
			final int iterations = TEST_LOOPS;
			for(int c=0;c<iterations;c++) {
				long startNanos = System.nanoTime();
				// addFirst
				String partial = null;
				try {
					try {
						PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(new RandomFailBuffer(getPersistentBuffer(tempFile, protectionLevel), allowFailure), String.class);
						try {
							int batchSize = random.nextInt(100)+1;
							for(int d=0;d<batchSize;d++) {
								partial = Long.toString(sequence.getNextSequenceValue());
								heapList.addFirst(partial);
								linkedFileList.addFirst(partial);
								partial = null;
							}
						} finally {
							linkedFileList.close();
						}
					} catch(WrappedException err) {
						Throwable cause = err.getCause();
						if(cause!=null && (cause instanceof IOException)) throw (IOException)cause;
						throw err;
					}
				} catch(IOException err) {
					System.out.println(protectionLevel+": "+(c+1)+" of "+iterations+": addFirst: Caught failure: "+err.toString());
				}
				// Check consistency
				{
					PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, protectionLevel), String.class);
					try {
						checkRecoveryConsistency(heapList, linkedFileList, partial);
					} finally {
						linkedFileList.close();
					}
				}
				// removeLast
				partial = null;
				try {
					try {
						PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(new RandomFailBuffer(getPersistentBuffer(tempFile, protectionLevel), allowFailure), String.class);
						try {
							int batchSize = random.nextInt(95)+1;
							if(batchSize>linkedFileList.size()) batchSize = linkedFileList.size();
							for(int d=0;d<batchSize;d++) {
								partial = heapList.getLast();
								assertEquals(linkedFileList.getLast(), partial);
								assertEquals(linkedFileList.removeLast(), heapList.removeLast());
								partial = null;
							}
						} finally {
							linkedFileList.close();
						}
					} catch(WrappedException err) {
						Throwable cause = err.getCause();
						if(cause!=null && (cause instanceof IOException)) throw (IOException)cause;
						throw err;
					}
				} catch(IOException err) {
					System.out.println(protectionLevel+": "+(c+1)+" of "+iterations+": removeLast: Caught failure: "+err.toString());
				}
				// Check consistency
				{
					PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, protectionLevel), String.class);
					try {
						checkRecoveryConsistency(heapList, linkedFileList, partial);
					} finally {
						linkedFileList.close();
					}
				}
				// addLast
				partial = null;
				try {
					try {
						PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(new RandomFailBuffer(getPersistentBuffer(tempFile, protectionLevel), allowFailure), String.class);
						try {
							int batchSize = random.nextInt(100)+1;
							for(int d=0;d<batchSize;d++) {
								partial = Long.toString(sequence.getNextSequenceValue());
								heapList.addLast(partial);
								linkedFileList.addLast(partial);
								partial = null;
							}
						} finally {
							linkedFileList.close();
						}
					} catch(WrappedException err) {
						Throwable cause = err.getCause();
						if(cause!=null && (cause instanceof IOException)) throw (IOException)cause;
						throw err;
					}
				} catch(IOException err) {
					System.out.println(protectionLevel+": "+(c+1)+" of "+iterations+": addLast: Caught failure: "+err.toString());
				}
				// Check consistency
				{
					PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, protectionLevel), String.class);
					try {
						checkRecoveryConsistency(heapList, linkedFileList, partial);
					} finally {
						linkedFileList.close();
					}
				}
				// removeFirst
				partial = null;
				try {
					try {
						PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(new RandomFailBuffer(getPersistentBuffer(tempFile, protectionLevel), allowFailure), String.class);
						try {
							int batchSize = random.nextInt(95)+1;
							if(batchSize>linkedFileList.size()) batchSize = linkedFileList.size();
							for(int d=0;d<batchSize;d++) {
								// This assigned value *is* used in catch block - ignore NetBeans warning
								partial = linkedFileList.getFirst();
								assertEquals(linkedFileList.removeFirst(), heapList.removeFirst());
								partial = null;
							}
						} finally {
							linkedFileList.close();
						}
					} catch(WrappedException err) {
						Throwable cause = err.getCause();
						if(cause!=null && (cause instanceof IOException)) throw (IOException)cause;
						throw err;
					}
				} catch(IOException err) {
					System.out.println(protectionLevel+": "+(c+1)+" of "+iterations+": removeFirst: Caught failure: "+err.toString());
				}
				// Check consistency
				{
					PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile, protectionLevel), String.class);
					try {
						checkRecoveryConsistency(heapList, linkedFileList, partial);
					} finally {
						linkedFileList.close();
					}
				}
				// TODO: add random index
				// TODO: remove random index
				long endNanos = System.nanoTime();
				if((c%10)==9) System.out.println(protectionLevel+": "+(c+1)+" of "+iterations+": Tested block buffer failure recovery in "+BigDecimal.valueOf((endNanos-startNanos)/1000, 3)+" ms");
			}
		} finally {
			FileUtils.delete(tempFile);
			File newFile = new File(tempFile.getPath()+".new");
			if(newFile.exists()) FileUtils.delete(newFile);
			File oldFile = new File(tempFile.getPath()+".old");
			if(oldFile.exists()) FileUtils.delete(oldFile);
		}
	}

	/**
	 * Tests the durability of a persistent linked list when in barrier mode.
	 */
	public void testFailureRecoveryBarrier() throws Exception {
		doTestFailureRecovery(ProtectionLevel.BARRIER);
	}

	/**
	 * Tests the durability of a persistent linked list when in force mode.
	 */
	public void testFailureRecoveryForce() throws Exception {
		doTestFailureRecovery(ProtectionLevel.FORCE);
	}
}
