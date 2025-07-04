/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2013, 2016, 2019, 2020, 2021, 2022, 2024, 2025  AO Industries, Inc.
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
import com.aoapps.lang.util.Sequence;
import com.aoapps.lang.util.UnsynchronizedSequence;
import com.aoapps.tempfiles.TempFile;
import com.aoapps.tempfiles.TempFileContext;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import junit.framework.TestCase;

/**
 * Tests the {@link PersistentLinkedList} against the standard {@link LinkedList}
 * by performing equal, random actions on each and ensuring equal results.
 *
 * @author  AO Industries, Inc.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public abstract class PersistentLinkedListTestParent extends TestCase {

  private static final int TEST_LOOPS = 2; // 10;

  private static final int CIRCULAR_LIST_SIZE = 10000; // 100000
  /**
   * A fast pseudo-random number generator for non-cryptographic purposes.
   */
  private static final Random fastRandom = new Random(IoUtils.bufferToLong(new SecureRandom().generateSeed(Long.BYTES)));

  protected PersistentLinkedListTestParent(String testName) {
    super(testName);
  }

  private static String getRandomString(boolean allowNull) {
    int len;
    if (allowNull) {
      len = fastRandom.nextInt(130) - 1;
      if (len == -1) {
        return null;
      }
    } else {
      len = fastRandom.nextInt(129);
    }
    StringBuilder sb = new StringBuilder(len);
    for (int d = 0; d < len; d++) {
      sb.append((char) fastRandom.nextInt(Character.MAX_VALUE + 1));
    }
    return sb.toString();
  }

  protected abstract PersistentBuffer getPersistentBuffer(File tempFile, ProtectionLevel protectionLevel) throws Exception;

  private void doTestCorrectnessString(int numElements) throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try {
        LinkedList<String> linkedList = new LinkedList<>();
        try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), String.class)) {
          // Populate the list
          for (int c = 0; c < numElements; c++) {
            String s = getRandomString(true);
            assertEquals(linkedFileList.add(s), linkedList.add(s));
          }
          // Check size match
          assertEquals(linkedFileList.size(), linkedList.size());
          if (numElements > 0) {
            // Check first
            assertEquals(linkedFileList.getFirst(), linkedList.getFirst());
            // Check last
            assertEquals(linkedFileList.getLast(), linkedList.getLast());
            // Update random locations to random values
            for (int c = 0; c < numElements; c++) {
              int index = fastRandom.nextInt(numElements);
              String newVal = getRandomString(true);
              assertEquals(linkedFileList.set(index, newVal), linkedList.set(index, newVal));
            }
          }
          // Check equality
          assertEquals(linkedFileList, linkedList);
          // Remove random indexes
          if (numElements > 0) {
            int numRemove = fastRandom.nextInt(numElements);
            for (int c = 0; c < numRemove; c++) {
              assertEquals(linkedFileList.size(), linkedList.size());
              int index = fastRandom.nextInt(linkedFileList.size());
              assertEquals(
                  linkedFileList.remove(index),
                  linkedList.remove(index)
              );
            }
          }
          // Add random values to end
          if (numElements > 0) {
            int numAdd = fastRandom.nextInt(numElements);
            for (int c = 0; c < numAdd; c++) {
              assertEquals(linkedFileList.size(), linkedList.size());
              String newVal = getRandomString(true);
              assertEquals(linkedFileList.add(newVal), linkedList.add(newVal));
            }
          }
          // Check equality
          assertEquals(linkedFileList, linkedList);
          // Add random values in middle
          if (numElements > 0) {
            int numAdd = fastRandom.nextInt(numElements);
            for (int c = 0; c < numAdd; c++) {
              assertEquals(linkedFileList.size(), linkedList.size());
              int index = fastRandom.nextInt(linkedFileList.size());
              String newVal = getRandomString(true);
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
        }
        // Save and restore, checking matches
        try (PersistentLinkedList<String> newFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.READ_ONLY), String.class)) {
          assertEquals(newFileList, linkedList);
        }
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
    }
  }

  /**
   * Tests for correctness comparing to standard LinkedList implementation.
   */
  public void testCorrectnessString() throws Exception {
    doTestCorrectnessString(0);
    doTestCorrectnessString(1);
    for (int c = 0; c < 10; c++) {
      doTestCorrectnessString(100 + fastRandom.nextInt(101));
    }
  }

  private void doTestCorrectnessInteger(int numElements) throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try {
        LinkedList<Integer> linkedList = new LinkedList<>();
        try (PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), Integer.class)) {
          // Populate the list
          for (int c = 0; c < numElements; c++) {
            Integer i = fastRandom.nextInt();
            assertEquals(linkedFileList.add(i), linkedList.add(i));
          }
          // Check size match
          assertEquals(linkedFileList.size(), linkedList.size());
          if (numElements > 0) {
            // Check first
            assertEquals(linkedFileList.getFirst(), linkedList.getFirst());
            // Check last
            assertEquals(linkedFileList.getLast(), linkedList.getLast());
            // Update random locations to random values
            for (int c = 0; c < numElements; c++) {
              int index = fastRandom.nextInt(numElements);
              Integer newVal = fastRandom.nextInt();
              assertEquals(linkedFileList.set(index, newVal), linkedList.set(index, newVal));
            }
          }
          // Check equality
          assertEquals(linkedFileList, linkedList);
          // Remove random indexes
          if (numElements > 0) {
            int numRemove = fastRandom.nextInt(numElements);
            for (int c = 0; c < numRemove; c++) {
              assertEquals(linkedFileList.size(), linkedList.size());
              int index = fastRandom.nextInt(linkedFileList.size());
              assertEquals(
                  linkedFileList.remove(index),
                  linkedList.remove(index)
              );
            }
          }
          // Add random values to end
          if (numElements > 0) {
            int numAdd = fastRandom.nextInt(numElements);
            for (int c = 0; c < numAdd; c++) {
              assertEquals(linkedFileList.size(), linkedList.size());
              Integer newVal = fastRandom.nextInt();
              assertEquals(linkedFileList.add(newVal), linkedList.add(newVal));
            }
          }
          // Check equality
          assertEquals(linkedFileList, linkedList);
          // Add random values in middle
          if (numElements > 0) {
            int numAdd = fastRandom.nextInt(numElements);
            for (int c = 0; c < numAdd; c++) {
              assertEquals(linkedFileList.size(), linkedList.size());
              int index = fastRandom.nextInt(linkedFileList.size());
              Integer newVal = fastRandom.nextInt();
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
        }
        // Save and restore, checking matches
        try (PersistentLinkedList<Integer> newFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.READ_ONLY), Integer.class)) {
          assertEquals(newFileList, linkedList);
        }
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
    }
  }

  /**
   * Tests for correctness comparing to standard LinkedList implementation.
   */
  public void testCorrectnessInteger() throws Exception {
    doTestCorrectnessInteger(0);
    doTestCorrectnessInteger(1);
    for (int c = 0; c < 10; c++) {
      doTestCorrectnessInteger(100 + fastRandom.nextInt(101));
    }
  }

  /**
   * Tests the time complexity by adding many elements and making sure the time stays near linear.
   */
  public void testAddRandomStrings() throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), String.class)) {
        // Add in groups of 1000, timing the add
        String[] toAdd = new String[1000];
        for (int d = 0; d < 1000; d++) {
          toAdd[d] = getRandomString(true);
        }
        for (int c = 0; c < TEST_LOOPS; c++) {
          long startNanos = System.nanoTime();
          for (int d = 0; d < 1000; d++) {
            linkedFileList.add(toAdd[d]);
          }
          long endNanos = System.nanoTime();
          System.out.println((c + 1) + " of " + TEST_LOOPS + ": Added 1000 random strings in " + BigDecimal.valueOf((endNanos - startNanos) / 1000, 3) + " ms");
        }
        // Calculate the mean and standard deviation, compare for linear
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
    }
  }

  /**
   * Tests the time complexity by adding many elements and making sure the time stays near linear.
   */
  public void testAddRandomIntegers() throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try (PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), Integer.class)) {
        // Add in groups of 1000, timing the add
        Integer[] toAdd = new Integer[1000];
        for (int d = 0; d < 1000; d++) {
          toAdd[d] = fastRandom.nextInt();
        }
        for (int c = 0; c < TEST_LOOPS; c++) {
          long startNanos = System.nanoTime();
          for (int d = 0; d < 1000; d++) {
            linkedFileList.add(toAdd[d]);
          }
          long endNanos = System.nanoTime();
          System.out.println((c + 1) + " of " + TEST_LOOPS + ": Added 1000 random integers in " + BigDecimal.valueOf((endNanos - startNanos) / 1000, 3) + " ms");
        }
        // Calculate the mean and standard deviation, compare for linear
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
    }
  }

  /**
   * Tests the time complexity for integers (all null to avoid serialization).
   */
  public void testAddNull() throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try (PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), Integer.class)) {
        // Add in groups of 1000, timing the add
        for (int c = 0; c < TEST_LOOPS; c++) {
          long startNanos = System.nanoTime();
          for (int d = 0; d < 1000; d++) {
            linkedFileList.add(null);
          }
          long endNanos = System.nanoTime();
          System.out.println((c + 1) + " of " + TEST_LOOPS + ": Added 1000 null Integer in " + BigDecimal.valueOf((endNanos - startNanos) / 1000, 3) + " ms");
        }
        // Calculate the mean and standard deviation, compare for linear
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
    }
  }

  /**
   * Test iteration performance.
   */
  public void testStringIterationPerformance() throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), String.class)) {
        for (int c = 0; c <= 10; c++) {
          if (c > 0) {
            for (int d = 0; d < 1000; d++) {
              linkedFileList.add(getRandomString(true));
            }
          }
          long startNanos = System.nanoTime();
          for (String value : linkedFileList) {
            // Do nothing
          }
          long endNanos = System.nanoTime();
          System.out.println("Iterated " + linkedFileList.size() + " random strings in " + BigDecimal.valueOf((endNanos - startNanos) / 1000, 3) + " ms");
        }
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
    }
  }

  /**
   * Test iteration performance.
   */
  public void testIntegerIterationPerformance() throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try (PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), Integer.class)) {
        for (int c = 0; c <= 10; c++) {
          if (c > 0) {
            for (int d = 0; d < 1000; d++) {
              linkedFileList.add(fastRandom.nextInt());
            }
          }
          long startNanos = System.nanoTime();
          for (Integer value : linkedFileList) {
            // Do nothing
          }
          long endNanos = System.nanoTime();
          System.out.println("Iterated " + linkedFileList.size() + " random integers in " + BigDecimal.valueOf((endNanos - startNanos) / 1000, 3) + " ms");
        }
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
    }
  }

  /**
   * Test circular list performance.
   */
  public void testStringCircularListPerformance() throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), String.class)) {
        for (int c = 0; c < CIRCULAR_LIST_SIZE; c++) {
          String newValue = getRandomString(true);
          String oldValue = null;
          if (linkedFileList.size() >= 1000) {
            oldValue = linkedFileList.removeLast();
          }
          linkedFileList.addFirst(newValue);
        }
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
    }
  }

  /**
   * Test circular list performance.
   */
  public void testIntegerCircularListPerformance() throws Exception {
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try (PersistentLinkedList<Integer> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), ProtectionLevel.NONE), Integer.class)) {
        for (int c = 0; c < CIRCULAR_LIST_SIZE; c++) {
          Integer newValue = fastRandom.nextInt();
          Integer oldValue = null;
          if (linkedFileList.size() >= 1000) {
            oldValue = linkedFileList.removeLast();
          }
          linkedFileList.addFirst(newValue);
        }
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
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
    assertTrue("Size mismatch: heapList.size()=" + heapList.size() + ", fileList.size()=" + fileList.size(), heapList.size() == fileList.size() || (heapList.size() - 1) == fileList.size());
    Iterator<String> heapIter = heapList.iterator();
    Iterator<String> fileIter = fileList.iterator();
    boolean removedPartial = false;
    while (heapIter.hasNext() || fileIter.hasNext()) {
      // The persistent list may not have any entry that is not in linkedList.
      String heapValue = heapIter.hasNext() ? heapIter.next() : null;
      String fileValue = fileIter.hasNext() ? fileIter.next() : null;
      if (heapValue != null) {
        //System.err.println("DEBUG: heapValue="+heapValue+", fileValue="+fileValue);
        if (!heapValue.equals(fileValue)) {
          assertTrue("Must be an exact match when partial is null: heapValue=" + heapValue + ", fileValue=" + fileValue, partial != null);
          if (fileValue != null) {
            assertTrue("Value found in fileList that is not found in heapList: " + fileValue, heapList.contains(fileValue));
          }
          assertTrue("The only value that may be in the heap but not in the fileList is partial: partial=" + partial + ", fileValue=" + fileValue, heapValue.equals(partial));
          assertFalse("Refusing to remove partial twice", removedPartial);
          heapIter.remove();
          heapValue = heapIter.hasNext() ? heapIter.next() : null;
          assertEquals(heapValue, fileValue);
          removedPartial = true;
        }
      } else {
        if (fileValue != null) {
          // The fileList may not have any entry that is not in heapList.
          throw new AssertionError("Value found in fileList that is not found in heapList: " + fileValue);
        } else {
          throw new AssertionError("heapValue and fileValue should not both be null");
        }
      }
    }
    // The lists must be the same size.
    assertTrue("Post-correction size mismatch: heapList.size()=" + heapList.size() + ", fileList.size()=" + fileList.size(), heapList.size() == fileList.size());
    // The lists must iterate forwards with identical elements.
    heapIter = heapList.iterator();
    fileIter = fileList.iterator();
    while (heapIter.hasNext()) {
      assertTrue("Forward iteration mismatch", fileIter.hasNext());
      assertEquals("Forward iteration mismatch", heapIter.next(), fileIter.next());
    }
    assertFalse("Forward iteration mismatch", fileIter.hasNext());
    // The lists must iterate backwards with identical elements.
    heapIter = heapList.descendingIterator();
    fileIter = fileList.descendingIterator();
    while (heapIter.hasNext()) {
      assertTrue("Backward iteration mismatch", fileIter.hasNext());
      assertEquals("Backward iteration mismatch", heapIter.next(), fileIter.next());
    }
    assertFalse("Backward iteration mismatch", fileIter.hasNext());
  }

  @SuppressWarnings("UnusedAssignment")
  private void doTestFailureRecovery(ProtectionLevel protectionLevel) throws Exception {
    boolean allowFailure = true;
    Sequence sequence = new UnsynchronizedSequence();
    try (
        TempFileContext tempFileContext = new TempFileContext();
        TempFile tempFile = tempFileContext.createTempFile("PersistentLinkedListTest_")
        ) {
      try {
        LinkedList<String> heapList = new LinkedList<>();
        final int iterations = TEST_LOOPS;
        for (int c = 0; c < iterations; c++) {
          final long startNanos = System.nanoTime();
          // addFirst
          String partial = null;
          try {
            try {
              try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(
                  new RandomFailBuffer(getPersistentBuffer(tempFile.getFile(), protectionLevel), allowFailure), String.class)) {
                int batchSize = fastRandom.nextInt(100) + 1;
                for (int d = 0; d < batchSize; d++) {
                  partial = Long.toString(sequence.getNextSequenceValue());
                  heapList.addFirst(partial);
                  linkedFileList.addFirst(partial);
                  partial = null;
                }
              }
            } catch (UncheckedIOException err) {
              IOException cause = err.getCause();
              if (cause != null) {
                throw cause;
              }
              throw err;
            }
          } catch (IOException err) {
            System.out.println(protectionLevel + ": " + (c + 1) + " of " + iterations + ": addFirst: Caught failure: " + err.toString());
          }
          // Check consistency
          {
            try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), protectionLevel), String.class)) {
              checkRecoveryConsistency(heapList, linkedFileList, partial);
            }
          }
          // removeLast
          partial = null;
          try {
            try {
              try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(
                  new RandomFailBuffer(getPersistentBuffer(tempFile.getFile(), protectionLevel), allowFailure), String.class)) {
                int batchSize = fastRandom.nextInt(95) + 1;
                if (batchSize > linkedFileList.size()) {
                  batchSize = linkedFileList.size();
                }
                for (int d = 0; d < batchSize; d++) {
                  partial = heapList.getLast();
                  assertEquals(linkedFileList.getLast(), partial);
                  assertEquals(linkedFileList.removeLast(), heapList.removeLast());
                  partial = null;
                }
              }
            } catch (UncheckedIOException err) {
              IOException cause = err.getCause();
              if (cause != null) {
                throw cause;
              }
              throw err;
            }
          } catch (IOException err) {
            System.out.println(protectionLevel + ": " + (c + 1) + " of " + iterations + ": removeLast: Caught failure: " + err.toString());
          }
          // Check consistency
          try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), protectionLevel), String.class)) {
            checkRecoveryConsistency(heapList, linkedFileList, partial);
          }
          // addLast
          partial = null;
          try {
            try {
              try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(
                  new RandomFailBuffer(getPersistentBuffer(tempFile.getFile(), protectionLevel), allowFailure), String.class)) {
                int batchSize = fastRandom.nextInt(100) + 1;
                for (int d = 0; d < batchSize; d++) {
                  partial = Long.toString(sequence.getNextSequenceValue());
                  heapList.addLast(partial);
                  linkedFileList.addLast(partial);
                  partial = null;
                }
              }
            } catch (UncheckedIOException err) {
              IOException cause = err.getCause();
              if (cause != null) {
                throw cause;
              }
              throw err;
            }
          } catch (IOException err) {
            System.out.println(protectionLevel + ": " + (c + 1) + " of " + iterations + ": addLast: Caught failure: " + err.toString());
          }
          // Check consistency
          try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), protectionLevel), String.class)) {
            checkRecoveryConsistency(heapList, linkedFileList, partial);
          }
          // removeFirst
          partial = null;
          try {
            try {
              try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(
                  new RandomFailBuffer(getPersistentBuffer(tempFile.getFile(), protectionLevel), allowFailure), String.class)) {
                int batchSize = fastRandom.nextInt(95) + 1;
                if (batchSize > linkedFileList.size()) {
                  batchSize = linkedFileList.size();
                }
                for (int d = 0; d < batchSize; d++) {
                  // This assigned value *is* used in catch block - ignore NetBeans warning
                  partial = linkedFileList.getFirst(); // Removing this line causes tests to fail
                  assertEquals(linkedFileList.removeFirst(), heapList.removeFirst());
                  partial = null;
                }
              }
            } catch (UncheckedIOException err) {
              IOException cause = err.getCause();
              if (cause != null) {
                throw cause;
              }
              throw err;
            }
          } catch (IOException err) {
            System.out.println(protectionLevel + ": " + (c + 1) + " of " + iterations + ": removeFirst: Caught failure: " + err.toString());
          }
          // Check consistency
          try (PersistentLinkedList<String> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile.getFile(), protectionLevel), String.class)) {
            checkRecoveryConsistency(heapList, linkedFileList, partial);
          }
          // TODO: add random index
          // TODO: remove random index
          long endNanos = System.nanoTime();
          if ((c % 10) == 9) {
            System.out.println(protectionLevel + ": " + (c + 1) + " of " + iterations + ": Tested block buffer failure recovery in " + BigDecimal.valueOf((endNanos - startNanos) / 1000, 3) + " ms");
          }
        }
      } finally {
        File newFile = new File(tempFile.getFile().getPath() + ".new");
        if (newFile.exists()) {
          Files.delete(newFile.toPath());
        }
        File oldFile = new File(tempFile.getFile().getPath() + ".old");
        if (oldFile.exists()) {
          Files.delete(oldFile.toPath());
        }
      }
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
