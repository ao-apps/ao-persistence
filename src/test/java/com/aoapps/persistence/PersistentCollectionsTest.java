/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2012, 2016, 2017, 2020, 2021, 2022  AO Industries, Inc.
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
import java.io.IOException;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Random;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Tests the {@link PersistentCollections} class.
 *
 * @author  AO Industries, Inc.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class PersistentCollectionsTest extends TestCase {

  private static final int ITERATIONS = 1000;

  private static final int ENSURE_ZEROS_TEST_SIZE = 1 << 16 /*20*/;

  /**
   * A fast pseudo-random number generator for non-cryptographic purposes.
   */
  private static final Random fastRandom = new Random(IoUtils.bufferToLong(new SecureRandom().generateSeed(Long.BYTES)));

  public static Test suite() {
    TestSuite suite = new TestSuite(PersistentCollectionsTest.class);
    return suite;
  }

  /**
   * Creates new {@link PersistentCollections} test.
   */
  public PersistentCollectionsTest(String testName) {
    super(testName);
  }

  /**
   * Tests {@link PersistentCollections#getPersistentBuffer(long)}.
   */
  public void testGetPersistentBuffer() throws Exception {
    PersistentBuffer smallBuffer = PersistentCollections.getPersistentBuffer(1L << 20);
    smallBuffer.close();

    PersistentBuffer largeBuffer = PersistentCollections.getPersistentBuffer(Long.MAX_VALUE);
    largeBuffer.close();
  }

  /**
   * Tests {@link IoUtils#charToBuffer(char, byte[])}.
   */
  public void testCharToBuffer() throws Exception {
    byte[] buff = new byte[Character.BYTES];
    for (int i = 0; i < ITERATIONS; i++) {
      char value = (char) fastRandom.nextInt(Character.MAX_VALUE + 1);
      IoUtils.charToBuffer(value, buff);
      char result = IoUtils.bufferToChar(buff);
      assertEquals(value, result);
    }
  }

  /**
   * Tests {@link IoUtils#shortToBuffer(short, byte[])}.
   */
  public void testShortToBuffer() throws Exception {
    byte[] buff = new byte[Short.BYTES];
    for (int i = 0; i < ITERATIONS; i++) {
      short value = (short) (fastRandom.nextInt(1 << Short.SIZE) + Short.MIN_VALUE);
      IoUtils.shortToBuffer(value, buff);
      short result = IoUtils.bufferToShort(buff);
      assertEquals(value, result);
    }
  }

  /**
   * Tests {@link IoUtils#intToBuffer(int, byte[])}.
   */
  public void testIntToBuffer() throws Exception {
    byte[] buff = new byte[Integer.BYTES];
    for (int i = 0; i < ITERATIONS; i++) {
      int value = fastRandom.nextInt();
      IoUtils.intToBuffer(value, buff);
      int result = IoUtils.bufferToInt(buff);
      assertEquals(value, result);
    }
  }

  /**
   * Tests {@link IoUtils#longToBuffer(long, byte[])}.
   */
  public void testLongToBuffer() throws Exception {
    byte[] buff = new byte[Long.BYTES];
    for (int i = 0; i < ITERATIONS; i++) {
      long value = fastRandom.nextLong();
      IoUtils.longToBuffer(value, buff);
      long result = IoUtils.bufferToLong(buff);
      assertEquals(value, result);
    }
  }

  private static void doTestEnsureZeros(PersistentBuffer buffer) throws IOException {
    long totalNanos = 0;
    for (int c = 0; c < 100; c++) {
      // Update 1/8192 of buffer with random values
      for (int d = 0; d < (ENSURE_ZEROS_TEST_SIZE >>> 13); d++) {
        buffer.put(
            fastRandom.nextInt() & (ENSURE_ZEROS_TEST_SIZE - 1),
            (byte) fastRandom.nextInt()
        );
      }
      long startNanos = System.nanoTime();
      buffer.ensureZeros(0, ENSURE_ZEROS_TEST_SIZE);
      totalNanos += System.nanoTime() - startNanos;
    }
    System.out.println(buffer.getClass().getName() + ": ensureZeros in " + BigDecimal.valueOf(totalNanos, 6) + " ms");
  }

  /**
   * Tests all zeros.
   */
  public void testEnsureZeros() throws Exception {
    try (PersistentBuffer smallBuffer = PersistentCollections.getPersistentBuffer(ENSURE_ZEROS_TEST_SIZE)) {
      smallBuffer.setCapacity(ENSURE_ZEROS_TEST_SIZE);
      for (int i = 0; i < 10; i++) {
        doTestEnsureZeros(smallBuffer);
      }
    }

    try (PersistentBuffer largeBuffer = PersistentCollections.getPersistentBuffer(Long.MAX_VALUE)) {
      largeBuffer.setCapacity(ENSURE_ZEROS_TEST_SIZE);
      for (int i = 0; i < 10; i++) {
        doTestEnsureZeros(largeBuffer);
      }
    }
  }
}
