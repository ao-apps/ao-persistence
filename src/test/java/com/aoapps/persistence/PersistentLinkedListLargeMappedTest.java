/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2016, 2020, 2021, 2022  AO Industries, Inc.
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

import java.io.File;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests the <code>LinkedFileList</code> against the standard <code>LinkedList</code>
 * by performing equal, random actions on each and ensuring equal results.
 *
 * @author  AO Industries, Inc.
 */
public class PersistentLinkedListLargeMappedTest extends PersistentLinkedListTestParent {

  public static Test suite() {
    TestSuite suite = new TestSuite(PersistentLinkedListLargeMappedTest.class);
    return suite;
  }

  public PersistentLinkedListLargeMappedTest(String testName) {
    super(testName);
  }

  @Override
  protected PersistentBuffer getPersistentBuffer(File tempFile, ProtectionLevel protectionLevel) throws Exception {
    return new LargeMappedPersistentBuffer(tempFile, protectionLevel);
  }

  /**
   * Test larger files.
   */
  /*
  public void testLargeList() throws Exception {
    try (
      TempFileContext tempFileContext = new TempFileContext();
      TempFile tempFile = tempFileContext.createTempFile("LinkedFileListTest");
      PersistentLinkedList<byte[]> linkedFileList = new PersistentLinkedList<>(getPersistentBuffer(tempFile), false, false)
    ) {
      byte[] buff = new byte[1024 * 1024];
      System.out.println("Filling list");
      for (int c=0;c<1024;c++) {
        linkedFileList.add(buff);
      }
      System.out.println("Testing as circular list");
      for (int c=0;c<1000000;c++) {
        linkedFileList.removeLast();
        linkedFileList.addFirst(buff);
      }
    }
  }*/
}
