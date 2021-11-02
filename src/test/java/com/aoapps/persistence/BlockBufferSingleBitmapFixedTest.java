/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2013, 2016, 2020, 2021  AO Industries, Inc.
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
import java.io.IOException;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author  AO Industries, Inc.
 */
public class BlockBufferSingleBitmapFixedTest extends BlockBufferTestParent {

	public static Test suite() {
		TestSuite suite = new TestSuite(BlockBufferSingleBitmapFixedTest.class);
		return suite;
	}

	public BlockBufferSingleBitmapFixedTest(String testName) {
		super(testName);
	}

	@Override
	public PersistentBuffer getBuffer(File tempFile, ProtectionLevel protectionLevel) throws IOException {
		return new SparseBuffer(protectionLevel);
	}

	@Override
	public PersistentBlockBuffer getBlockBuffer(PersistentBuffer pbuffer) throws IOException {
		return new FixedPersistentBlockBuffer(pbuffer, (1L<<30));
	}

	@Override
	public long getAllocationSize() throws IOException {
		return fastRandom.nextInt((1 << 30) + 1);
	}

	/**
	 * This test is not compatible with non-persistent {@link SparseBuffer}
	 */
	@Override
	public void testFailureRecoveryBarrier() {
		// Skip test
	}

	/**
	 * This test is not compatible with non-persistent {@link SparseBuffer}
	 */
	@Override
	public void testFailureRecoveryForce() {
		// Skip test
	}
}
