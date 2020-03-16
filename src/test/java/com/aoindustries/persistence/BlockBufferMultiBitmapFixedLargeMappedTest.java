/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2013, 2016, 2020  AO Industries, Inc.
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

import java.io.File;
import java.io.IOException;
import java.util.Random;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author  AO Industries, Inc.
 */
public class BlockBufferMultiBitmapFixedLargeMappedTest extends BlockBufferTestParent {

	public static Test suite() {
		TestSuite suite = new TestSuite(BlockBufferMultiBitmapFixedLargeMappedTest.class);
		return suite;
	}

	public BlockBufferMultiBitmapFixedLargeMappedTest(String testName) {
		super(testName);
	}

	@Override
	public PersistentBuffer getBuffer(File tempFile, ProtectionLevel protectionLevel) throws IOException {
		return new LargeMappedPersistentBuffer(tempFile, protectionLevel);
	}

	@Override
	public PersistentBlockBuffer getBlockBuffer(PersistentBuffer pbuffer) throws IOException {
		return new FixedPersistentBlockBuffer(pbuffer, 4096);
	}

	@Override
	public long getAllocationSize(Random random) throws IOException {
		return random.nextInt(4097);
	}
}
