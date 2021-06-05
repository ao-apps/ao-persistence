/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2017, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.lang.io.IoUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
// import org.checkthread.annotations.NotThreadSafe;
// import org.checkthread.annotations.ThreadSafe;

/**
 * Serializes <code>Character</code> objects.
 * This class is not thread safe.
 *
 * @author  AO Industries, Inc.
 */
public class CharacterSerializer implements Serializer<Character> {

	// @ThreadSafe
	@Override
	public boolean isFixedSerializedSize() {
		return true;
	}

	// @NotThreadSafe
	@Override
	public long getSerializedSize(Character value) {
		return 2;
	}

	private final byte[] buffer = new byte[2];

	// @NotThreadSafe
	@Override
	public void serialize(Character value, OutputStream out) throws IOException {
		IoUtils.charToBuffer(value, buffer);
		out.write(buffer, 0, 2);
	}

	// @NotThreadSafe
	@Override
	public Character deserialize(InputStream in) throws IOException {
		IoUtils.readFully(in, buffer, 0, 2);
		return IoUtils.bufferToChar(buffer);
	}
}
