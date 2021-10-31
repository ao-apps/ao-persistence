/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2020, 2021  AO Industries, Inc.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
// import org.checkthread.annotations.NotThreadSafe;
// import org.checkthread.annotations.ThreadSafe;

/**
 * Writes and reads serialized forms of objects to and from <code>OutputStream</code> and <code>InputStreams</code>.
 * There is no need to handle <code>null</code> values as they will not be passed-in.
 * All <code>Serializers</code> should be considered not thread-safe.
 *
 * @author  AO Industries, Inc.
 */
public interface Serializer<E> {

	/**
	 * If a serializer always creates the same number of bytes, containers can
	 * choose a fixed-size block for higher performance.  If this method
	 * returns <code>true</code>, <code>getSerializedSize</code> must return
	 * the same value for every access, it may be accessed with a <code>null</code>
	 * parameter, and it may be accessed less than once per serialized object.
	 *
	 * @return  To indicate that the same number of bytes will be created, return
	 *          <code>true</code>.  Otherwise, there may be a dynamic number of
	 *          bytes and return <code>false</code>.
	 */
	// @ThreadSafe
	boolean isFixedSerializedSize();

	/**
	 * <p>
	 * Determines the size of the object after serialization.
	 * This allows some optimizations avoiding unnecessary copying of data.
	 * </p>
	 * The common pattern is:
	 * <ol>
	 *   <li>Get size from <code>getSerializedSize</code></li>
	 *   <li>Allocate appropriate space</li>
	 *   <li>Write serialized object with <code>serialize</code></li>
	 * </ol>
	 * It may be best to remember the most recently used object between calls
	 * to <code>getSerializedSize</code> and <code>serialize</code> when it can
	 * reduce processing time.
	 *
	 * @return  the exact number of bytes the object will take to serialize
	 */
	// @NotThreadSafe
	long getSerializedSize(E value) throws IOException;

	/**
	 * Writes the object to the <code>OutputStream</code>.  <code>null</code> will
	 * not be passed in.
	 */
	//// @NotThreadSafe
	void serialize(E value, OutputStream out) throws IOException;

	/**
	 * Restores an object from an <code>InputStream</code>.
	 */
	// @NotThreadSafe
	E deserialize(InputStream in) throws IOException;
}
