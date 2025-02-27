/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2020, 2021, 2022, 2024  AO Industries, Inc.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Serializes any objects by using a buffer between the {@link Serializer#getSerializedSize(java.lang.Object)} and
 * {@link Serializer#serialize(java.lang.Object, java.io.OutputStream)} calls.  This avoids serializing the object twice
 * in the common sequence of getSerializedSize followed by serialize.  This and all subclasses are not fixed size.
 *
 * <p>This class is not thread safe.</p>
 *
 * @author  AO Industries, Inc.
 */
public abstract class BufferedSerializer<E> implements Serializer<E> {

  private E lastSerialized = null;
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

  protected BufferedSerializer() {
    // Do nothing
  }

  private void serializeToBuffer(E value) throws IOException {
    if (lastSerialized != value) {
      lastSerialized = null;
      buffer.reset();
      serialize(value, buffer);
      lastSerialized = value;
    }
  }

  @Override
  public final boolean isFixedSerializedSize() {
    return false;
  }

  @Override
  public final long getSerializedSize(E value) throws IOException {
    serializeToBuffer(value);
    return buffer.size();
  }

  @Override
  public final void serialize(E value, OutputStream out) throws IOException {
    serializeToBuffer(value);
    buffer.writeTo(out);
  }

  protected abstract void serialize(E value, ByteArrayOutputStream buffer) throws IOException;
}
