/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Serializes any <code>Serializable</code> objects.
 * This class is not thread safe.
 *
 * @author  AO Industries, Inc.
 */
public class ObjectSerializer<E> extends BufferedSerializer<E> {

  private final Class<E> type;

  public ObjectSerializer(Class<E> type) {
    if (!Serializable.class.isAssignableFrom(type)) {
      throw new IllegalArgumentException("Class is not Serializable: "+type.getName());
    }
    this.type = type;
  }

  @Override
  protected void serialize(E value, ByteArrayOutputStream buffer) throws IOException {
    try (ObjectOutputStream oout = new ObjectOutputStream(buffer)) {
      oout.writeObject(value);
    }
  }

  @Override
  public E deserialize(InputStream in) throws IOException {
    try (ObjectInputStream oin = new ObjectInputStream(in)) {
      return type.cast(oin.readObject());
    } catch (ClassNotFoundException | ClassCastException err) {
      throw new IOException(err);
    }
  }
}
