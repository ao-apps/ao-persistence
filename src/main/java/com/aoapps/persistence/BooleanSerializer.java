/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2020, 2021, 2022  AO Industries, Inc.
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serializes <code>Boolean</code> objects.
 *
 * @author  AO Industries, Inc.
 */
public class BooleanSerializer implements Serializer<Boolean> {

  @Override
  public boolean isFixedSerializedSize() {
    return true;
  }

  @Override
  public long getSerializedSize(Boolean value) {
    return 1;
  }

  @Override
  public void serialize(Boolean value, OutputStream out) throws IOException {
    out.write(value ? 1 : 0);
  }

  @Override
  public Boolean deserialize(InputStream in) throws IOException {
    int value = in.read();
    if (value == -1) {
      throw new EOFException();
    }
    return value != 0;
  }
}
