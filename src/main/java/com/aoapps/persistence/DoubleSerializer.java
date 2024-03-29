/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2016, 2017, 2020, 2021, 2022  AO Industries, Inc.
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
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serializes {@link Double} objects.
 * This class is not thread safe.
 *
 * @author  AO Industries, Inc.
 */
public class DoubleSerializer implements Serializer<Double> {

  @Override
  public boolean isFixedSerializedSize() {
    return true;
  }

  @Override
  public long getSerializedSize(Double value) {
    return Double.BYTES;
  }

  private final byte[] buffer = new byte[Double.BYTES];

  @Override
  public void serialize(Double value, OutputStream out) throws IOException {
    IoUtils.longToBuffer(Double.doubleToRawLongBits(value), buffer);
    out.write(buffer, 0, Double.BYTES);
  }

  @Override
  public Double deserialize(InputStream in) throws IOException {
    IoUtils.readFully(in, buffer, 0, Double.BYTES);
    return Double.longBitsToDouble(IoUtils.bufferToLong(buffer));
  }
}
