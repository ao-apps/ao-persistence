/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2012, 2016, 2020, 2021, 2022, 2024  AO Industries, Inc.
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

/**
 * A persistent buffer retains its data between uses.  They should not be used by
 * multiple virtual machines or even multiple instances within the same
 * virtual machine.  They are meant for persistence only, not interprocess
 * communication.
 *
 * <p>To ensure the data integrity of higher-level data structures, the {@link PersistentBuffer#barrier(boolean)} method
 * must be used.  A barrier ensures that all writes before the barrier happen before
 * all writes after the barrier.  It also accepts a parameter indicating it should
 * also {@code force} (fsync) all writes before the barrier to physical media.  Write order
 * between {@link PersistentBuffer#barrier(boolean)} calls is not maintained.</p>
 *
 * @author  AO Industries, Inc.
 */
public interface PersistentBuffer extends Closeable {

  /**
   * Checks if this buffer is closed.
   */
  boolean isClosed();

  /**
   * Closes this buffer.  It is OK to close an already closed buffer.
   */
  @Override
  void close() throws IOException;

  /**
   * Gets the capacity of this buffer.
   */
  long capacity() throws IOException;

  /**
   * Sets the capacity of this buffer.  If the buffer is increased in size, the
   * new space will be zero-filled.  Setting the capacity may impose an
   * automatic {@link PersistentBuffer#barrier(boolean) barrier(true)}, depending on implementation.  This
   * should be considered an expensive operation.
   */
  void setCapacity(long newCapacity) throws IOException;

  /**
   * Reads to the provided {@code byte[]}, starting at the provided
   * position and for the designated number of bytes.
   *
   * @exception  BufferUnderflowException on end of file
   */
  void get(long position, byte[] buff, int off, int len) throws IOException;

  /**
   * Reads to the provided {@code byte[]}, may read fewer than {@code len}
   * bytes, but will always read at least one byte.  Blocks if no data is
   * available.
   *
   * @exception  BufferUnderflowException on end of file
   */
  int getSome(long position, byte[] buff, int off, int len) throws IOException;

  /**
   * Reads a boolean at the provided position, zero is considered {@code false}
   * and any non-zero value is {@code true}.
   */
  boolean getBoolean(long position) throws IOException;

  /**
   * Reads a byte at the provided position.
   */
  byte get(long position) throws IOException;

  /**
   * Reads an integer at the provided position.
   */
  int getInt(long position) throws IOException;

  /**
   * Reads a long at the provided position.
   */
  long getLong(long position) throws IOException;

  /**
   * Ensures that all values from the position for the provided length
   * are zeros.  This may or may not modify the buffer in the process.
   * The values will all be zero upon return.  Some implementations
   * may choose to overwrite zeros and return modified, others may choose
   * to detect zeros and avoid modifications.  Thus it is possible for
   * existing zeros to still result in a modification.
   */
  void ensureZeros(long position, long len) throws IOException;

  /**
   * Puts a single value in the buffer.
   */
  void put(long position, byte value) throws IOException;

  /**
   * Writes the bytes to the provided position.  The buffer will not be expanded
   * automatically.
   *
   * @exception  BufferOverflowException on end of file
   */
  void put(long position, byte[] buff, int off, int len) throws IOException;

  /**
   * Writes an integer at the provided position.  The buffer will not be expanded
   * automatically.
   *
   * @exception  BufferOverflowException on end of file
   */
  void putInt(long position, int value) throws IOException;

  /**
   * Writes a long at the provided position.  The buffer will not be expanded
   * automatically.
   *
   * @exception  BufferOverflowException on end of file
   */
  void putLong(long position, long value) throws IOException;

  /**
   * Gets the protection level currently enforced by the buffer.
   *
   * @see  PersistentBuffer#barrier(boolean)
   */
  ProtectionLevel getProtectionLevel();

  /**
   * Ensures that all writes before this barrier occur before all writes after
   * this barrier.  If {@code force} is {@code true}, will also
   * commit to physical media synchronously before returning.  This request
   * may be ignored or force downgraded to barrier-only depending on the current
   * protection level.
   *
   * @see  PersistentBuffer#getProtectionLevel()
   */
  void barrier(boolean force) throws IOException;

  /**
   * Gets an input stream that reads from this buffer.  Bounds checking is performed.
   *
   * @exception  BufferUnderflowException on end of file
   */
  InputStream getInputStream(long position, long length) throws IOException, BufferUnderflowException;

  /**
   * Gets an output stream that writes to this buffer.  Bounds checking is performed.
   *
   * @exception  BufferUnderflowException on end of file
   */
  OutputStream getOutputStream(long position, long length) throws IOException, BufferOverflowException;
}
