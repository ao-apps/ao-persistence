/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2009, 2010, 2011, 2012, 2013, 2016, 2017, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.tempfiles.TempFileContext;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Uses a set of <code>MappedByteBuffer</code> for persistence.  Each buffer
 * covers a maximum of 2^30 bytes.  This handles mapping of up to
 * 2^30 * 2^31-2 bytes.
 *
 * @see  MappedPersistentBuffer
 *
 * @author  AO Industries, Inc.
 */
public class LargeMappedPersistentBuffer extends AbstractPersistentBuffer {

  //private static final Logger logger = Logger.getLogger(LargeMappedPersistentBuffer.class.getName());

  // For testing, change this value to something smaller, like 12.  This will
  // allow the testing of the buffer boundary conditions.
  // 12 matches Linux page size of 4096 bytes and still performs well.
  // 10 performed less consistently, with frequently higher CPU usage.
  private static final int BUFFER_NUM_BIT_SHIFT = 30; // 12 for testing.
  private static final int BUFFER_SIZE = 1 << BUFFER_NUM_BIT_SHIFT;
  private static final int BUFFER_INDEX_MASK = BUFFER_SIZE - 1;

  private final TempFileContext tempFileContext;
  private final RandomAccessFile raf;
  private final FileChannel channel;
  private final List<MappedByteBuffer> mappedBuffers = new ArrayList<>();
  private final List<Boolean> modifiedBuffers;
  private boolean closed;

  /**
   * Creates a read-write buffer backed by a temporary file.  The protection level
   * is set to <code>NONE</code>.  The temporary file will be deleted when this
   * buffer is closed or on JVM shutdown.
   */
  public LargeMappedPersistentBuffer() throws IOException {
    super(ProtectionLevel.NONE);
    tempFileContext = new TempFileContext();
    raf = new RandomAccessFile(tempFileContext.createTempFile("LargeMappedPersistentBuffer_").getFile(), "rw");
    channel = raf.getChannel();
    // Lock the file
    channel.lock(0L, Long.MAX_VALUE, false);
    modifiedBuffers = null;
    fillMappedBuffers();
  }

  /**
   * Creates a read-write buffer with <code>BARRIER</code> protection level.
   */
  public LargeMappedPersistentBuffer(String name) throws IOException {
    this(new RandomAccessFile(name, "rw"), ProtectionLevel.BARRIER);
  }

  /**
   * Creates a buffer.
   */
  public LargeMappedPersistentBuffer(String name, ProtectionLevel protectionLevel) throws IOException {
    this(new RandomAccessFile(name, protectionLevel == ProtectionLevel.READ_ONLY ? "r" : "rw"), protectionLevel);
  }

  /**
   * Creates a read-write buffer with <code>BARRIER</code> protection level.
   */
  public LargeMappedPersistentBuffer(File file) throws IOException {
    this(new RandomAccessFile(file, "rw"), ProtectionLevel.BARRIER);
  }

  /**
   * Creates a buffer.
   */
  public LargeMappedPersistentBuffer(File file, ProtectionLevel protectionLevel) throws IOException {
    this(new RandomAccessFile(file, protectionLevel == ProtectionLevel.READ_ONLY ? "r" : "rw"), protectionLevel);
  }

  /**
   * Creates a buffer using the provided <code>RandomAccessFile</code>.
   */
  public LargeMappedPersistentBuffer(RandomAccessFile raf, ProtectionLevel protectionLevel) throws IOException {
    super(protectionLevel);
    this.tempFileContext = null;
    this.raf = raf;
    channel = raf.getChannel();
    // Lock the file
    channel.lock(0L, Long.MAX_VALUE, protectionLevel == ProtectionLevel.READ_ONLY);
    if (protectionLevel.compareTo(ProtectionLevel.BARRIER) >= 0) {
      modifiedBuffers = new ArrayList<>();
    } else {
      modifiedBuffers = null;
    }
    fillMappedBuffers();
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * @deprecated The finalization mechanism is inherently problematic.
   */
  @Deprecated(since="9")
  @Override
  @SuppressWarnings("FinalizeDeclaration")
  protected void finalize() throws Throwable {
    try {
      close();
    } finally {
      super.finalize();
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    raf.close();
    if (tempFileContext != null) {
      tempFileContext.close();
    }
  }

  @Override
  public long capacity() throws IOException {
    return raf.length();
  }

  /**
   * Fills the buffers to cover the entire file length.
   */
  private void fillMappedBuffers() throws IOException {
    long len = raf.length();
    long maxBuffNum = len >>> BUFFER_NUM_BIT_SHIFT;
    if (maxBuffNum >= Integer.MAX_VALUE) {
      throw new IOException("file too large for LargeMappedPersistentBuffer: "+len);
    }
    int buffNumInt = (int)maxBuffNum;
    // Expand list
    while (mappedBuffers.size() <= buffNumInt) {
      long mapStart = ((long)mappedBuffers.size()) << BUFFER_NUM_BIT_SHIFT;
      long size = raf.length() - mapStart;
      if (size>BUFFER_SIZE) {
        size = BUFFER_SIZE;
      }
      mappedBuffers.add(channel.map(protectionLevel == ProtectionLevel.READ_ONLY ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, mapStart, size));
      if (modifiedBuffers != null) {
        modifiedBuffers.add(false);
      }
    }
  }

  private static int getBufferNum(long position) throws IOException {
    if (position<0) {
      throw new IllegalArgumentException("position<0: "+position);
    }
    long buffNum = position >>> BUFFER_NUM_BIT_SHIFT;
    if (buffNum >= Integer.MAX_VALUE) {
      throw new IOException("position too large for LargeMappedPersistentBuffer: "+position);
    }
    return (int)buffNum;
  }

  /**
   * Gets the position as an integer or throws IOException if too big for a mapped buffer.
   */
  private static int getIndex(long position) {
    return (int)(position&BUFFER_INDEX_MASK);
  }

  @Override
  public void setCapacity(long newLength) throws IOException {
    long oldLength = capacity();
    if (oldLength != newLength) {
      // Remove any buffers that could be affected
      long affectedFrom = getBufferNum(Math.min(oldLength, newLength));
      while (mappedBuffers.size()>affectedFrom) {
        int index = mappedBuffers.size()-1;
        MappedByteBuffer mappedBuffer = mappedBuffers.get(index);
        if (modifiedBuffers != null) {
          if (modifiedBuffers.get(index)) {
            mappedBuffer.force();
          }
          modifiedBuffers.remove(index);
        }
        mappedBuffers.remove(index);
      }
      raf.setLength(newLength);
      fillMappedBuffers();
      if (newLength>oldLength) {
        // Ensure zero-filled
        ensureZeros(oldLength, newLength-oldLength);
      }
    }
  }

  @Override
  public void get(long position, byte[] buff, int off, int len) throws IOException {
    if (len>0) {
      int bufferNum = getBufferNum(position);
      int bufferStart = getIndex(position);
      int bufferSize = BUFFER_SIZE - bufferStart;
      do {
        if (bufferSize > len) {
          bufferSize = len;
        }
        MappedByteBuffer mappedBuffer = mappedBuffers.get(bufferNum);
        mappedBuffer.position(bufferStart);
        mappedBuffer.get(buff, off, bufferSize);
        len -= bufferSize;
        // Don't need to offset values on last iteration, end loop here
        if (len <= 0) {
          assert len == 0;
          break;
        }
        position += bufferSize;
        off += bufferSize;
        // Starting at the beginning of the next buffer
        bufferNum++;
        bufferStart = 0;
        bufferSize = BUFFER_SIZE;
      } while (true);
    }
  }

  @Override
  public int getSome(long position, byte[] buff, int off, int len) throws IOException {
    get(position, buff, off, len);
    return len;
  }

  /**
   * Gets a single byte from the buffer.
   */
  @Override
  public byte get(long position) throws IOException {
    return mappedBuffers.get(getBufferNum(position)).get(getIndex(position));
  }

  @Override
  public void ensureZeros(long position, long len) throws IOException {
    if (len>0) {
      int bufferNum = getBufferNum(position);
      int bufferStart = getIndex(position);
      int bufferSize = BUFFER_SIZE - bufferStart;
      do {
        if (bufferSize > len) {
          bufferSize = (int)len;
        }
        MappedByteBuffer mappedBuffer = mappedBuffers.get(bufferNum);
        if (
          PersistentCollections.ensureZeros(mappedBuffer, bufferStart, bufferSize)
          && modifiedBuffers != null
        ) {
          modifiedBuffers.set(bufferNum, true);
        }
        len -= bufferSize;
        // Don't need to offset values on last iteration, end loop here
        if (len <= 0) {
          assert len == 0;
          break;
        }
        position += bufferSize;
        // Starting at the beginning of the next buffer
        bufferNum++;
        bufferStart = 0;
        bufferSize = BUFFER_SIZE;
      } while (true);
    }
  }

  /**
   * Puts a single byte in the buffer.
   */
  @Override
  public void put(long position, byte value) throws IOException {
    int bufferNum = getBufferNum(position);
    mappedBuffers.get(bufferNum).put(getIndex(position), value);
    if (modifiedBuffers != null) {
      modifiedBuffers.set(bufferNum, true);
    }
  }

  @Override
  public void put(long position, byte[] buff, int off, int len) throws IOException {
    if (len>0) {
      int bufferNum = getBufferNum(position);
      int bufferStart = getIndex(position);
      int bufferSize = BUFFER_SIZE - bufferStart;
      do {
        if (bufferSize > len) {
          bufferSize = len;
        }
        MappedByteBuffer mappedBuffer = mappedBuffers.get(bufferNum);
        mappedBuffer.position(bufferStart);
        mappedBuffer.put(buff, off, bufferSize);
        if (modifiedBuffers != null) {
          modifiedBuffers.set(bufferNum, true);
        }
        len -= bufferSize;
        // Don't need to offset values on last iteration, end loop here
        if (len <= 0) {
          assert len == 0;
          break;
        }
        position += bufferSize;
        off += bufferSize;
        // Starting at the beginning of the next buffer
        bufferNum++;
        bufferStart = 0;
        bufferSize = BUFFER_SIZE;
      } while (true);
    }
  }

  /**
   * There is not currently a way to provide a barrier without using <code>force</code>.
   * This just uses force for both.
   */
  @Override
  public void barrier(boolean force) throws IOException {
    if (protectionLevel.compareTo(ProtectionLevel.BARRIER) >= 0) {
      for (int c=0, len=mappedBuffers.size(); c<len; c++) {
        if (modifiedBuffers.get(c)) {
          mappedBuffers.get(c).force();
          modifiedBuffers.set(c, false);
        }
      }
      // channel.force(true);
    }
  }

  @Override
  public boolean getBoolean(long position) throws IOException {
    return get(position) != 0;
  }

  @Override
  public int getInt(long position) throws IOException {
    int startBufferNum = getBufferNum(position);
    int endBufferNum = getBufferNum(position+3);
    if (startBufferNum == endBufferNum) {
      return mappedBuffers.get(startBufferNum).getInt(getIndex(position));
    } else {
      // Boundary condition
      return
          ((get(position)&255) << 24)
        + ((get(position+1)&255) << 16)
        + ((get(position+2)&255) << 8)
        + (get(position+3)&255)
      ;
    }
  }

  @Override
  public long getLong(long position) throws IOException {
    int startBufferNum = getBufferNum(position);
    int endBufferNum = getBufferNum(position+7);
    if (startBufferNum == endBufferNum) {
      return mappedBuffers.get(startBufferNum).getLong(getIndex(position));
    } else {
      // Boundary condition
      return
          ((get(position)&255L) << 56)
        + ((get(position+1)&255L) << 48)
        + ((get(position+2)&255L) << 40)
        + ((get(position+3)&255L) << 32)
        + ((get(position+4)&255L) << 24)
        + ((get(position+5)&255L) << 16)
        + ((get(position+6)&255L) << 8)
        + (get(position+7)&255L)
      ;
    }
  }

  @Override
  public void putInt(long position, int value) throws IOException {
    int startBufferNum = getBufferNum(position);
    int endBufferNum = getBufferNum(position+3);
    if (startBufferNum == endBufferNum) {
      mappedBuffers.get(startBufferNum).putInt(getIndex(position), value);
      if (modifiedBuffers != null) {
        modifiedBuffers.set(startBufferNum, true);
      }
    } else {
      // Boundary condition
      put(position, (byte)(value >>> 24));
      put(position+1, (byte)(value >>> 16));
      put(position+2, (byte)(value >>> 8));
      put(position+3, (byte)value);
    }
  }

  @Override
  public void putLong(long position, long value) throws IOException {
    int startBufferNum = getBufferNum(position);
    int endBufferNum = getBufferNum(position+7);
    if (startBufferNum == endBufferNum) {
      mappedBuffers.get(startBufferNum).putLong(getIndex(position), value);
      if (modifiedBuffers != null) {
        modifiedBuffers.set(startBufferNum, true);
      }
    } else {
      // Boundary condition
      put(position, (byte)(value >>> 56));
      put(position+1, (byte)(value >>> 48));
      put(position+2, (byte)(value >>> 40));
      put(position+3, (byte)(value >>> 32));
      put(position+4, (byte)(value >>> 24));
      put(position+5, (byte)(value >>> 16));
      put(position+6, (byte)(value >>> 8));
      put(position+7, (byte)value);
    }
  }
}
