/*
 * ao-persistence - Highly efficient persistent collections for Java.
 * Copyright (C) 2008, 2009, 2010, 2011, 2012, 2013, 2016, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.NotImplementedException;

/**
 * This buffer wraps a buffer and introduces random failures.  All writes between
 * barriers are cached.  During a write, there is a chance of failure proportional
 * to the overall size of the write.  During a failure, a random subset of 512-byte
 * blocks of the cached writes will be written.  Once a failure has occurred, every
 * other access to this buffer will throw an exception.  This is to simulate a
 * complete power failure.
 * <p>
 * To simulate power failures, assumptions about the underlying operating system
 * and hardware include:
 * </p>
 * <ol>
 *   <li>Writes of a single sector will either be completely written or not written at all - no in-between states.</li>
 *   <li>Writes of different sectors between {@code force} calls can occur in any order</li>
 *   <li>Writes of a single sector will occur in order - an older version of the sector will never overwrite a newer version</li>
 *   <li>The wrapped {@link PersistentBuffer} correctly implements the {@link PersistentBuffer#barrier(boolean) barrier} method.
 * </ol>
 *
 * @author  AO Industries, Inc.
 */
public class RandomFailBuffer extends AbstractPersistentBuffer {

  /**
   * The average number of calls between failures.
   */
  private enum FailureMethod {
    capacity {
      @Override
      @SuppressWarnings("AssertWithSideEffects")
      int getFailInterval() {
        int failInterval = 5000;
        assert (failInterval = 500000) != 0; // Intentional assertion side-effect to reduce failure frequency due to higher buffer access rates
        return failInterval;
      }
    },
    setCapacity {
      @Override
      int getFailInterval() {
        return 50;
      }
    },
    getSome {
      @Override
      @SuppressWarnings("AssertWithSideEffects")
      int getFailInterval() {
        int failInterval = 5000;
        assert (failInterval = 50000) != 0; // Intentional assertion side-effect to reduce failure frequency due to higher buffer access rates
        return failInterval;
      }
    },
    put {
      @Override
      int getFailInterval() {
        return 5000;
      }
    },
    barrier {
      @Override
      int getFailInterval() {
        return 5000;
      }
    };

    abstract int getFailInterval();
  }

  /**
   * The number of bytes per sector.  This should match the physical media
   * on which normal buffers will resize.
   */
  private static final int SECTOR_SIZE = 512;

  /**
   * A fast pseudo-random number generator for non-cryptographic purposes.
   */
  private static final Random fastRandom = new Random(IoUtils.bufferToLong(new SecureRandom().generateSeed(Long.BYTES)));

  private final PersistentBuffer wrapped;
  private final boolean allowFailures;
  private boolean isClosed;

  /**
   * Keeps track of the last version of all sectors that have been written.  Each
   * entry will be {@link #SECTOR_SIZE} in length, even if at the end of the
   * capacity.
   */
  private final Map<Long, byte[]> writeCache = new HashMap<>();

  /**
   * Creates a read-write test buffer with protection level {@link ProtectionLevel#NONE}.
   */
  public RandomFailBuffer(PersistentBuffer wrapped, boolean allowFailures) {
    super(wrapped.getProtectionLevel());
    this.wrapped = wrapped;
    this.allowFailures = allowFailures;
  }

  /**
   * Fails in a one-in-interval chance.
   */
  @SuppressWarnings("ConvertToTryWithResources")
  private void randomFail(FailureMethod failureMethod) throws IOException {
    if (allowFailures) {
      if (fastRandom.nextInt(failureMethod.getFailInterval()) == 0) {
        isClosed = true;
        if (!writeCache.isEmpty()) {
          long capacity = wrapped.capacity();
          // Write current write cache in a partial state
          List<Long> sectors = new ArrayList<>(writeCache.keySet());
          Collections.shuffle(sectors, fastRandom);
          int numToWrite = fastRandom.nextInt(sectors.size());
          for (int c = 0; c < numToWrite; c++) {
            long sector = sectors.get(c);
            long sectorEnd = sector + SECTOR_SIZE;
            if (sectorEnd > capacity) {
              sectorEnd = capacity;
            }
            wrapped.put(sector, writeCache.get(sector), 0, (int) (sectorEnd - sector));
          }
          writeCache.clear();
        }
        wrapped.barrier(true);
        wrapped.close();
        throw new IOException(failureMethod + ": Random simulated failure.  The stream will be unusable to simulate power failure or software crash.");
      }
    }
  }

  private void flushWriteCache() throws IOException {
    if (!writeCache.isEmpty()) {
      long capacity = wrapped.capacity();
      // Write current write cache in full
      for (Map.Entry<Long, byte[]> entry : writeCache.entrySet()) {
        long sector = entry.getKey();
        long sectorEnd = sector + SECTOR_SIZE;
        if (sectorEnd > capacity) {
          sectorEnd = capacity;
        }
        wrapped.put(sector, entry.getValue(), 0, (int) (sectorEnd - sector));
      }
      writeCache.clear();
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  @SuppressWarnings("ConvertToTryWithResources")
  public void close() throws IOException {
    flushWriteCache();
    isClosed = true;
    wrapped.close();
  }

  /**
   * Checks if closed and throws IOException if so.
   */
  private void checkClosed() throws IOException {
    if (isClosed) {
      throw new IOException("RandomFailBuffer closed");
    }
  }

  @Override
  public long capacity() throws IOException {
    checkClosed();
    randomFail(FailureMethod.capacity);
    return wrapped.capacity();
  }

  @Override
  public void setCapacity(long newCapacity) throws IOException {
    checkClosed();
    randomFail(FailureMethod.setCapacity);
    Iterator<Map.Entry<Long, byte[]>> entries = writeCache.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<Long, byte[]> entry = entries.next();
      long sector = entry.getKey();
      if (sector >= newCapacity) {
        // Remove any cached writes that start >= newCapacity
        entries.remove();
      } else {
        long sectorEnd = sector + SECTOR_SIZE;
        assert newCapacity > sector;
        if (newCapacity < sectorEnd) {
          // Also, zero-out any part of the last sector (beyond newCapacity) if it is a cached write
          Arrays.fill(entry.getValue(), (int) (newCapacity - sector), SECTOR_SIZE, (byte) 0);
        }
      }
    }
    wrapped.setCapacity(newCapacity);
  }

  @Override
  public int getSome(long position, final byte[] buff, int off, int len) throws IOException {
    checkClosed();
    if (position < 0) {
      throw new IllegalArgumentException("position<0: " + position);
    }
    if (off < 0) {
      throw new IllegalArgumentException("off<0: " + off);
    }
    if (len < 0) {
      throw new IllegalArgumentException("len<0: " + len);
    }
    final long end = position + len;
    assert end <= capacity();
    randomFail(FailureMethod.getSome);
    int bytesRead = 0;
    while (position < end) {
      long sector = position & (-SECTOR_SIZE);
      assert (sector & (SECTOR_SIZE - 1)) == 0 : "Sector not aligned";
      int buffEnd = off + (SECTOR_SIZE + (int) (sector - position));
      if (buffEnd > (off + len)) {
        buffEnd = off + len;
      }
      int bytesToRead = buffEnd - off;
      assert bytesToRead <= len;
      byte[] cached = writeCache.get(sector);
      int count;
      if (cached != null) {
        System.arraycopy(cached, (int) (position - sector), buff, off, bytesToRead);
        count = bytesToRead;
      } else {
        count = wrapped.getSome(position, buff, off, bytesToRead);
      }
      bytesRead += count;
      if (count < bytesToRead) {
        break;
      }
      position += count;
      off += count;
      len -= count;
    }
    return bytesRead;
  }

  @Override
  public void ensureZeros(long position, long len) throws IOException {
    throw new NotImplementedException("Implement when first needed");
  }

  @Override
  public void put(long position, byte[] buff, int off, int len) throws IOException {
    checkClosed();
    if (position < 0) {
      throw new IllegalArgumentException("position<0: " + position);
    }
    if (off < 0) {
      throw new IllegalArgumentException("off<0: " + off);
    }
    if (len < 0) {
      throw new IllegalArgumentException("len<0: " + len);
    }
    long capacity = capacity();
    final long end = position + len;
    assert end <= capacity;
    randomFail(FailureMethod.put);
    while (position < end) {
      long sector = position & (-SECTOR_SIZE);
      assert (sector & (SECTOR_SIZE - 1)) == 0 : "Sector not aligned";
      int buffEnd = off + (SECTOR_SIZE + (int) (sector - position));
      if (buffEnd > (off + len)) {
        buffEnd = off + len;
      }
      int bytesToWrite = buffEnd - off;
      byte[] cached = writeCache.get(sector);
      if (cached == null) {
        // Populate cache (consider capacity)
        cached = new byte[SECTOR_SIZE];
        long sectorEnd = sector + SECTOR_SIZE;
        if (sectorEnd > capacity) {
          sectorEnd = capacity;
        }
        wrapped.get(sector, cached, 0, (int) (sectorEnd - sector));
        writeCache.put(sector, cached);
      }
      // Update cache only (do not write-through)
      System.arraycopy(buff, off, cached, (int) (position - sector), bytesToWrite);
      position += bytesToWrite;
      off += bytesToWrite;
      len -= bytesToWrite;
    }
  }

  @Override
  public ProtectionLevel getProtectionLevel() {
    return wrapped.getProtectionLevel();
  }

  @Override
  public void barrier(boolean force) throws IOException {
    checkClosed();
    randomFail(FailureMethod.barrier);
    flushWriteCache();
    wrapped.barrier(force);
  }
}
