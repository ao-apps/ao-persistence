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

/**
 * The different protection levels offered.
 */
public enum ProtectionLevel {

  /**
   * Read-only access.  Highest performance and protection.
   */
  READ_ONLY,

  /**
   * Offers no data protection.  Highest performance.
   */
  NONE,

  /**
   * Prevents data corruption, but does not prevent data loss.  Moderate performance.
   */
  BARRIER,

  /**
   * Prevents both data corruption and data loss.  Lowest performance.
   */
  FORCE
}
