/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import javax.annotation.concurrent.ThreadSafe;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates globally unique inode ids.
 */
@ThreadSafe
public final class InodeId {

    /**
     * no instance allowed
     */
    private InodeId() { /**/ }

    /**
     * generates a 36 chars long globally unique new inode id, The format is:
     * chars 0-3   - fsid, unused
     * chars 4-35  - inode id
     *
     * @param fsId filesystem id.
     * @return new inode unique id.
     */
    public static String newID(int fsId) {

        UUID newId = UUIDv7.randomUUID();

        String idString = digits((long) fsId >> 32, 4) +
              digits(newId.getMostSignificantBits() >> 32, 8) +
              digits(newId.getMostSignificantBits() >> 16, 4) +
              digits(newId.getMostSignificantBits(), 4) +
              digits(newId.getLeastSignificantBits() >> 48, 4) +
              digits(newId.getLeastSignificantBits(), 12);

        return idString.toUpperCase();
    }

    /**
     * Returns val represented by the specified number of hex digits.
     */
    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

    // By Robson Kades https://github.com/robsonkades/uuidv7
    // Licensed under MIT License
    /**
     * UUID version 7 generator compatible with https://www.rfc-editor.org/rfc/rfc9562
     *
     * <p>UUIDv7 is a new time-ordered UUID based on the current timestamp and random bits.
     * UUIDv7 is roughly sortable by generation time, which is more efficient for usage databases.
     */
    public final class UUIDv7 {

        private UUIDv7() {
            // Prevent instantiation
        }

        /**
         * Generates a UUID version 7.
         *
         * <p>The format is:
         * <ul>
         *   <li>Bits 0–47: 48-bit timestamp (milliseconds since epoch, big-endian).</li>
         *   <li>Bits 48–51: 4-bit version (binary 0111).</li>
         *   <li>Bits 52–63: 12 random bits (extracted from a 64-bit random value).</li>
         *   <li>Bits 64–65: 2-bit variant (binary 10).</li>
         *   <li>Bits 66–127: Remaining 62 random bits (52 bits from 64-bit random, plus
         *       10 bits from a 32-bit random, for a total of 74 bits entropy).</li>
         * </ul>
         *
         * @return a {@link java.util.UUID} instance representing a UUIDv7.
         *
         * @see java.util.UUID
         * @see java.util.concurrent.ThreadLocalRandom
         */
        public static UUID randomUUID() {
            // 1) Fetch current time in ms, mask to 48 bits
            long currentMillis = System.currentTimeMillis();
            long ts48 = currentMillis & 0xFFFFFFFFFFFFL;  // 48-bit mask

            // 2) Get 74 bits of entropy from ThreadLocalRandom: 64 + 32 bits
            long random64 = ThreadLocalRandom.current().nextLong();
            int random32 = ThreadLocalRandom.current().nextInt();

            // Assemble the high 64 bits:
            //   [ 48-bit timestamp ] [ 4-bit version=7 ] [ 12 high random bits ]
            long high = (ts48 << 16);                         // place 48 ms bits at bits 0–47 of high<<16 = bits 16–63
            long randHigh12 = (random64 >>> 52) & 0x0FFFL;    // top 12 bits of random64
            high |= randHigh12;                              // bits 52–63
            high |= 0x0000000000007000L;                     // set version (4 bits = 0b0111) at bits 48–51

            // Assemble the low 64 bits:
            //   [ 2-bit variant=10 ] [ 52 low bits of random64 ] [ 10 high bits of random32 ]
            long low = 0x8000000000000000L;                   // set variant 0b10 at bits 64–65
            long randLow52 = random64 & 0x000FFFFFFFFFFFFFL;  // lower 52 bits of random64
            int rand32High10 = (random32 >>> 22) & 0x3FF;     // top 10 bits of random32
            low |= (randLow52 << 10);                         // place 52 bits at bits 66–117
            low |= rand32High10;                              // place 10 bits at bits 118–127

            return new UUID(high, low);
        }
    }
}
