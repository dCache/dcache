/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.webdav.transfer;

import diskCacheV111.util.ThirdPartyTransferFailedCacheException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

/**
 * Support for verifying the integrity of a file that the door pulls from a remote server.
 */
final class ProxiedTransferChecksums {

    private ProxiedTransferChecksums() {
        // Utility class
    }

    /**
     * An InputStream that calculates checksums of all data read and reports how much data was read.
     */
    static class CalculatingInputStream extends FilterInputStream {

        private final Map<ChecksumType, MessageDigest> digests = new EnumMap<>(ChecksumType.class);
        private final LongConsumer onRead;

        CalculatingInputStream(InputStream in, Collection<ChecksumType> types,
              LongConsumer onRead) {
            super(in);
            types.forEach(t -> digests.put(t, t.createMessageDigest()));
            this.onRead = onRead;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) {
                digests.values().forEach(d -> d.update((byte) b));
                onRead.accept(1);
            }
            return b;
        }

        @Override
        public int read(byte[] buffer, int off, int len) throws IOException {
            int count = super.read(buffer, off, len);
            if (count > 0) {
                digests.values().forEach(d -> d.update(buffer, off, count));
                onRead.accept(count);
            }
            return count;
        }

        @Override
        public long skip(long n) throws IOException {
            // Skipping would leave the checksums wrong; read instead.
            byte[] buffer = new byte[(int) Math.min(n, 8192)];
            long skipped = 0;
            while (skipped < n) {
                int count = read(buffer, 0, (int) Math.min(buffer.length, n - skipped));
                if (count < 0) {
                    break;
                }
                skipped += count;
            }
            return skipped;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        /**
         * The checksums of all data read.  Must be called only once, after all data was read.
         */
        List<Checksum> checksums() {
            return digests.values().stream().map(Checksum::new).collect(Collectors.toList());
        }
    }

    /**
     * Compare the checksums that the remote server supplied with those dCache knows.
     *
     * @param remote   checksums that the remote server reported
     * @param local    checksums of the file as dCache has it
     * @param required whether at least one remote checksum must have been verified
     * @throws ThirdPartyTransferFailedCacheException if a checksum differs, or if verification was
     *                                                required but no checksum could be compared
     */
    static void verify(Collection<Checksum> remote, Collection<Checksum> local, boolean required)
          throws ThirdPartyTransferFailedCacheException {
        Map<ChecksumType, Checksum> localByType = new EnumMap<>(ChecksumType.class);
        local.forEach(c -> localByType.putIfAbsent(c.getType(), c));

        boolean verified = false;
        for (Checksum theirs : remote) {
            Checksum ours = localByType.get(theirs.getType());
            if (ours != null) {
                if (!ours.equals(theirs)) {
                    throw new ThirdPartyTransferFailedCacheException(
                          "checksum mismatch: " + theirs.getType().getName() + " "
                                + ours.getValue() + " != " + theirs.getValue());
                }
                verified = true;
            }
        }

        if (required && !verified) {
            String described = new ArrayList<>(remote).stream()
                  .map(Object::toString)
                  .collect(Collectors.joining(","));
            throw new ThirdPartyTransferFailedCacheException(
                  "no useful checksum from remote server: {" + described + "}");
        }
    }
}
