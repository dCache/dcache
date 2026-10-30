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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;

import diskCacheV111.util.ThirdPartyTransferFailedCacheException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.junit.Test;

public class ProxiedTransferChecksumsTest {

    private static final byte[] DATA = "some file content".getBytes(StandardCharsets.UTF_8);

    private static Checksum checksum(ChecksumType type, byte[] data) {
        return type.calculate(data);
    }

    @Test
    public void shouldCalculateChecksumsAndCountBytesWhenReadingInChunks() throws IOException {
        AtomicLong count = new AtomicLong();
        var in = new ProxiedTransferChecksums.CalculatingInputStream(
              new ByteArrayInputStream(DATA),
              Set.of(ChecksumType.MD5_TYPE, ChecksumType.ADLER32), count::addAndGet);

        byte[] buffer = new byte[5];
        while (in.read(buffer, 0, buffer.length) != -1) {
            // read everything
        }

        assertThat(in.checksums(), containsInAnyOrder(checksum(ChecksumType.MD5_TYPE, DATA),
              checksum(ChecksumType.ADLER32, DATA)));
        assertThat(count.get(), equalTo((long) DATA.length));
    }

    @Test
    public void shouldCalculateChecksumsWhenReadingSingleBytesAndSkipping() throws IOException {
        AtomicLong count = new AtomicLong();
        var in = new ProxiedTransferChecksums.CalculatingInputStream(
              new ByteArrayInputStream(DATA), Set.of(ChecksumType.MD5_TYPE), count::addAndGet);

        assertThat(in.read(), equalTo((int) DATA[0]));
        assertThat(in.skip(3), equalTo(3L));
        in.transferTo(java.io.OutputStream.nullOutputStream());

        assertThat(in.checksums(), containsInAnyOrder(checksum(ChecksumType.MD5_TYPE, DATA)));
        assertThat(count.get(), equalTo((long) DATA.length));
    }

    @Test
    public void shouldAcceptMatchingChecksum() throws Exception {
        Checksum md5 = checksum(ChecksumType.MD5_TYPE, DATA);

        ProxiedTransferChecksums.verify(List.of(md5), List.of(md5), true);
    }

    @Test
    public void shouldAcceptMissingRemoteChecksumWhenVerificationNotRequired() throws Exception {
        ProxiedTransferChecksums.verify(List.of(),
              List.of(checksum(ChecksumType.MD5_TYPE, DATA)), false);
    }

    @Test
    public void shouldAcceptUncomparableChecksumWhenVerificationNotRequired() throws Exception {
        ProxiedTransferChecksums.verify(List.of(checksum(ChecksumType.SHA1, DATA)),
              List.of(checksum(ChecksumType.MD5_TYPE, DATA)), false);
    }

    @Test
    public void shouldRejectMismatchingChecksum() {
        Checksum remote = checksum(ChecksumType.MD5_TYPE,
              "other".getBytes(StandardCharsets.UTF_8));

        try {
            ProxiedTransferChecksums.verify(List.of(remote),
                  List.of(checksum(ChecksumType.MD5_TYPE, DATA)), false);
            fail("expected failure");
        } catch (ThirdPartyTransferFailedCacheException e) {
            assertThat(e.getMessage(), containsString("checksum mismatch"));
        }
    }

    @Test
    public void shouldRejectMissingChecksumWhenVerificationRequired() {
        try {
            ProxiedTransferChecksums.verify(List.of(),
                  List.of(checksum(ChecksumType.MD5_TYPE, DATA)), true);
            fail("expected failure");
        } catch (ThirdPartyTransferFailedCacheException e) {
            assertThat(e.getMessage(), containsString("no useful checksum"));
        }
    }

    @Test
    public void shouldRejectUncomparableChecksumWhenVerificationRequired() {
        try {
            ProxiedTransferChecksums.verify(List.of(checksum(ChecksumType.SHA1, DATA)),
                  List.of(checksum(ChecksumType.MD5_TYPE, DATA)), true);
            fail("expected failure");
        } catch (ThirdPartyTransferFailedCacheException e) {
            assertThat(e.getMessage(), containsString("no useful checksum"));
        }
    }
}
