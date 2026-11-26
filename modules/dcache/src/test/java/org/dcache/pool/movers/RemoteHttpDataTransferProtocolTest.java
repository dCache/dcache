/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.pool.movers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.HttpEntity;
import org.junit.Test;

public class RemoteHttpDataTransferProtocolTest {

    private static final int BODY_SIZE = 1_000_000;

    /**
     * A body of {@link #BODY_SIZE} bytes that records how much of it was read and whether it was
     * closed.
     */
    private static class Body extends InputStream {

        private int read;
        private boolean closed;

        @Override
        public int read() {
            if (read == BODY_SIZE) {
                return -1;
            }
            read++;
            return 0;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (read == BODY_SIZE) {
                return -1;
            }
            int n = Math.min(len, BODY_SIZE - read);
            Arrays.fill(b, off, off + n, (byte) 0);
            read += n;
            return n;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static HttpEntity entityWith(InputStream content) throws IOException {
        HttpEntity entity = mock(HttpEntity.class);
        given(entity.getContent()).willReturn(content);
        return entity;
    }

    @Test
    public void shouldCopyWholeBody() throws Exception {
        byte[] data = new byte[BODY_SIZE];
        data[BODY_SIZE - 1] = 42;
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        RemoteHttpDataTransferProtocol.receiveBody(entityWith(new ByteArrayInputStream(data)), out);

        assertThat(out.toByteArray(), equalTo(data));
    }

    @Test
    public void shouldNotDrainOrCloseBodyWhenWriteFails() throws Exception {
        Body content = new Body();
        OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new ClosedByInterruptException();
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw new ClosedByInterruptException();
            }
        };

        try {
            RemoteHttpDataTransferProtocol.receiveBody(entityWith(content), out);
            fail("expected failure");
        } catch (ClosedByInterruptException expected) {
        }

        assertThat(content.read, lessThan(BODY_SIZE));
        assertThat(content.closed, equalTo(false));
    }

    @Test
    public void shouldNotCloseBodyWhenReadFails() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        InputStream content = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("connection reset");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw new IOException("connection reset");
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        try {
            RemoteHttpDataTransferProtocol.receiveBody(entityWith(content),
                  new ByteArrayOutputStream());
            fail("expected failure");
        } catch (IOException expected) {
        }

        assertThat(closed.get(), equalTo(false));
    }
}
