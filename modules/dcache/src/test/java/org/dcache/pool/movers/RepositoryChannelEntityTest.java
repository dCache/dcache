/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.dcache.pool.repository.RepositoryChannel;
import org.junit.Before;
import org.junit.Test;

public class RepositoryChannelEntityTest {

    private static final String CHANNEL_DATA = "THIS IS SOME TEST DATA";
    private static final byte[] CHANNEL_RAW_DATA = CHANNEL_DATA.getBytes(StandardCharsets.UTF_8);

    private RepositoryChannel channel;
    private RepositoryChannelEntity entity;

    @Before
    public void setup() throws Exception {
        channel = mock(RepositoryChannel.class);

        given(channel.read(any(), anyLong())).willAnswer(i -> {
            ByteBuffer bb = i.getArgument(0);
            int requestedOffset = ((Long) i.getArgument(1)).intValue();

            int requestedEnd = requestedOffset + bb.remaining();
            int boundedEnd = Math.min(requestedEnd, CHANNEL_RAW_DATA.length);
            int boundedOffset = Math.min(requestedOffset, CHANNEL_RAW_DATA.length);

            int transferred = boundedEnd - boundedOffset;

            boolean isEof = bb.remaining() > 0 && transferred == 0;

            bb.put(CHANNEL_RAW_DATA, boundedOffset, transferred);

            return isEof ? -1 : transferred;
        });

        entity = new RepositoryChannelEntity(channel);
    }

    @Test
    public void shouldBeRepeatable() {
        assertTrue(entity.isRepeatable());
    }

    @Test
    public void shouldBeStreaming() {
        assertTrue(entity.isStreaming());
    }

    @Test
    public void shouldNotBeChunked() {
        assertFalse(entity.isChunked());
    }

    @Test
    public void shouldNotSpecifyContentType() {
        assertThat(entity.getContentType(), equalTo(null));
    }

    @Test
    public void shouldNotSpecifyContentEncoding() {
        assertThat(entity.getContentEncoding(), equalTo(null));
    }

    @Test
    public void shouldReturnKnownFileSize() throws Exception {
        given(channel.size()).willReturn(10240L);

        long size = entity.getContentLength();

        assertThat(size, equalTo(10240L));
    }

    @Test
    public void shouldChunkEncodeForUnknownFileSize() throws Exception {
        given(channel.size()).willThrow(new IOException("Unable to stat file"));

        long size = entity.getContentLength();

        assertThat(size, equalTo(-1L));
    }

    @Test
    public void shouldNotCloseStreamOnGetContentClose() throws Exception {
        entity.getContent().close();

        verify(channel, never()).close();
    }

    @Test
    public void shouldNotCloseStreamOnWriteTo() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        entity.writeTo(output);

        verify(channel, never()).close();
        assertThat(output.toString("UTF-8"), equalTo(CHANNEL_DATA));
    }

    @Test
    public void shouldSupportMultipleWriteTo() throws Exception {
        ByteArrayOutputStream output1 = new ByteArrayOutputStream();
        ByteArrayOutputStream output2 = new ByteArrayOutputStream();

        entity.writeTo(output1);
        entity.writeTo(output2);

        verify(channel, never()).close();
        assertThat(output1.toString("UTF-8"), equalTo(CHANNEL_DATA));
        assertThat(output2.toString("UTF-8"), equalTo(CHANNEL_DATA));
    }
}
