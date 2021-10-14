/*
 COPYRIGHT STATUS:
 Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
 software are sponsored by the U.S. Department of Energy under Contract No.
 DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
 non-exclusive, royalty-free license to publish or reproduce these documents
 and software for U.S. Government purposes.  All documents and software
 available from this server are protected under the U.S. and Foreign
 Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



 DISCLAIMER OF LIABILITY (BSD):

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
 LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
 FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
 OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
 FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
 CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
 LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
 SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


 Liabilities of the Government:

 This software is provided by URA, independent from its Prime Contract
 with the U.S. Department of Energy. URA is acting independently from
 the Government and in its own private capacity and is not acting on
 behalf of the U.S. Government, nor as its contractor nor its agent.
 Correspondingly, it is understood and agreed that the U.S. Government
 has no connection to this software and in no manner whatsoever shall
 be liable for nor assume any responsibility or obligation for any claim,
 cost, or damages arising out of or resulting from the use of the software
 available from this server.


 Export Control:

 All documents and software available from this server are subject to U.S.
 export control laws.  Anyone downloading information from this server is
 obligated to secure any necessary Government licenses before exporting
 documents or software obtained from this server.
 */

package org.dcache.ftp.proxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EDataBlockNio {

    private static final Logger LOGGER = LoggerFactory.getLogger(EDataBlockNio.class);

    private ByteBuffer header;

    private ByteBuffer data;

    private String _myName = "unknown";
    private final ByteBuffer _initial;

    public static final int EOR_DESCRIPTOR = 128;

    public static final int EOF_DESCRIPTOR = 64;

    public static final int SUSPECTED_ERROR_DESCRIPTOR = 32;

    public static final int RESTART_MARKER_DESCRIPTOR = 16;

    public static final int EOD_DESCRIPTOR = 8;

    public static final int SENDER_CLOSES_THIS_STREAM_DESCRIPTOR = 4;

    public static final int HEADER_LENGTH = 17;

    public EDataBlockNio(String name, ByteBuffer initial) {
        _myName = name;
        _initial = initial;
    }

    public ByteBuffer getHeader() {
        return header;
    }

    public byte getDescriptors() {
        return header.get(0);
    }

    public boolean isDescriptorSet(int descriptor) {
        return (getDescriptors() & descriptor) != 0;
    }

    public void setDCCountTo1() {
        for (int i = 9; i < 17; i++) {
            header.put(i, (byte) 0);
        }
        header.put(16, (byte) 1);
    }

    public void setDescriptor(int descriptor) {
        header.put(0, (byte) (getDescriptors() | descriptor));
    }

    public void unsetDescriptor(int descriptor) {
        header.put(0, (byte) (getDescriptors() & ~descriptor));
    }

    public ByteBuffer getData() {
        return data;
    }

    public long getSize() {
        return header.getLong(1);
    }

    public long getDataChannelCount() {
        // XXX probably throwing an exception would be a better plan here...
        if (isDescriptorSet(EOF_DESCRIPTOR)) {
            return getOffset();
        } else {
            return -1;
        }
    }

    public long getOffset() {
        return header.getLong(9);
    }

    // Copy data from initial to target.  This behaves similarly to ByteBuffer#put
    // except that no exception is thrown if initial.remaining() > target.remaining()
    private static void prefill(ByteBuffer initial, ByteBuffer target) {
        if (initial.remaining() > 0) {
            int count = Math.min(initial.remaining(), target.remaining());
            LOGGER.debug("prefilling, initial data is {}", initial);
            int oldLimit = initial.limit();
            initial.limit(initial.position() + count);

            target.put(initial);

            initial.limit(oldLimit);
            initial.compact().flip();
            LOGGER.debug("after prefilling, initial data now {}", initial);
        }
    }

    public long readHeader(SocketChannel socketChannel) {
        LOGGER.debug("Reading header");
        if (header == null) {
            header = ByteBuffer.allocate(HEADER_LENGTH);
        }
        header.clear();
        prefill(_initial, header);
        int len = header.position();

        while (len < HEADER_LENGTH) {
            int n;
            try {
                n = socketChannel.read(header);
            } catch (IOException e) {
                LOGGER.error("Error reading header: {}", e.toString());
                break;
            }

            if (n == -1) {
                LOGGER.debug("Connection closed while reading header");
                markInputClosed(socketChannel);
                break;
            }
            len += n;
        }

        LOGGER.debug("Finish reading header: {} bytes read from {}", len,
              socketChannel.socket().getRemoteSocketAddress());

        return len < HEADER_LENGTH ? -1 : HEADER_LENGTH;
    }

    public long readData(SocketChannel socketChannel, long size) {
        LOGGER.debug("Reading {} bytes", size);

        if (data == null || data.capacity() < (int) size) {
            data = ByteBuffer.allocate((int) size);
        }

        data.clear();
        data.limit((int) size);
        prefill(_initial, data);

        int n = data.position();
        while (n < size) {
            int nr;
            try {
                nr = socketChannel.read(data);
            } catch (Exception e) {
                break;
            }
            if (nr == -1) {
                markInputClosed(socketChannel);
                break;
            }
            n += nr;
        }
        LOGGER.debug("Finish reading data. {} bytes read", n);
        if (n < size) {
            n = -1;
        }
        // System.out.println("EDataBlock(" + _myName + ").read(): returning " +
        // n);
        return n;
    }

    public long read(SocketChannel socketChannel) {
        LOGGER.debug("Beginning read");
        if (readHeader(socketChannel) == -1) {
            markInputClosed(socketChannel);
            return -1;
        }
        return readData(socketChannel, getSize());
    }

    private void markInputClosed(SocketChannel channel) {
        try {
            channel.shutdownInput();
        } catch (IOException e) {
            LOGGER.error("Failed to mark socket {} closed: {}", channel, e.toString());
        }
    }
}
