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
package org.dcache.xrootd.tpc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;

import java.util.Set;

import org.dcache.util.ByteUnit;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.xrootd.tpc.protocol.messages.InboundChecksumResponse;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_error;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ok;

/**
 * <p>Executes optional checksum verification by requesting the
 *    checksum from the channel and from the source server.</p>
 */
public final class TpcWriteDescriptorHandler extends TpcSourceReadHandler
{
    private static final int DEFAULT_CHUNK = ByteUnit.MiB.toBytes(4);

    private final TpcWriteDescriptor descriptor;

    public TpcWriteDescriptorHandler(TpcWriteDescriptor descriptor)
    {
        this.descriptor = descriptor;
    }

    @Override
    protected void validateChecksum(InboundChecksumResponse response,
                                    ChannelHandlerContext ctx)
    {
        ChannelId id = ctx.channel().id();
        int streamId = client.getStreamId();
        XrootdTpcInfo info = client.getInfo();
        String requestedType = info.getCks();
        ChecksumType type
                        = ChecksumType.getChecksumType(requestedType.toUpperCase());
        Set<Checksum> fileCksums =
                        descriptor.getChannel().getFileAttributes().getChecksums();
        String sourceValue = response.getChecksums().get(requestedType);
        String dCacheValue = null;
        for (Checksum checksum: fileCksums) {
            if (checksum.getType() == type) {
                dCacheValue = checksum.getValue();
                if (sourceValue != null && sourceValue.equals(dCacheValue)) {
                    LOGGER.trace("Checksum query for {} on {}, "
                                                 + "channel {}, stream {}, succeeded.",
                                 info.getLfn(),
                                 info.getSrc(),
                                 id,
                                 streamId);
                    handleDisconnect(kXR_ok, null, ctx);
                    return;
                }
            }
        }

        String error = String.format("Checksum for %s not found or did not match "
                        + "(channel %s, stream %d): source was %s, dcache was %s.",
                                     info.getLfn(),
                                     id,
                                     streamId,
                                     sourceValue,
                                     dCacheValue);
        handleDisconnect(kXR_error, error, ctx);
    }

    @Override
    protected int getChunkSize() {
        return DEFAULT_CHUNK;
    }
}
