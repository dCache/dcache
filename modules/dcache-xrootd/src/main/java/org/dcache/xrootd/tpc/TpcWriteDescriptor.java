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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.dcache.pool.movers.NettyTransferService;
import org.dcache.pool.movers.NettyTransferService.NettyMoverChannel;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.pool.WriteDescriptor;
import org.dcache.xrootd.pool.XrootdPoolRequestHandler;
import org.dcache.xrootd.pool.XrootdTransferService;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.OkResponse;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.XrootdResponse;
import org.dcache.xrootd.tpc.protocol.messages.InboundReadResponse;
import org.dcache.xrootd.tpc.protocol.messages.InboundRedirectResponse;
import org.dcache.xrootd.util.ByteBuffersProvider;
import org.dcache.xrootd.util.FileStatus;
import org.dcache.xrootd.util.ParseException;

import static org.dcache.xrootd.protocol.XrootdProtocol.*;

/**
 * <p>An extension of the WriteDescriptor allowing for delayed response to
 *      a sync request.</p>
 *
 * <p>According to the Xrootd Third Party client protocol, the client calls
 *      sync twice on the destination file after it has called open.</p>
 *
 * <p>The first sync call should return after the copy has begun (i.e., the
 *      client started.)</p>
 *
 * <p>The second sync call should not return until the transfer has completed.</p>
 */
public final class TpcWriteDescriptor extends WriteDescriptor
                implements TpcDelayedSyncWriteHandler
{
    private static final Logger                 LOGGER
                    = LoggerFactory.getLogger(TpcWriteDescriptor.class);

    private final NioEventLoopGroup           group;
    private final ChannelHandlerContext       userResponseCtx;
    private final List<ChannelHandlerFactory> authPlugins;

    /*
     * May be reassigned because of a redirect.
     */
    private XrootdTpcClient client;
    private SyncRequest     syncRequest;
    private boolean         isFirstSync;
    private Integer         transferStatus;

    public TpcWriteDescriptor(NettyTransferService<XrootdProtocolInfo>.NettyMoverChannel channel,
                              boolean posc,
                              ChannelHandlerContext ctx,
                              XrootdTransferService service,
                              String userUrn,
                              XrootdTpcInfo info)
    {
        super(channel, posc);
        userResponseCtx = ctx;
        client = new XrootdTpcClient(userUrn,
                                     info,
                                     this,
                                     service.getThirdPartyShutdownExecutor());
        client.setResponseTimeout(service.getTpcServerResponseTimeoutInSeconds());
        group = service.getThirdPartyClientGroup();
        authPlugins = service.getTpcClientPlugins();
        isFirstSync = true;

        /*
         *  Add any checksum requirement to the channel
         */
        String cksum = info.getCks();
        if (cksum != null) {
            channel.addChecksumType(ChecksumType.getChecksumType(cksum.toUpperCase()));
        }

        /*
         * For dcache as destination, the transfer is initiated by the
         * user client, but we want the mover billing record to reflect
         * the source IP, so we overwrite the protocol info client (unused).
         */
        getChannel().getProtocolInfo()
                    .setSocketAddress(new InetSocketAddress(info.getSrcHost(),
                                                            info.getSrcPort()));
    }

    @Override
    public synchronized void fireDelayedSync(int result, String error)
    {
        int errno = client.getErrno();
        LOGGER.debug("fireDelayedSync (result {}), (error {}), (serverError {}); "
                                     + "syncRequest {}, isFirstSync {}",
                     result, error, client.getError(), syncRequest, isFirstSync);
        transferStatus = result;
        if (syncRequest != null) {
            if (result == kXR_ok && errno == kXR_ok) {
                userResponseCtx.writeAndFlush(new OkResponse<>(syncRequest))
                               .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            } else if (error != null) {
                userResponseCtx.writeAndFlush(
                                new ErrorResponse<>(syncRequest, result, error))
                               .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            } else {
                userResponseCtx.writeAndFlush(
                                new ErrorResponse<>(syncRequest, errno , client.getError()))
                               .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            }
        }
    }

    public synchronized XrootdResponse<StatRequest> handleStat(StatRequest msg)
                    throws XrootdException
    {
        if (client.getError() != null) {
            return new ErrorResponse<>(msg,
                                       client.getErrno(),
                                       client.getError());
        }

        int fd = msg.getFhandle();
        NettyMoverChannel channel = getChannel();
        FileStatus fileStatus;
        try {
            fileStatus = new FileStatus(fd,
                                        channel.size(),
                                        XrootdPoolRequestHandler.DEFAULT_FILESTATUS_FLAGS,
                                        channel.getFileAttributes()
                                               .getModificationTime()
                                                        / 1000);
        } catch (IOException e) {
            String error = String.format("Failed to get channel "
                                                         + "info for %s: %s.",
                                         msg, e.toString());
            throw new XrootdException(kXR_IOError, error);
        }

        return new StatResponse(msg, fileStatus);
    }

    @Override
    public synchronized void redirect(ChannelHandlerContext ctx,
                         InboundRedirectResponse response)
                    throws XrootdException
    {
        try {
            LOGGER.info("redirect {} called for client channel {}, stream {}.",
                            response,
                            client.getChannelFuture().channel().id(),
                            client.getStreamId());
            XrootdTpcClient current = client;
            XrootdTpcInfo currentInfo = current.getInfo();
            XrootdTpcInfo info = response.isReconnect() ? currentInfo:
                            currentInfo.copyForRedirect(response);
            client = new XrootdTpcClient(current.getUserUrn(),
                                         info,
                                         this,
                                         current.getExecutor());
            client.configureRedirects(current);

            try {
                current.shutDown(ctx);
            } catch (InterruptedException e) {
                LOGGER.warn("redirect, shutdown of old client, channel {}, "
                                            + "stream {} was interrupted.",
                            current.getChannelFuture().channel().id(),
                            current.getStreamId());
                return;
            }

            if (!client.canRedirect()) {
                throw new XrootdException(kXR_ServerError, "Client was redirected "
                                + "more than the maximum number of times in the "
                                + "past 10 minutes; quitting.");
            }

            /*
             *  Done on the executor thread, else we risk deadlock because this
             *  method is usually called from the event loop thread.
             */
            client.getExecutor().schedule(()-> {
                try {
                    client.connect(group,
                                   authPlugins,
                                   new TpcWriteDescriptorHandler(this));
                    LOGGER.info("redirect, created and connected new client, "
                                                + "channel {}, stream {}.",
                                client.getChannelFuture().channel().id(),
                                client.getStreamId());
                } catch (InterruptedException e) {
                    LOGGER.warn("redirect, connection of new client, channel {}, "
                                                + "stream {} was interrupted.",
                                client.getChannelFuture().channel().id(),
                                client.getStreamId());
                }
            }, response.getWsec(), TimeUnit.SECONDS);
        } catch (ParseException e) {
            throw new XrootdException(kXR_ArgInvalid, e.getMessage());
        }
    }

    /*
     *  In the case of third-party destination, the first sync call
     *  should start the embedded third-party client; the second
     *  sync should not receive a response until the copy/transfer
     *  has terminated.
     */
    @Override
    public synchronized XrootdResponse<SyncRequest> sync(SyncRequest syncRequest)
                    throws IOException, InterruptedException
    {
        if (client.getError() != null) {
            return new ErrorResponse<>(syncRequest,
                                       client.getErrno() ,
                                       client.getError());
        }

        LOGGER.debug("Request to sync ({}) is for third-party write.",
                     syncRequest);

        if (isFirstSync) {
            /*
             * The tpc should be started now,
             * and OK returned to the caller.
             */
            LOGGER.debug("fireDelayedSync starting TPC client.");

            /*
             * Start the client connection.
             */
            client.connect(group,
                           authPlugins,
                           new TpcWriteDescriptorHandler(this));
            isFirstSync = false;
            return new OkResponse<>(syncRequest);
        }

        if (transferStatus == null) {
            /*
             * Not yet terminated.  Wait for fireDelayedSync call.
             */
            this.syncRequest = syncRequest;
            return null;
        }

        /*
         *  The tpc client has terminated and response was already sent.
         *
         *  Respond to subsequent syncs, if any, normally.
         */
        return super.sync(syncRequest);
    }

    @Override
    public void write(InboundReadResponse inboundReadResponse)
                    throws IOException
    {
        write((ByteBuffersProvider)inboundReadResponse);
    }

    public void shutDown()
    {
        if (client == null) {
            return;
        }

        ChannelFuture future = client.getChannelFuture();
        if (future == null) {
            return;
        }

        Channel channel = future.channel();
        if (channel == null) {
            return;
        }

        ChannelPipeline pipeline = channel.pipeline();
        if (pipeline == null) {
            return;
        }

        ChannelHandlerContext ctx = pipeline.lastContext();
        if (ctx == null) {
            return;
        }

        try {
            client.shutDown(ctx);
        } catch (InterruptedException e) {
            LOGGER.debug("shutDown of tpc client interrupted.");
        }
    }
}
