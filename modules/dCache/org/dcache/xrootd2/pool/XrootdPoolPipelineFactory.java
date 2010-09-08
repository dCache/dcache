package org.dcache.xrootd2.pool;

import static org.jboss.netty.channel.Channels.pipeline;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.dcache.xrootd2.core.XrootdDecoder;
import org.dcache.xrootd2.core.XrootdEncoder;
import org.dcache.xrootd2.core.XrootdHandshakeHandler;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel-pipeline factory used on the pool. Additionally to the normal
 * handlers, an IdleStateHandler is part of its pipelines. This handler serves
 * as a timeout event generator. It is backed by a timer which will cause it
 * to send an IdleStateEvent upstream if the client performed neither read nor
 * write actions on the channel within clientTimeout.
 *
 * If the upstream handler extends IdleStateAwareChannelHandler, it can
 * process this event in its channelIdle() method.
 *
 * On the pool, this is used to disconnect clients that have connected but
 * not opened any files on the mover.
 *
 * @author tzangerl
 *
 */
public class XrootdPoolPipelineFactory implements ChannelPipelineFactory
{
    private static final Logger _log =
        LoggerFactory.getLogger(XrootdPoolPipelineFactory.class);

    /** this must be one shared instance for all pipelines, because otherwise
     * a new thread will be created for every new pipeline having a timer
     */
    private final Timer _timer;
    private final Executor _diskExecutor;
    private final long _clientTimeout;


    public XrootdPoolPipelineFactory(Timer timer,
                                     Executor diskExecutor,
                                     long clientTimeout) {
        _timer = timer;
        _diskExecutor = diskExecutor;
        _clientTimeout = clientTimeout;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("encoder", new XrootdEncoder());
        pipeline.addLast("decoder", new XrootdDecoder());
        if (_log.isDebugEnabled()) {
            pipeline.addLast("logger",
                             new LoggingHandler(XrootdProtocol_3.class));
        }
        pipeline.addLast("handshake",
                         new XrootdHandshakeHandler(XrootdProtocol.DATA_SERVER));
        pipeline.addLast("executor", new ExecutionHandler(_diskExecutor));
        pipeline.addLast("timeout", new IdleStateHandler(_timer,
                                                         0,
                                                         0,
                                                         _clientTimeout,
                                                         TimeUnit.MILLISECONDS));
        pipeline.addLast("transfer", new XrootdPoolRequestHandler());
        return pipeline;
    }
}
