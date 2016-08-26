/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

package diskCacheV111.doors;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.LineEncoder;
import io.netty.handler.codec.string.LineSeparator;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandExitException;
import dmg.util.LineWriter;

import org.dcache.cells.AbstractCell;
import org.dcache.poolmanager.PoolManagerHandler;
import org.dcache.util.Args;
import org.dcache.util.BoundedExecutor;
import org.dcache.util.SequentialExecutor;
import org.dcache.util.Transfer;

/**
 * Login cell for line based protocols.
 *
 * <p>To be used with LoginManager. The cell is a Netty handler and expects Netty to pass
 * and accept Strings. These are passed on to an interpreter for processing.
 */
public class NettyLineBasedDoor
    extends AbstractCell implements ChannelInboundHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyLineBasedDoor.class);

    /**
     * Factory for creating the protocol interpreter.
     */
    private final NettyLineBasedInterpreterFactory factory;

    /**
     * Executor for processing commands.
     */
    private final BoundedExecutor commandExecutor;

    /**
     * Executor for the interpreter.
     */
    private final Executor executor;

    /**
     * Shared handler for communicating with pool manager.
     */
    private final PoolManagerHandler poolManager;

    private final Charset charset;

    private final LineSeparator lineSeparator;

    /**
     * Line oriented protocol interpreter.
     */
    private LineBasedInterpreter interpreter;

    /**
     * Cell logging context under which protocol lines are interpreted.
     */
    private CDC cdc;

    /**
     * Netty channel. The life cycle of this cell is bound to this channel.
     */
    private Channel channel;

    private String clientAddress;

    public NettyLineBasedDoor(String cellName, Args args, NettyLineBasedInterpreterFactory factory,
                              ExecutorService executor, PoolManagerHandler poolManagerHandler)
    {
        super(cellName, args, executor);

        this.factory = factory;
        this.executor = executor;
        this.commandExecutor = new SequentialExecutor(executor);
        this.poolManager = poolManagerHandler;

        this.charset = Charset.forName(args.getOption("charset", "UTF-8"));
        String lineSeparator = args.getOption("lineSeparator", "WINDOWS");
        switch (lineSeparator) {
        case "WINDOWS":
            this.lineSeparator = LineSeparator.WINDOWS;
            break;
        case "UNIX":
            this.lineSeparator = LineSeparator.UNIX;
            break;
        default:
            throw new IllegalArgumentException("Invalid line separator value: " + lineSeparator);
        }
    }

    public void messageArrived(NoRouteToCellException e)
    {
        LOGGER.warn(e.getMessage());
    }

    /**
     * Called by the cell infrastructure when the cell has been killed.
     *
     * The socket will be closed by this method. It is quite important
     * that this does not happen earlier, as several threads use the
     * output stream. This is the only place where we can be 100%
     * certain, that all the other threads are done with their job.
     *
     * The method blocks until the worker thread has terminated.
     */
    @Override
    public void stopped()
    {
        channel.close().syncUninterruptibly();

        super.stopped();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("     User Host  : " + clientAddress);
        LineBasedInterpreter interpreter = this.interpreter;
        if (interpreter instanceof CellInfoProvider) {
            ((CellInfoProvider) interpreter).getInfo(pw);
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception
    {
        try (CDC ignored = CDC.reset(getNucleus().getThisAddress())) {
            Transfer.initSession(false, true);
            cdc = new CDC();
        }

        channel = ctx.channel();
        channel.config().setOption(ChannelOption.ALLOW_HALF_CLOSURE, true);
        channel.config().setOption(ChannelOption.TCP_NODELAY, true);
        channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception
    {

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        try (CDC ignored = cdc.restore()) {
            InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
            InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            LineWriter writer = ctx::writeAndFlush;

            clientAddress = remoteAddress.getAddress().getHostAddress();
            LOGGER.debug("Client host: {}", clientAddress);

            interpreter = factory.create(this, getNucleus().getThisAddress(),
                                         remoteAddress, localAddress, writer, executor, poolManager);
            if (interpreter instanceof CellCommandListener) {
                addCommandListener(interpreter);
            }
            if (interpreter instanceof CellMessageReceiver) {
                addMessageListener((CellMessageReceiver) interpreter);
            }
            start().get(); // Blocking to prevent that we process any commands before the cell is alive
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        try (CDC ignored = cdc.restore()) {
            try {
                if (!commandExecutor.isShutdown()) {
                    interpreter.shutdown();
                    commandExecutor.shutdownNow();
                }
                commandExecutor.awaitTermination();
            } catch (InterruptedException e) {
                LOGGER.error("Failed to shut down command processing: {}", e.getMessage());
            }

            kill();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        if (msg instanceof String) {
            commandExecutor.execute(new Command((String) msg));
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
    {

    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
    {
        try (CDC ignored = cdc.restore()) {
            if (evt instanceof ChannelInputShutdownEvent) {
                if (!commandExecutor.isShutdown()) {
                    interpreter.shutdown();
                    commandExecutor.shutdownNow();
                    commandExecutor.awaitTermination();
                }

                kill();
            }
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
    {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        LOGGER.warn(cause.toString());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception
    {
        ChannelPipeline pipeline = ctx.pipeline();
        String self = ctx.name();

        // Decoders
        pipeline.addBefore(self, "frameDecoder", new LineBasedFrameDecoder(65536));
        pipeline.addBefore(self, "stringDecoder", new StringDecoder(charset));

        // Encoder
        pipeline.addBefore(self, "lineEncoder", new LineEncoder(lineSeparator, charset));

        pipeline.addBefore(self, "logger", new LoggingHandler());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception
    {

    }

    private class Command implements Runnable
    {
        private final String command;

        public Command(String command)
        {
            this.command = command;
        }

        @Override
        public void run()
        {
            try (CDC ignored = cdc.restore()) {
                try {
                    interpreter.execute(command);
                } catch (CommandExitException e) {
                    channel.close();
                } catch (RuntimeException e) {
                    LOGGER.error("Bug detected", e);
                }
            }
        }
    }
}
