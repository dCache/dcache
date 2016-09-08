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

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.LineEncoder;
import io.netty.handler.codec.string.LineSeparator;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.logging.LoggingHandler;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginCellFactory;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolManagerHandlerSubscriber;
import org.dcache.util.Args;
import org.dcache.util.CDCThreadFactory;
import org.dcache.util.Option;
import org.dcache.util.OptionParser;

public class NettyLineBasedDoorFactory extends AbstractService implements LoginCellFactory
{
    private final CellEndpoint parentEndpoint;
    private final String parentCellName;
    private final Args args;
    private final NettyLineBasedInterpreterFactory factory;
    private ExecutorService executor;
    private PoolManagerHandlerSubscriber poolManagerHandler;
    private NioEventLoopGroup socketGroup;

    @Option(name = "poolManager",
            description = "Well known name of the pool manager",
            defaultValue = "PoolManager")
    protected CellPath poolManager;

    @Option(name = "poolManagerTimeout",
            defaultValue = "1500")
    protected int poolManagerTimeout;

    @Option(name = "poolManagerTimeoutUnit",
            defaultValue = "SECONDS")
    protected TimeUnit poolManagerTimeoutUnit;

    @Option(name = "poolTimeout",
            defaultValue = "300")
    protected int poolTimeout;

    @Option(name = "poolTimeoutUnit",
            defaultValue = "SECONDS")
    protected TimeUnit poolTimeoutUnit;

    public NettyLineBasedDoorFactory(NettyLineBasedInterpreterFactory factory, Args args, CellEndpoint parentEndpoint,
                                     String parentCellName)
    {
        this.factory = factory;
        this.parentEndpoint = parentEndpoint;
        this.parentCellName = parentCellName;
        this.args = args;

        new OptionParser(args).inject(this);
    }

    @Override
    public String getName()
    {
        return factory.getClass().getSimpleName();
    }

    @Override
    public Cell newCell(Socket socket) throws InvocationTargetException
    {
        NettyLineBasedDoor door = new NettyLineBasedDoor(parentCellName + "*", args, factory, executor, poolManagerHandler);

        NioSocketChannel channel = new NioSocketChannel(socket.getChannel());

        ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast("door", door);

        socketGroup.register(channel);

        return door;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("  Interpreter    : " + factory.getClass());
    }

    @Override
    protected void doStart()
    {
        executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat(parentCellName + "-%d").build());

        poolManagerHandler = new PoolManagerHandlerSubscriber();
        poolManagerHandler.setPoolManager(new CellStub(parentEndpoint, poolManager, poolManagerTimeout, poolManagerTimeoutUnit));
        poolManagerHandler.start();
        poolManagerHandler.afterStart();

        socketGroup = new NioEventLoopGroup(0, new CDCThreadFactory(new ThreadFactoryBuilder().setNameFormat(parentCellName + "-io-%d").build()));

        notifyStarted();
    }

    @Override
    protected void doStop()
    {
        socketGroup.shutdownGracefully(500, 2000, TimeUnit.MILLISECONDS).syncUninterruptibly();
        poolManagerHandler.beforeStop();
        executor.shutdown();
        notifyStopped();
    }
}
