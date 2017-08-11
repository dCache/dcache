/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dcache.ftp.client.extended;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;

import org.dcache.ftp.client.DataChannelAuthentication;
import org.dcache.ftp.client.DataSink;
import org.dcache.ftp.client.DataSource;
import org.dcache.ftp.client.GridFTPSession;
import org.dcache.ftp.client.HostPort;
import org.dcache.ftp.client.HostPort6;
import org.dcache.ftp.client.HostPortList;
import org.dcache.ftp.client.Options;
import org.dcache.ftp.client.RetrieveOptions;
import org.dcache.ftp.client.Session;
import org.dcache.ftp.client.dc.EBlockImageDCWriter;
import org.dcache.ftp.client.dc.EBlockParallelTransferContext;
import org.dcache.ftp.client.dc.ManagedSocketBox;
import org.dcache.ftp.client.dc.SocketBox;
import org.dcache.ftp.client.dc.SocketOperator;
import org.dcache.ftp.client.dc.SocketPool;
import org.dcache.ftp.client.dc.StripeContextManager;
import org.dcache.ftp.client.dc.TransferContext;
import org.dcache.ftp.client.dc.TransferThreadManager;
import org.dcache.ftp.client.exception.ClientException;
import org.dcache.ftp.client.exception.DataChannelException;
import org.dcache.ftp.client.vanilla.FTPControlChannel;
import org.dcache.ftp.client.vanilla.FTPServerFacade;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;

public class GridFTPServerFacade extends FTPServerFacade
{

    private static final Logger logger =
            LoggerFactory.getLogger(GridFTPServerFacade.class);

    // utility alias to session
    protected GridFTPSession gSession = null;
    protected SocketPool socketPool = null;
    protected TransferThreadManager transferThreadManager = null;

    // current transfer, if striped retrieve, is associated to this manager
    // (striped store does not suffer for EOD complications and don't need
    // manager)
    protected StripeContextManager stripeRetrContextManager = null;

    public GridFTPServerFacade(FTPControlChannel remoteControlChannel)
    {
        super(remoteControlChannel);
        gSession = new GridFTPSession();
        session = gSession;
        // make sure this doesn't get used
        dataChannelFactory = null;
        socketPool = new SocketPool();
        transferThreadManager = createTransferThreadManager();
    }

    public void setDataChannelProtection(int protection)
    {
        gSession.dataChannelProtection = protection;
    }

    public void setDataChannelAuthentication(DataChannelAuthentication authentication)
    {
        gSession.dataChannelAuthentication = authentication;
    }

    @Override
    public void setOptions(Options opts)
    {
        if (opts instanceof RetrieveOptions) {
            gSession.parallel =
                    ((RetrieveOptions) opts).getStartingParallelism();
            logger.debug("parallelism set to {}", gSession.parallel);
        }
    }

    @Override
    protected void transferAbort()
    {
        if (session.serverMode == Session.SERVER_PASSIVE) {
            unblockServer();
            transferThreadManager.stopTaskThread();
        }
    }

    /**
     * All sockets opened when this server was active
     * should send a special EBlock header before closing.
     */
    private void closeOutgoingSockets() throws ClientException
    {

        SocketOperator op = new SocketOperator()
        {
            @Override
            public void operate(SocketBox sb) throws Exception
            {
                if (((ManagedSocketBox) sb).isReusable()) {
                    Socket s = sb.getSocket();
                    if (s != null) {
                        // write the closing Eblock and close the socket
                        EBlockImageDCWriter.close(
                                new DataOutputStream(s.getOutputStream()));
                    }
                }
            }
        };

        try {
            socketPool.applyToAll(op);
        } catch (IOException e) {
            // ignore - sometimes server might close the socket
        } catch (Exception e) {
            ClientException ce =
                    new ClientException(ClientException.SOCKET_OP_FAILED);
            ce.setRootCause(e);
            throw ce;
        }
    }

    @Override
    public void setActive(HostPort hp)
            throws UnknownHostException, ClientException, IOException
    {
        if (logger.isDebugEnabled()) {
            logger.debug("hostport: {} {}", hp.getHost(), hp.getPort());
        }

        if (session.serverMode == Session.SERVER_ACTIVE) {
            closeOutgoingSockets();
        }

        socketPool.flush();

        session.serverMode = Session.SERVER_ACTIVE;
        // may be needed later, if parallelism increases and
        // new connections need to be open
        this.remoteServerAddress = hp;

        transferThreadManager.activeConnect(hp, gSession.parallel);
    }

    public void setStripedActive(HostPortList hpl)
            throws UnknownHostException, IOException
    {
        if (hpl == null) {
            throw new IllegalArgumentException("null HostPortList");
        }
        int stripes = hpl.size();
        if (stripes < 1) {
            throw new IllegalArgumentException("empty HostPortList");
        }

        socketPool.flush(); // = new SocketBox[pathes * stripes];

        //create context manager that will be used by retrieve()
        this.stripeRetrContextManager =
                new StripeContextManager(stripes, socketPool, this);

        int pathes = gSession.parallel;
        gSession.serverMode = GridFTPSession.SERVER_EACT;

        for (int stripe = 0; stripe < stripes; stripe++) {
            transferThreadManager.activeConnect(hpl.get(stripe), pathes);

        }
    }

    @Override
    public HostPort setPassive(PortRange range, int queue) throws IOException
    {
        // remove existing sockets, if any
        socketPool.flush();

        return super.setPassive(range, queue);
    }

    public HostPortList setStripedPassive() throws IOException
    {
        return setStripedPassive(new PortRange(0), DEFAULT_QUEUE);
    }

    public HostPortList setStripedPassive(PortRange range, int queue)
            throws IOException
    {

        // remove existing sockets, if any
        socketPool.flush();

        if (serverSocket == null) {
            ServerSocketChannel channel = ServerSocketChannel.open();
            range.bind(channel.socket(), queue);
            serverSocket = channel.socket();
        }

        gSession.serverMode = GridFTPSession.SERVER_EPAS;
        gSession.serverAddressList = new HostPortList();

        String address = NetworkUtils.getLocalAddress(InetAddress.getByName(remoteControlChannel.getHost())).getHostAddress();
        int localPort = serverSocket.getLocalPort();

        HostPort hp = null;
        if (remoteControlChannel.isIPv6()) {
            String version = HostPort6.getIPAddressVersion(address);
            hp = new HostPort6(version, address, localPort);
        } else {
            hp = new HostPort(address, localPort);
        }

        gSession.serverAddressList.add(hp);

        logger.debug("started single striped passive server at port {}",
                     gSession.serverAddressList.get(0).getPort());

        return gSession.serverAddressList;
    }

    /**
     * Store the data from the data channel to the data sink.
     * Does not block.
     * If operation fails, exception might be thrown via local control channel.
     *
     * @param sink source of data
     **/
    @Override
    public void store(DataSink sink)
    {
        try {

            localControlChannel.resetReplyCount();

            if (session.transferMode != GridFTPSession.MODE_EBLOCK) {
                //
                // no EBLOCK
                //
                EBlockParallelTransferContext context =
                        (EBlockParallelTransferContext) createTransferContext();
                context.setEodsTotal(0);

                if (session.serverMode == Session.SERVER_PASSIVE) {
                    transferThreadManager.passiveConnect(
                            sink,
                            context,
                            1,
                            serverSocket);

                } else {
                    //1 non reusable connection
                    transferThreadManager.startTransfer(
                            sink,
                            context,
                            1,
                            ManagedSocketBox.NON_REUSABLE);
                }

            } else if (
                    session.serverMode != GridFTPSession.SERVER_EPAS
                    && session.serverMode != GridFTPSession.SERVER_PASSIVE) {
                // 
                // EBLOCK, local server not passive
                //
                exceptionToControlChannel(
                        new DataChannelException(
                                DataChannelException.BAD_SERVER_MODE),
                        "refusing to store with active mode");
            } else {
                //
                // EBLOCK, local server passive
                //

                // data channels will
                // share this transfer context
                EBlockParallelTransferContext context =
                        (EBlockParallelTransferContext) createTransferContext();

                // we are the passive side, so we don't really get to decide
                // how many connections will be used
                int willReuseConnections = socketPool.countFree();
                int needNewConnections = 0;
                if (gSession.parallel > willReuseConnections) {
                    needNewConnections = gSession.parallel - willReuseConnections;
                }

                logger.debug("will reuse {}  connections and start {}  new ones.",
                        willReuseConnections,
                        needNewConnections);

                transferThreadManager.startTransfer(
                        sink,
                        context,
                        willReuseConnections,
                        ManagedSocketBox.REUSABLE);

                if (needNewConnections > 0) {
                    transferThreadManager.passiveConnect(
                            sink,
                            context,
                            needNewConnections,
                            serverSocket);
                }
            }
        } catch (Exception e) {
            exceptionToControlChannel(e, "ocurred during store()");
        }
    }

    /**
     * Retrieve the data from the data source and write to the data channel.
     * This method does not block.
     * If operation fails, exception might be thrown via local control channel.
     *
     * @param source source of data
     **/
    @Override
    public void retrieve(DataSource source)
    {
        try {
            localControlChannel.resetReplyCount();

            if (session.transferMode != GridFTPSession.MODE_EBLOCK) {

                //
                // No EBLOCK
                //

                EBlockParallelTransferContext context =
                        (EBlockParallelTransferContext) createTransferContext();
                context.setEodsTotal(0);

                logger.debug("starting outgoing transfer without mode E");
                if (session.serverMode == Session.SERVER_PASSIVE) {

                    transferThreadManager.passiveConnect(source, context, serverSocket);

                } else {

                    transferThreadManager.startTransfer(
                            source,
                            context,
                            1,
                            ManagedSocketBox.NON_REUSABLE);
                }

            } else if (session.serverMode == Session.SERVER_ACTIVE) {

                //
                // EBLOCK, no striping
                //

                // data channels will share this transfer context
                EBlockParallelTransferContext context =
                        (EBlockParallelTransferContext) createTransferContext();

                int total = gSession.parallel;
                //we should send as many EODS as there are parallel streams
                context.setEodsTotal(total);

                int free = socketPool.countFree();
                int willReuseConnections = (total > free) ? free : total;
                int willCloseConnections = (free > total) ? free - total : 0;
                int needNewConnections = (total > free) ? total - free : 0;

                logger.debug("will reuse {} connections, start {}  new ones, and close {}", willReuseConnections,
                             needNewConnections, willCloseConnections);

                if (needNewConnections > 0) {
                    transferThreadManager.activeConnect(this.remoteServerAddress,
                                                        needNewConnections);
                }

                if (willCloseConnections > 0) {
                    transferThreadManager.activeClose(context,
                                                      willCloseConnections);
                }

                transferThreadManager.startTransfer(
                        source,
                        context,
                        willReuseConnections + needNewConnections,
                        ManagedSocketBox.REUSABLE);

            } else if (session.serverMode == GridFTPSession.SERVER_EACT) {

                // 
                // EBLOCK, striping
                //

                if (stripeRetrContextManager == null) {
                    throw new IllegalStateException();
                }

                int stripes = stripeRetrContextManager.getStripes();

                for (int stripe = 0; stripe < stripes; stripe++) {

                    EBlockParallelTransferContext context =
                            stripeRetrContextManager.getStripeContext(stripe);
                    context.setEodsTotal(gSession.parallel);

                    transferThreadManager.startTransfer(
                            source,
                            context,
                            gSession.parallel,
                            ManagedSocketBox.REUSABLE);

                }

            } else {

                //
                // EBLOCK and local server not active
                //

                throw new DataChannelException(
                        DataChannelException.BAD_SERVER_MODE);
            }
        } catch (Exception e) {
            exceptionToControlChannel(e, "ocurred during retrieve()");
        }
    }

    //override
    @Override
    public void abort() throws IOException
    {
        super.abort();
        if (socketPool != null) {
            socketPool.flush();
        }
    }

    //override
    @Override
    public void close() throws IOException
    {
        super.close();
        if (transferThreadManager != null) {
            transferThreadManager.close();
        }
    }

    @Override
    protected TransferContext createTransferContext()
    {
        EBlockParallelTransferContext context =
                new EBlockParallelTransferContext();
        context.setSocketPool(socketPool);
        context.setTransferThreadManager(this.transferThreadManager);
        return context;
    }

    public TransferThreadManager createTransferThreadManager()
    {
        return new TransferThreadManager(socketPool,
                                         this,
                                         localControlChannel,
                                         gSession);
    }

}
