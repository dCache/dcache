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
package org.globus.ftp.dc;

import java.net.ServerSocket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globus.ftp.DataSink;
import org.globus.ftp.DataSource;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.HostPort;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.extended.GridFTPServerFacade;
import org.globus.ftp.vanilla.BasicServerControlChannel;
import org.globus.ftp.vanilla.FTPServerFacade;

public class TransferThreadManager {

    static Log logger = 
        LogFactory.getLog(TransferThreadManager.class.getName());

    protected SocketPool socketPool;
    protected GridFTPServerFacade facade;
    protected BasicServerControlChannel localControlChannel;
    protected GridFTPSession gSession;
    protected TaskThread taskThread;
    protected int transferThreadCount = 0;
    protected DataChannelFactory dataChannelFactory;

    public TransferThreadManager(SocketPool socketPool,
                                 GridFTPServerFacade facade,
                                 BasicServerControlChannel myControlChannel,
                                 GridFTPSession gSession) {
        this.socketPool = socketPool;
        this.facade = facade;
        this.gSession = gSession;
        this.localControlChannel = myControlChannel;
        this.dataChannelFactory = new GridFTPDataChannelFactory();
    }

    /** 
     * Act as the active side. Connect to the server and 
     * store the newly connected sockets in the socketPool.
     */
    public void activeConnect(HostPort hp, int connections) {
        for (int i = 0; i < connections; i++) {

            SocketBox sbox = new ManagedSocketBox();
            logger.debug("adding new empty socketBox to the socket pool");
            socketPool.add(sbox);

            logger.debug(
                         "connecting active socket "
                         + i
                         + "; total cached sockets = "
                         + socketPool.count());
            
            Task task =
                new GridFTPActiveConnectTask(
                                             hp,
                                             localControlChannel,
                                             sbox,
                                             gSession);
            
            runTask(task);
        }
    }

    /** use only in mode E */
    public void activeClose(TransferContext context, int connections) {

        try {
            
            //this could be improved; for symmetry and performance,
            //make it a separate task class and pass to the taskThread
            for (int i = 0; i < connections; i++) {
                
                SocketBox sbox = socketPool.checkOut();
                GridFTPDataChannel dc = new GridFTPDataChannel(gSession, sbox);
                EBlockImageDCWriter writer = (EBlockImageDCWriter)dc.getDataChannelSink(context);
                writer.setDataStream(sbox.getSocket().getOutputStream());
                // close the socket
                writer.close();
                // do not reuse the socket
                socketPool.remove(sbox);
                sbox.setSocket(null);
                
            }
            
        } catch (Exception e) {
            FTPServerFacade.exceptionToControlChannel(
                e,
                "closing of a reused connection failed",
                localControlChannel);   
        }
    }

    /** 
     * This should be used once the remote active server connected to us.
     * This method starts transfer threads that will
     * read data from the source and send.
     * 
     * @param reusable if set to false, the sockets will not be reused after
     *        the transfer
     */
    public synchronized void startTransfer(DataSource source,
                                           TransferContext context,
                                           int connections,
                                           boolean reusable) 
        throws ServerException {
                        
        // things would get messed up if more than 1 file was transfered 
        // simultaneously with the same transfer manager
        if (transferThreadCount != 0) {
            throw new ServerException(
                            ServerException.PREVIOUS_TRANSFER_ACTIVE);
        }
        
        for (int i = 0; i < connections; i++) {
            logger.debug(
                         "checking out a socket; total cached sockets = "
                         + socketPool.count()
                         + "; free = "
                         + socketPool.countFree()
                         + "; busy = "
                         + socketPool.countBusy());

            SocketBox sbox = socketPool.checkOut();
            if (sbox == null) {
                logger.debug("No free sockets available, aborting.");
                return;
            }
            ((ManagedSocketBox) sbox).setReusable(reusable);

            Task task =
                new ActiveStartTransferTask(source,
                                            localControlChannel,
                                            sbox,
                                            gSession,
                                            dataChannelFactory,
                                            context);
            
            runTask(task);
        }
    }

    /** 
     * This should be used once the remote active server connected to us.
     * This method starts transfer threads that will
     * receive the data and store them in the sink.
     * Because of transfer direction, this method cannot be used with EBLOCK.
     * Therefore the number of connections is fixed at 1.
     * 
     * @param reusable if set to false, the sockets will not be reused after
     *        the transfer
     */
    public synchronized void startTransfer(DataSink sink,
                                           TransferContext context,
                                           int connections,
                                           boolean reusable) 
        throws ServerException {

        // things would get messed up if more than 1 file was transfered
        // simultaneously with the same transfer manager
        if (transferThreadCount != 0) {
            throw new ServerException(
                            ServerException.PREVIOUS_TRANSFER_ACTIVE);
        }

        for (int i = 0; i < connections; i++) {
            logger.debug(
                         "checking out a socket; total cached sockets = "
                         + socketPool.count()
                         + "; free = "
                         + socketPool.countFree()
                         + "; busy = "
                         + socketPool.countBusy());

            SocketBox sbox = socketPool.checkOut();
            if (sbox == null) {
                logger.debug("No free sockets available, aborting.");
                return;
            }
            ((ManagedSocketBox) sbox).setReusable(reusable);

            Task task =
                new ActiveStartTransferTask(
                                            sink,
                                            localControlChannel,
                                            sbox,
                                            gSession,
                                            dataChannelFactory,
                                            context);
            
            runTask(task);
        }
    }
    
    /** 
     * Accept connections from the remote server,
     * and start transfer threads that will read incoming data and store
     * in the sink.
     * 
     * @param connections the number of expected connections
     */
    public synchronized void passiveConnect(DataSink sink,
                                            TransferContext context,
                                            int connections,
                                            ServerSocket serverSocket) 
        throws ServerException {

        // things would get messed up if more than 1 file was transfered 
        // simultaneously with the same transfer manager
        if (transferThreadCount != 0) {
            throw new ServerException(
                            ServerException.PREVIOUS_TRANSFER_ACTIVE);
        }

        for (int i = 0; i < connections; i++) {
            Task task =
                new GridFTPPassiveConnectTask(
                                serverSocket,
                                sink,
                                localControlChannel,
                                gSession,
                                dataChannelFactory,
                                (EBlockParallelTransferContext) context);

            runTask(task);
        }
    }

    /** 
     * Accept connection from the remote server
     * and start transfer thread that will read incoming data and store in 
     * the sink. This method, because of direction of transfer, cannot be
     * used with EBlock. Therefore it is fixed to create only 1 connection.
     */
    public synchronized void passiveConnect(DataSource source,
                                            TransferContext context,
                                            ServerSocket serverSocket) 
        throws ServerException {
        
        // things would get messed up if more than 1 file was transfered 
        // simultaneously with the same transfer manager
        if (transferThreadCount != 0) {
            throw new ServerException(
                            ServerException.PREVIOUS_TRANSFER_ACTIVE);
        }

        Task task =
            new GridFTPPassiveConnectTask(
                             serverSocket,
                             source,
                             localControlChannel,
                             gSession,
                             dataChannelFactory,
                             (EBlockParallelTransferContext) context);

        runTask(task);
        
    }

    public synchronized int getTransferThreadCount() {
        return transferThreadCount;
    }
    
    public synchronized void transferThreadStarting() {
        transferThreadCount++;
        logger.debug("one transfer started, total active = " + 
                     transferThreadCount);
    }

    public synchronized void transferThreadTerminating() {
        transferThreadCount--;
        logger.debug("one transfer terminated, total active = " + 
                     transferThreadCount);         
    }

    /**
       Use this as an interface to the local manager thread.
       This submits the task to the thread queue.
       The thread will perform it when it's ready with other
       waiting tasks.
    **/
    private void runTask(Task task) {
        if (taskThread == null) {
            taskThread = new TaskThread();
        }
        taskThread.runTask(task);
    }

    public void stopTaskThread() {
        if (taskThread != null) {
            taskThread.stop();
            taskThread.join();
            taskThread = null;
        }
    }
    
    public void close() {
        stopTaskThread();
    }
    
}
