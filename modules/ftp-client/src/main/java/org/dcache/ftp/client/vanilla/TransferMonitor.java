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
package org.dcache.ftp.client.vanilla;

import org.dcache.ftp.client.MarkerListener;
import org.dcache.ftp.client.PerfMarker;
import org.dcache.ftp.client.GridFTPRestartMarker;
import org.dcache.ftp.client.exception.ServerException;
import org.dcache.ftp.client.exception.UnexpectedReplyCodeException;
import org.dcache.ftp.client.exception.FTPReplyParseException;

import java.io.InterruptedIOException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferMonitor implements Runnable
{

    public static final int
            LOCAL = 1,
            REMOTE = 2;
    private final int side; // source or dest

    private Logger logger = null;

    private final int maxWait;
    private final int ioDelay;

    private final BasicClientControlChannel controlChannel;
    private final TransferState transferState;
    private final MarkerListener mListener;

    private TransferMonitor other;

    private boolean abortable;
    private final Flag aborted = new Flag();

    private Thread thread;

    public TransferMonitor(BasicClientControlChannel controlChannel,
                           TransferState transferState,
                           MarkerListener mListener,
                           int maxWait,
                           int ioDelay,
                           int side)
    {
        logger = LoggerFactory.getLogger(TransferMonitor.class.getName() +
                                         ((side == LOCAL) ? ".Local" : ".Remote"));
        this.controlChannel = controlChannel;
        this.transferState = transferState;
        this.mListener = mListener;
        this.maxWait = maxWait;
        this.ioDelay = ioDelay;
        abortable = true;
        aborted.flag = false;
        this.side = side;
    }

    /**
     * In this class, each instance gets a separate logger which is
     * assigned the name in the constructor.
     * This name is in the form "...GridFTPClient.thread host:port".
     *
     * @return the logger name.
     **/
    public String getLoggerName()
    {
        return logger.toString();
    }

    public void setOther(TransferMonitor other)
    {
        this.other = other;
    }

    /**
     * Abort the tpt transfer
     * but do not close resources
     */
    public synchronized void abort()
    {
        logger.debug("abort");

        if (!this.abortable) {
            return;
        }

        controlChannel.abortTransfer();

        aborted.flag = true;
    }

    private synchronized void done()
    {
        this.abortable = false;
    }

    public void start(boolean blocking)
    {
        if (blocking) {
            this.thread = Thread.currentThread();
            run();
        } else {
            this.thread = new Thread(this);
            this.thread.setName("TransferMonitor" + this.thread.getName());
            this.thread.start();
        }
    }

    @Override
    public void run()
    {

        try {
            // if the other thread had already terminated
            // with an error, behave as if it happened just now.
            if (transferState.hasError()) {
                logger.debug("the other thread terminated before this one started.");
                throw new InterruptedException();
            }

            logger.debug("waiting for 1st reply;  maxWait = " +
                         maxWait + ", ioDelay = " + ioDelay);
            this.controlChannel.waitFor(aborted,
                                        ioDelay,
                                        maxWait);

            logger.debug("reading first reply");
            Reply firstReply = controlChannel.read();

            // 150 Opening BINARY mode data connection.
            // or
            // 125 Data connection already open; transfer starting
            if (Reply.isPositivePreliminary(firstReply)) {
                transferState.transferStarted();
                logger.debug("first reply OK: " + firstReply.toString());

                for (; ; ) {

                    logger.debug("reading next reply");
                    this.controlChannel.waitFor(aborted,
                                                ioDelay);
                    logger.debug("got next reply");
                    Reply nextReply = controlChannel.read();

                    //perf marker
                    if (nextReply.getCode() == 112) {
                        logger.debug("marker arrived: " + nextReply.toString());
                        if (mListener != null) {
                            mListener.markerArrived(
                                    new PerfMarker(nextReply.getMessage()));
                        }
                        continue;
                    }

                    //restart marker
                    if (nextReply.getCode() == 111) {
                        logger.debug("marker arrived: " + nextReply.toString());
                        if (mListener != null) {
                            mListener.markerArrived(
                                    new GridFTPRestartMarker(
                                            nextReply.getMessage()));
                        }
                        continue;
                    }

                    //226 Transfer complete
                    if (nextReply.getCode() == 226) {
                        abortable = false;
                        logger.debug("transfer complete: " + nextReply.toString());
                        break;
                    }

                    // any other reply
                    logger.debug("unexpected reply: " + nextReply.toString());
                    logger.debug("exiting the transfer thread");
                    ServerException e = ServerException.embedUnexpectedReplyCodeException(
                            new UnexpectedReplyCodeException(nextReply),
                            "Server reported transfer failure");

                    transferState.transferError(e);
                    other.abort();
                    break;
                }

            } else {    //first reply negative
                logger.debug("first reply bad: " + firstReply.toString());
                logger.debug("category: " + firstReply.getCategory());
                abortable = false;
                ServerException e = ServerException.embedUnexpectedReplyCodeException(
                        new UnexpectedReplyCodeException(firstReply));

                transferState.transferError(e);
                other.abort();
            }

            logger.debug("thread dying naturally");

        } catch (InterruptedException td) {
            //other transfer thread called abort()
            logger.debug("thread dying of InterruptedException.");
            transferState.transferError(td);
        } catch (InterruptedIOException td) {
            //other transfer thread called abort() which occurred
            //while this thread was performing IO
            logger.debug("thread dying of InterruptedIOException.");
            transferState.transferError(td);
        } catch (IOException e) {
            logger.debug("thread dying of IOException");
            transferState.transferError(e);
            other.abort();

        } catch (FTPReplyParseException rpe) {
            logger.debug("thread dying of FTPReplyParseException");
            ServerException se = ServerException.embedFTPReplyParseException(rpe);
            transferState.transferError(se);
            other.abort();
        } catch (ServerException e) {
            logger.debug("thread dying of timeout");
            transferState.transferError(e);
            other.abort();
        } finally {
            done();
            transferState.transferDone();
        }
    }

}
