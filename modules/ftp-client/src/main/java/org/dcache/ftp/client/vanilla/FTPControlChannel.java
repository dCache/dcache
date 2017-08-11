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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

import org.dcache.ftp.client.exception.FTPReplyParseException;
import org.dcache.ftp.client.exception.ServerException;
import org.dcache.ftp.client.exception.UnexpectedReplyCodeException;

/**
 * <p>
 * Represents FTP Protocol Interpreter. Encapsulates
 * control channel communication.
 * <p>
 * </p>
 */
public class FTPControlChannel extends BasicClientControlChannel
{

    private static final Logger logger =
            LoggerFactory.getLogger(FTPControlChannel.class);

    public static final String CRLF = "\r\n";

    // used in blocking waitForReply()
    private static final int WAIT_FOREVER = -1;

    protected Socket socket;
    //input stream
    protected BufferedReader ftpIn;
    //raw stream underlying ftpIn
    protected InputStream rawFtpIn;
    //output stream
    protected OutputStream ftpOut;
    protected String host;
    protected final int port;
    //true if connection has already been opened.
    protected boolean hasBeenOpened = false;
    private final boolean ipv6;

    private Reply lastReply;

    public FTPControlChannel(String host, int port)
    {
        this.host = host;
        this.port = port;
        this.ipv6 = (this.host.indexOf(':') != -1);
    }

    public String getHost()
    {
        return this.host;
    }

    public int getPort()
    {
        return this.port;
    }

    public boolean isIPv6()
    {
        return this.ipv6;
    }

    // not intended to be public. you can set streams in the constructor.
    protected void setInputStream(InputStream in)
    {
        rawFtpIn = in;
        ftpIn = new BufferedReader(new InputStreamReader(rawFtpIn));
    }

    protected void setOutputStream(OutputStream out)
    {
        ftpOut = out;
    }

    /**
     * opens the connection and returns after it is ready for communication.
     * Before returning, it intercepts the initial server reply(-ies),
     * and not positive, throws UnexpectedReplyCodeException.
     * After returning, there should be no more queued replies on the line.
     * <p>
     * Here's the sequence for connection establishment (rfc959):
     * <PRE>
     * 120
     * 220
     * 220
     * 421
     * </PRE>
     *
     * @throws IOException     on I/O error
     * @throws ServerException on negative or faulty server reply
     **/
    public void open() throws IOException, ServerException
    {

        if (hasBeenOpened()) {
            throw new IOException("Attempt to open an already opened connection");
        }

        InetAddress allIPs[];

        //depending on constructor used, we may already have streams
        if (!haveStreams()) {
            boolean found = false;
            int i = 0;
            boolean firstPass = true;

            allIPs = InetAddress.getAllByName(host);

            while (!found) {
                try {
                    logger.debug("opening control channel to {} : {}",
                                 allIPs[i], port);
                    InetSocketAddress isa =
                            new InetSocketAddress(allIPs[i], port);

                    socket = new Socket();
                    socket.connect(isa);
                    found = true;
                } catch (IOException ioEx) {
                    logger.debug("failed connecting to {} : {}:{}",
                                 allIPs[i], port, ioEx.toString());
                    i++;
                    if (i == allIPs.length) {
                        if (firstPass) {
                            firstPass = false;
                            i = 0;
                        } else {
                            throw ioEx;
                        }
                    }
                }
            }

            host = socket.getInetAddress().getHostAddress();

            setInputStream(socket.getInputStream());
            setOutputStream(socket.getOutputStream());
        }

        readInitialReplies();

        hasBeenOpened = true;
    }


    //intercepts the initial replies 
    //(that the server sends after opening control ch.)
    protected void readInitialReplies() throws IOException, ServerException
    {
        Reply reply = null;
        try {
            reply = read();
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(
                    rpe,
                    "Received faulty initial reply");
        }

        if (Reply.isPositivePreliminary(reply)) {
            try {
                reply = read();
            } catch (FTPReplyParseException rpe) {
                throw ServerException.embedFTPReplyParseException(
                        rpe,
                        "Received faulty second reply");
            }
        }

        if (!Reply.isPositiveCompletion(reply)) {
            close();
            throw ServerException.embedUnexpectedReplyCodeException(
                    new UnexpectedReplyCodeException(reply),
                    "Server refused connection.");
        }
    }

    /**
     * Returns the last reply received from the server.
     */
    public Reply getLastReply()
    {
        return lastReply;
    }

    /**
     * Closes the control channel
     */
    public void close() throws IOException
    {
        logger.debug("ftp socket closed");
        if (ftpIn != null)
            ftpIn.close();
        if (ftpOut != null)
            ftpOut.close();
        if (socket != null)
            socket.close();

        hasBeenOpened = false;
    }

    private int checkSocketDone(Flag aborted, int ioDelay, int maxWait)
            throws ServerException,
            IOException, InterruptedException
    {
        int oldTOValue = this.socket.getSoTimeout();
        int c = -10;
        int time = 0;
        boolean done = false;

        if (ioDelay <= 0) {
            ioDelay = 2000;
        }

        while (!done) {
            try {
                if (aborted.flag) {
                    throw new InterruptedException();
                }
                this.socket.setSoTimeout(ioDelay);
                ftpIn.mark(2);
                c = ftpIn.read();
                done = true;
            } catch (SocketTimeoutException e) {
                // timeouts will happen
                logger.debug("temp timeout {}", e.toString());
            } catch (Exception e) {
                throw new InterruptedException();
            } finally {
                ftpIn.reset();
                this.socket.setSoTimeout(oldTOValue);
            }
            time += ioDelay;
            if (time > maxWait && maxWait != WAIT_FOREVER) {
                throw new ServerException(ServerException.REPLY_TIMEOUT);
            }
        }

        return c;
    }

    /**
     * Block until one of the conditions are true:
     * <ol>
     * <li> a reply is available in the control channel,
     * <li> timeout (maxWait) expired
     * <li> aborted flag changes to true.
     * </ol>
     * If maxWait == WAIT_FOREVER, never timeout
     * and only check conditions (1) and (3).
     *
     * @param maxWait timeout in miliseconds
     * @param ioDelay frequency of polling the control channel
     *                and checking the conditions
     * @param aborted flag indicating wait aborted.
     **/
    @Override
    public void waitFor(Flag aborted, int ioDelay, int maxWait) throws ServerException,
            IOException, InterruptedException
    {

        int oldTimeout = this.socket.getSoTimeout();

        try {
            int c = 0;
            if (maxWait != WAIT_FOREVER) {
                this.socket.setSoTimeout(maxWait);
            } else {
                this.socket.setSoTimeout(0);
            }

            c = this.checkSocketDone(aborted, ioDelay, maxWait);

            /*
              A bug in the server causes it to append \0 to each reply.
              As the result, we receive this \0 before the next reply.
              The code below handles this case.

            */
            if (c != 0) {
                // if we're here, the server is healthy
                // and the reply is waiting in the buffer
                return;
            }

            // if we're here, we deal with the buggy server.
            // we discarded the \0 and now resume wait.

            logger.debug("Server sent \\0; resume wait");
            try {
                // gotta read past the 0 we just remarked
                c = ftpIn.read();
                c = this.checkSocketDone(aborted, ioDelay, maxWait);
            } catch (SocketTimeoutException e) {
                throw new ServerException(ServerException.REPLY_TIMEOUT);
            } catch (EOFException e) {
                throw new InterruptedException();
            }
        } finally {
            this.socket.setSoTimeout(oldTimeout);
        }
    }


    /**
     * Block until a reply is available in the control channel.
     *
     * @return the first unread reply from the control channel.
     * @throws IOException            on I/O error
     * @throws FTPReplyParseException on malformatted server reply
     **/
    @Override
    public Reply read()
            throws ServerException, IOException, FTPReplyParseException, EOFException
    {

        Reply reply = new Reply(ftpIn);
        //System.out.println("FTP IN string "+reply.toString());
        if (logger.isDebugEnabled()) {
            logger.debug("Control channel received: {}", reply);
        }
        lastReply = reply;
        return reply;
    }

    @Override
    public void abortTransfer()
    {
    }

    /**
     * Sends the command over the control channel.
     * Do not wait for reply.
     *
     * @param cmd FTP command
     * @throws java.io.IOException on I/O error
     */
    public void write(Command cmd)
            throws IOException, IllegalArgumentException
    {
        //we delete the initial reply when the first command is sent
        if (cmd == null) {
            throw new IllegalArgumentException("null argument: cmd");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Control channel sending: {}", cmd);
        }
        ftpOut.write(cmd.toString().getBytes(StandardCharsets.US_ASCII));
        ftpOut.flush();
    }

    /**
     * Write the command to the control channel,
     * block until reply arrives and return the reply.
     * Before calling this method make sure that no old replies are
     * waiting on the control channel. Otherwise the reply returned
     * may not be the reply to this command.
     *
     * @param cmd FTP command
     * @return the first reply that waits in the control channel
     * @throws java.io.IOException    on I/O error
     * @throws FTPReplyParseException on bad reply format
     **/
    public Reply exchange(Command cmd)
            throws ServerException, IOException, FTPReplyParseException
    {
        // send the command
        write(cmd);
        // get the reply
        return read();
    }

    /**
     * Write the command to the control channel,
     * block until reply arrives and check if the command
     * completed successfully (reply code 200).
     * If so, return the reply, otherwise throw exception.
     * Before calling this method make sure that no old replies are
     * waiting on the control channel. Otherwise the reply returned
     * may not be the reply to this command.
     *
     * @param cmd FTP command
     * @return the first reply that waits in the control channel
     * @throws java.io.IOException          on I/O error
     * @throws FTPReplyParseException       on bad reply format
     * @throws UnexpectedReplyCodeException if reply is not a positive
     *                                      completion reply (code 200)
     **/
    public Reply execute(Command cmd)
            throws
            ServerException,
            IOException,
            FTPReplyParseException,
            UnexpectedReplyCodeException
    {

        Reply reply = exchange(cmd);
        // check for positive reply
        if (!Reply.isPositiveCompletion(reply)) {
            throw new UnexpectedReplyCodeException(reply);
        }
        return reply;
    }

    public InetSocketAddress getLocalAddress()
    {
        return (InetSocketAddress) socket.getLocalSocketAddress();
    }

    public InetSocketAddress getRemoteAddress()
    {
        return (InetSocketAddress) socket.getRemoteSocketAddress();
    }

    protected boolean hasBeenOpened()
    {
        return hasBeenOpened;
    }

    protected boolean haveStreams()
    {
        return (ftpIn != null && ftpOut != null);
    }

} // end StandardPI
