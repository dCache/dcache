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
package org.globus.ftp.vanilla;

import java.net.Socket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.globus.net.SocketFactory;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.exception.FTPReplyParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Represents FTP Protocol Interpreter. Encapsulates
 * control channel communication.
 *
 * </p>
 */
public class FTPControlChannel extends BasicClientControlChannel {

    private static Log logger =
        LogFactory.getLog(FTPControlChannel.class.getName());

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
    protected int port;
    //true if connection has already been opened.
    protected boolean hasBeenOpened = false;
    private boolean ipv6 = false;
    
    private Reply lastReply;

    public FTPControlChannel(String host, int port) {
        this.host = host;
        this.port = port;
        this.ipv6 = (this.host.indexOf(':') != -1);
    }

    /** 
     * Using this constructor, you can initialize an instance that does not 
     * talk directly to the socket. If you use this constructor using streams
     * that belong to an active connection, there's no need to call open()
     * afterwards.
     **/
    public FTPControlChannel(InputStream in, OutputStream out) {
        setInputStream(in);
        setOutputStream(out);
    }
    
    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public boolean isIPv6() {
        return this.ipv6;
    }

    protected BufferedReader getBufferedReader() {
        return ftpIn;
    }
    
    protected OutputStream getOutputStream() {
        return ftpOut;
    }

    // not intended to be public. you can set streams in the constructor.
    protected void setInputStream(InputStream in) {
        rawFtpIn = in;
        ftpIn = new BufferedReader(new InputStreamReader(rawFtpIn));
    }

    protected void setOutputStream(OutputStream out) {
        ftpOut = out;
    }

    /**
     * opens the connection and returns after it is ready for communication.
     * Before returning, it intercepts the initial server reply(-ies),
     * and not positive, throws UnexpectedReplyCodeException.
     * After returning, there should be no more queued replies on the line.
     *
     * Here's the sequence for connection establishment (rfc959):
     * <PRE>
     *     120
     *         220
     *     220
     *     421
     *</PRE>
     * @throws IOException on I/O error
     * @throws ServerException on negative or faulty server reply 
     **/
    public void open() throws IOException, ServerException {

        if (hasBeenOpened()) {
            throw new IOException("Attempt to open an already opened connection");
        }

        InetAddress                     allIPs[];

        //depending on constructor used, we may already have streams
        if (!haveStreams()) {
            boolean                     found = false;
            int                         timeout = 30000;
            int                         i = 0;
            boolean                     firstPass = true;
            
            String toS = System.getProperty("org.globus.ftp.openTO");
            if(toS != null)
            {
                try
                {
                    timeout = Integer.parseInt(toS);
                }
                catch(NumberFormatException ex)
                {
                    throw new NumberFormatException("Invalid value for property "
                        + "org.globus.ftp.openTO (" + toS + "). Must be numeric.");
                }
            }
            else
            {
                timeout = 0;
                firstPass = false;
            }


            allIPs = InetAddress.getAllByName(host);

            while(!found)
            {
                try
                {
                    logger.debug("opening control channel to " 
                        + allIPs[i] + " : " + port);
                    InetSocketAddress isa = 
                        new InetSocketAddress(allIPs[i], port);

                    socket = new Socket();
                    socket.connect(isa, timeout);
                    found = true;
                }
                catch(IOException ioEx)
                {
                    logger.debug("failed connecting to  " 
                        + allIPs[i] + " : " + port +":"+ioEx);
                    i++;
                    if(i == allIPs.length)
                    {
                        if(firstPass)
                        {
                            firstPass = false;
                            i = 0;
                            timeout = 0; // next time let system time it out
                        }
                        else
                        {
                            throw ioEx;
                        }
                    }
                }
            }
            
            String pv = System.getProperty("org.globus.ftp.IPNAME");
            if(pv != null)
            {
                host = socket.getInetAddress().getHostAddress();
            }
            else
            {
                host = socket.getInetAddress().getCanonicalHostName();
            }

            setInputStream(socket.getInputStream());
            setOutputStream(socket.getOutputStream());
        }

        readInitialReplies();

        hasBeenOpened = true;
    }
    

    //intercepts the initial replies 
    //(that the server sends after opening control ch.)
    protected void readInitialReplies() throws IOException, ServerException {
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
    public Reply getLastReply() {
        return lastReply;
    }

    /**
     * Closes the control channel
     */
    public void close() throws IOException {
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

        if (ioDelay <= 0)
        {
            ioDelay = 2000;
        }

        while(!done)
        {
            try
            {
                if (aborted.flag)
                {
                    throw new InterruptedException();
                }
                this.socket.setSoTimeout(ioDelay);
                ftpIn.mark(2);
                c = ftpIn.read();
                done = true;
            }
            catch (SocketTimeoutException e)
            {
                // timeouts will happen
                logger.debug("temp timeout" + e);
            }
            catch (Exception e)
            {
                throw new InterruptedException();
            }
            finally
            {
	    		ftpIn.reset();
                this.socket.setSoTimeout(oldTOValue);
            }
            time += ioDelay;
            if(time > maxWait && maxWait != WAIT_FOREVER)
            {
                throw new ServerException(ServerException.REPLY_TIMEOUT);
            }
        }

        return c;
    }

    /**
       Block until one of the conditions are true:
       <ol>
       <li> a reply is available in the control channel,
       <li> timeout (maxWait) expired
       <li> aborted flag changes to true.
       </ol>
       If maxWait == WAIT_FOREVER, never timeout
       and only check conditions (1) and (3).
       @param maxWait timeout in miliseconds
       @param ioDelay frequency of polling the control channel
       and checking the conditions
       @param aborted flag indicating wait aborted.
    **/
	public void waitFor(Flag aborted, int ioDelay, int maxWait) throws ServerException,
			IOException, InterruptedException {

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
	}
    
    
    /**
     * Block until a reply is available in the control channel.
     * @return the first unread reply from the control channel.
     * @throws IOException on I/O error
     * @throws FTPReplyParseException on malformatted server reply
     **/
    public Reply read()
        throws ServerException, IOException, FTPReplyParseException {
        Reply reply = new Reply(ftpIn);
        if (logger.isDebugEnabled()) {
            logger.debug("Control channel received: " + reply);
        }
        lastReply = reply;
        return reply;
    }

    public void abortTransfer() {
    }
    
    /**
     * Sends the command over the control channel.
     * Do not wait for reply.
     * @throws java.io.IOException on I/O error
     * @param cmd FTP command
     */
    public void write(Command cmd)
        throws IOException, IllegalArgumentException {
        //we delete the initial reply when the first command is sent
        if (cmd == null) {
            throw new IllegalArgumentException("null argument: cmd");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Control channel sending: " + cmd);
        }
        writeStr(cmd.toString());
    }
    
    /**
     * Write the command to the control channel,
     * block until reply arrives and return the reply. 
     * Before calling this method make sure that no old replies are
     * waiting on the control channel. Otherwise the reply returned
     * may not be the reply to this command.
     * @throws java.io.IOException on I/O error
     * @throws FTPReplyParseException on bad reply format
     * @param cmd FTP command
     * @return the first reply that waits in the control channel
     **/
    public Reply exchange(Command cmd)
        throws ServerException, IOException, FTPReplyParseException {
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
     * @throws java.io.IOException on I/O error
     * @throws FTPReplyParseException on bad reply format
     * @throws UnexpectedReplyCodeException if reply is not a positive
     *         completion reply (code 200)
     * @param cmd FTP command
     * @return the first reply that waits in the control channel
     **/
    public Reply execute(Command cmd)
        throws
            ServerException,
            IOException,
            FTPReplyParseException,
            UnexpectedReplyCodeException {
        
        Reply reply = exchange(cmd);
        // check for positive reply
        if (!Reply.isPositiveCompletion(reply)) {
            throw new UnexpectedReplyCodeException(reply);
        }
        return reply;
    }
    
    protected void writeln(String msg) throws IOException {
        writeStr(msg + CRLF);
    }
    
    protected void writeStr(String msg) throws IOException {
        ftpOut.write(msg.getBytes());
        ftpOut.flush();
    }
    
    protected boolean hasBeenOpened() {
        return hasBeenOpened;
    }
    
    protected boolean haveStreams() {
        return (ftpIn != null && ftpOut != null);
    }
    
} // end StandardPI
