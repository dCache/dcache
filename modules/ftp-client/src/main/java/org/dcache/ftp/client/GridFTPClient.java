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
package org.dcache.ftp.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Vector;

import org.dcache.dss.DssContextFactory;
import org.dcache.ftp.client.exception.ClientException;
import org.dcache.ftp.client.exception.FTPException;
import org.dcache.ftp.client.exception.FTPReplyParseException;
import org.dcache.ftp.client.exception.ServerException;
import org.dcache.ftp.client.exception.UnexpectedReplyCodeException;
import org.dcache.ftp.client.extended.GridFTPControlChannel;
import org.dcache.ftp.client.extended.GridFTPServerFacade;
import org.dcache.ftp.client.vanilla.Command;
import org.dcache.ftp.client.vanilla.FTPControlChannel;
import org.dcache.ftp.client.vanilla.FTPServerFacade;
import org.dcache.ftp.client.vanilla.Reply;
import org.dcache.util.PortRange;
import org.dcache.util.Version;


/**
 * This is the main user interface for GridFTP operations.
 * Use this class for client - server or third party transfers
 * with mode E, parallelism, markers, striping or GSI authentication.
 * Consult the manual for general usage.
 * <br><b>Note:</b> If using with GridFTP servers operations like
 * {@link #setMode(int) setMode()}, {@link #setType(int) setType()},
 * {@link #setDataChannelProtection(int) setDataChannelProtection()},
 * and {@link #setDataChannelAuthentication(DataChannelAuthentication)
 * setDataChannelAuthentication()} that affect data channel settings
 * <b>must</b> be called before passive or active data channel mode is set.
 **/
public class GridFTPClient extends FTPClient
{

    private static Logger logger =
            LoggerFactory.getLogger(GridFTPClient.class);

    private static final Version VERSION = Version.of(GridFTPClient.class);

    //utility alias to session and localServer
    protected final GridFTPSession gSession;
    protected final String expectedHostName;
    protected GridFTPServerFacade gLocalServer;
    protected String usageString;

    /**
     * Constructs client and connects it to the remote server.
     *
     * @param host remote server host
     * @param port remote server port
     */
    public GridFTPClient(String host, int port)
            throws IOException, ServerException
    {
        gSession = new GridFTPSession();
        session = gSession;

        expectedHostName = host;
        controlChannel = new FTPControlChannel(host, port);
        controlChannel.open();

        gLocalServer = new GridFTPServerFacade(controlChannel);
        localServer = gLocalServer;
        gLocalServer.authorize();
        this.useAllo = true;

        setUsageInformation("dCache", VERSION.getVersion());
    }

    /**
     * Establishes a secure and authenticated context with the server.
     *
     * @param factory    factory for creating the DssContext of the secure session
     * @throws IOException     on i/o error
     * @throws ServerException on server refusal or faulty server behavior
     */
    public void authenticate(DssContextFactory factory)
            throws IOException, ServerException
    {
        authenticate(factory, null);
    }

    public void setUsageInformation(
            String appName,
            String appVer)
    {
        usageString = "CLIENTINFO appname=" + appName + ";appver=" + appVer + ";schema=gsiftp;";
    }

    /**
     * Establishes a secure and authenticated context with the server.
     *
     * @param factory    factory for creating the DssContext of the secure session
     * @param username   specific username to authenticate as.
     * @throws IOException     on i/o error
     * @throws ServerException on server refusal or faulty server behavior
     */
    public void authenticate(DssContextFactory factory, String username)
            throws IOException, ServerException
    {
        try {
            // authenticate
            GridFTPControlChannel gridFTPControlChannel = new GridFTPControlChannel(controlChannel, factory, expectedHostName);

            //from now on, the commands and replies
            //are protected and pass through gsi wrapped socket

            // login
            try {
                Reply reply = gridFTPControlChannel.exchange(new Command("USER", (username == null) ? ":globus-mapping:" : username));
                // wu-gsiftp sends intermediate code while
                // gssftp send completion reply code
                if (!Reply.isPositiveCompletion(reply) && !Reply.isPositiveIntermediate(reply)) {
                    throw ServerException.embedUnexpectedReplyCodeException(
                            new UnexpectedReplyCodeException(reply), "User authorization failed.");
                }
            } catch (FTPReplyParseException rpe) {
                throw ServerException.embedFTPReplyParseException(rpe, "Received faulty reply to USER command.");
            }

            try {
                Reply reply = gridFTPControlChannel.exchange(new Command("PASS", "dummy"));;
                if (!Reply.isPositiveCompletion(reply)) {
                    throw ServerException.embedUnexpectedReplyCodeException(
                            new UnexpectedReplyCodeException(reply),
                            "Bad password.");
                }
            } catch (FTPReplyParseException rpe) {
                throw ServerException.embedFTPReplyParseException(rpe, "Received faulty reply to PASS command.");
            }

            // quietly send version information to the server.
            // ignore errors
            try {
                this.site(usageString);
            } catch (Exception ex) {
            }

            this.gSession.authorized = true;
            this.username = username;
            this.controlChannel = gridFTPControlChannel;
        } catch (ServerException | IOException e) {
            close();
            throw e;
        }
    }

    /**
     * Performs remote directory listing like
     * {@link FTPClient#list(String, String) FTPClient.list()}.
     * <b>Note:</b> This method cannot be used
     * in conjunction with parallelism or striping; set parallelism to
     * 1 before calling it. Otherwise, use
     * {@link FTPClient#list(String, String, DataSink) FTPClient.list()}.
     * Unlike in vanilla FTP, here IMAGE mode is allowed.
     * For more documentation, look at FTPClient.
     */
    @Override
    public Vector list(String filter, String modifier)
            throws ServerException, ClientException, IOException
    {
        if (gSession.parallel > 1) {
            throw new ClientException(
                    ClientException.BAD_MODE,
                    "list cannot be called with parallelism");
        }
        return super.list(filter, modifier);
    }

    /**
     * Performs remote directory listing like
     * {@link FTPClient#nlist(String) FTPClient.nlist()}.
     * <b>Note:</b> This method cannot be used
     * in conjunction with parallelism or striping; set parallelism to
     * 1 before calling it. Otherwise, use
     * {@link FTPClient#nlist(String, DataSink) FTPClient.nlist()}.
     * Unlike in vanilla FTP, here IMAGE mode is allowed.
     * For more documentation, look at FTPClient.
     */
    @Override
    public Vector nlist(String path)
            throws ServerException, ClientException, IOException
    {
        if (gSession.parallel > 1) {
            throw new ClientException(
                    ClientException.BAD_MODE,
                    "nlist cannot be called with parallelism");
        }
        return super.nlist(path);
    }

    /**
     * Performs remote directory listing like
     * {@link FTPClient#mlsd(String) FTPClient.mlsd()}.
     * <b>Note:</b> This method cannot be used
     * in conjunction with parallelism or striping; set parallelism to
     * 1 before calling it. Otherwise, use
     * {@link FTPClient#mlsd(String, DataSink) FTPClient.mlsd()}.
     * Unlike in vanilla FTP, here IMAGE mode is allowed.
     * For more documentation, look at FTPClient.
     */
    @Override
    public Vector mlsd(String filter)
            throws ServerException, ClientException, IOException
    {
        if (gSession.parallel > 1) {
            throw new ClientException(
                    ClientException.BAD_MODE,
                    "mlsd cannot be called with parallelism");
        }
        return super.mlsd(filter);
    }

    @Override
    protected void listCheck() throws ClientException
    {
        // do nothing
    }

    @Override
    protected void checkTransferParamsGet()
            throws ServerException, IOException, ClientException
    {
        Session localSession = localServer.getSession();
        session.matches(localSession);

        // if transfer modes have not been defined, 
        // set this (dest) as active
        if (session.serverMode == Session.SERVER_DEFAULT) {
            HostPort hp = setLocalPassive();
            setActive(hp);
        }
    }

    @Override
    protected String getModeStr(int mode)
    {
        switch (mode) {
        case Session.MODE_STREAM:
            return "S";
        case Session.MODE_BLOCK:
            return "B";
        case GridFTPSession.MODE_EBLOCK:
            return "E";
        default:
            throw new IllegalArgumentException("Bad mode: " + mode);
        }
    }

    /**
     * Sets remote server to striped passive server mode (SPAS).
     **/
    public HostPortList setStripedPassive()
            throws IOException,
            ServerException
    {
        Command cmd = new Command("SPAS",
                                  (controlChannel.isIPv6()) ? "2" : null);
        Reply reply = null;

        try {
            reply = controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }

        this.gSession.serverMode = GridFTPSession.SERVER_EPAS;
        if (controlChannel.isIPv6()) {
            gSession.serverAddressList =
                    HostPortList.parseIPv6Format(reply.getMessage());
            int size = gSession.serverAddressList.size();
            for (int i = 0; i < size; i++) {
                HostPort6 hp = (HostPort6) gSession.serverAddressList.get(i);
                if (hp.getHost() == null) {
                    hp.setVersion(HostPort6.IPv6);
                    hp.setHost(controlChannel.getHost());
                }
            }
        } else {
            gSession.serverAddressList =
                    HostPortList.parseIPv4Format(reply.getMessage());
        }
        return gSession.serverAddressList;
    }

    /**
     * Sets remote server to striped active server mode (SPOR).
     **/
    public void setStripedActive(HostPortList hpl)
            throws IOException,
            ServerException
    {
        Command cmd = new Command("SPOR", hpl.toFtpCmdArgument());

        try {
            controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }

        this.gSession.serverMode = GridFTPSession.SERVER_EACT;
    }

    /**
     * Starts local server in striped passive mode. Since the local server
     * is not distributed, it will only listen on one socket.
     *
     * @param range required server port
     * @param queue max size of queue of awaiting new data channel connection
     *              requests
     * @return the HostPortList of 1 element representing the socket where the
     * local server is listening
     **/
    public HostPortList setLocalStripedPassive(PortRange range, int queue)
            throws IOException
    {
        return gLocalServer.setStripedPassive(range, queue);
    }

    /**
     * Behaves like setLocalStripedPassive(FTPServerFacade.ANY_PORT,
     * FTPServerFacade.DEFAULT_QUEUE)
     **/
    public HostPortList setLocalStripedPassive()
            throws IOException
    {
        return gLocalServer.setStripedPassive(portRange, FTPServerFacade.DEFAULT_QUEUE);
    }

    /**
     * Starts local server in striped active mode.
     * setStripedPassive() must be called before that.
     * This method takes no parameters. HostPortList of the remote
     * server, known from the last call of setStripedPassive(), is stored
     * internally and the local server will connect to this address.
     **/
    public void setLocalStripedActive()
            throws ClientException,
            IOException
    {
        if (gSession.serverAddressList == null) {
            throw new ClientException(ClientException.CALL_PASSIVE_FIRST);
        }
        try {
            gLocalServer.setStripedActive(gSession.serverAddressList);
        } catch (UnknownHostException e) {
            throw new ClientException(ClientException.UNKNOWN_HOST);
        }
    }

    /**
     * Performs extended retrieve (partial retrieve mode starting
     * at offset 0).
     *
     * @param remoteFileName file to retrieve
     * @param size           number of bytes of remote file to transmit
     * @param sink           data sink to store the file
     * @param mListener      marker listener
     **/
    public void extendedGet(String remoteFileName,
                            long size,
                            DataSink sink,
                            MarkerListener mListener)
            throws IOException,
            ClientException,
            ServerException
    {
        extendedGet(remoteFileName,
                    0,
                    size,
                    sink,
                    mListener);
    }

    /**
     * Performs extended retrieve (partial retrieve mode).
     *
     * @param remoteFileName file to retrieve
     * @param offset         the staring offset in the remote file
     * @param size           number of bytes of remote file to transmit
     * @param sink           data sink to store the file
     * @param mListener      marker listener
     **/
    public void extendedGet(String remoteFileName,
                            long offset,
                            long size,
                            DataSink sink,
                            MarkerListener mListener)
            throws IOException,
            ClientException,
            ServerException
    {

        // servers support GridFTP?
        checkGridFTPSupport();
        // all parameters set correctly (or still unset)?
        checkTransferParamsGet();

        gLocalServer.store(sink);

        controlChannel.write(new Command("ERET",
                                         "P " + offset + " " + size
                                         + " " + remoteFileName));

        transferRunSingleThread(localServer.getControlChannel(),
                                mListener);
    }

    /**
     * Performs extended store (adujsted store mode with offset 0).
     *
     * @param remoteFileName file name to store
     * @param source         source for the data to transfer
     * @param mListener      marker listener
     **/
    public void extendedPut(String remoteFileName,
                            DataSource source,
                            MarkerListener mListener)
            throws IOException,
            ServerException,
            ClientException
    {
        extendedPut(remoteFileName,
                    0,
                    source,
                    mListener);
    }

    /**
     * Performs extended store (adujsted store mode).
     *
     * @param remoteFileName file name to store
     * @param offset         the offset added to the file pointer before storing
     *                       the blocks of the file.
     * @param source         source for the data to transfer
     * @param mListener      marker listener
     **/
    public void extendedPut(String remoteFileName,
                            long offset,
                            DataSource source,
                            MarkerListener mListener)
            throws IOException,
            ServerException,
            ClientException
    {

        // servers support GridFTP?
        checkGridFTPSupport();
        // all parameters set correctly (or still unset)?
        checkTransferParamsPut();

        localServer.retrieve(source);

        controlChannel.write(new Command("ESTO",
                                         "A " + offset + " " +
                                         remoteFileName));

        transferRunSingleThread(localServer.getControlChannel(),
                                mListener);
    }

    /*
      3rd party transfer code
     */


    /**
     * Performs a third-party transfer between two servers using extended
     * block mode.
     * If server modes are unset, source will be set to active
     * and destination to passive.
     *
     * @param remoteSrcFile source filename
     * @param destination   destination server
     * @param remoteDstFile destination filename
     * @param mListener     transer progress listener.
     *                      Can be set to null.
     */
    public void extendedTransfer(String remoteSrcFile,
                                 GridFTPClient destination,
                                 String remoteDstFile,
                                 MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {
        extendedTransfer(remoteSrcFile, 0, getSize(remoteSrcFile),
                         destination,
                         remoteDstFile, 0,
                         mListener);
    }

    /**
     * Performs a third-party transfer between two servers using extended
     * block mode.
     * If server modes are unset, source will be set to active
     * and destination to passive.
     *
     * @param remoteSrcFile       source filename
     * @param remoteSrcFileOffset source filename offset
     * @param remoteSrcFileLength source filename length to transfer
     * @param destination         destination server
     * @param remoteDstFile       destination filename
     * @param remoteDstFileOffset destination filename offset
     * @param mListener           transer progress listener.
     *                            Can be set to null.
     */
    public void extendedTransfer(String remoteSrcFile,
                                 long remoteSrcFileOffset,
                                 long remoteSrcFileLength,
                                 GridFTPClient destination,
                                 String remoteDstFile,
                                 long remoteDstFileOffset,
                                 MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {

        // FIXME: ESTO & ERET do not require MODE E this needs to be fixed

        // servers support GridFTP?
        checkGridFTPSupport();
        destination.checkGridFTPSupport();
        // all parameters set correctly (or still unset)?
        gSession.matches(destination.gSession);

        //mode E
        if (gSession.transferMode != GridFTPSession.MODE_EBLOCK) {
            throw new ClientException(ClientException.BAD_MODE,
                                      "Extended transfer mode is necessary");
        }

        // if transfer modes have not been defined, 
        // set this (source) as active
        if (gSession.serverMode == Session.SERVER_DEFAULT) {
            HostPort hp = destination.setPassive();
            this.setActive(hp);
        }

        Command estoCmd =
                new Command("ESTO",
                            "A " + remoteDstFileOffset + " " + remoteDstFile);
        destination.controlChannel.write(estoCmd);

        Command eretCmd =
                new Command("ERET",
                            "P " + remoteSrcFileOffset + " " + remoteSrcFileLength
                            + " " + remoteSrcFile);

        controlChannel.write(eretCmd);

        transferRunSingleThread(destination.controlChannel, mListener);
    }

    public void
    extendedMultipleTransfer(
            long remoteSrcFileOffset[],
            long remoteSrcFileLength[],
            String remoteSrcFile[],
            GridFTPClient destination,
            long remoteDstFileOffset[],
            String remoteDstFile[],
            MarkerListener mListener,
            MultipleTransferCompleteListener doneListener)
            throws IOException, ServerException, ClientException
    {
        // servers support GridFTP?
        checkGridFTPSupport();
        destination.checkGridFTPSupport();
        // all parameters set correctly (or still unset)?
        gSession.matches(destination.gSession);

        //mode E
        if (gSession.transferMode != GridFTPSession.MODE_EBLOCK) {
            throw new ClientException(ClientException.BAD_MODE,
                                      "Extended transfer mode is necessary");
        }

        if (remoteSrcFile.length != remoteDstFile.length) {
            throw new ClientException(ClientException.OTHER,
                                      "All array paremeters must be smae length");
        }

        // if transfer modes have not been defined,
        // set this (source) as active
        if (gSession.serverMode == Session.SERVER_DEFAULT) {
            HostPort hp = destination.setPassive();
            this.setActive(hp);
        }

        /* send them all down the pipe */
        for (int i = 0; i < remoteSrcFile.length; i++) {
            Command estoCmd =
                    new Command("ESTO",
                                "A " + remoteDstFileOffset[i] + " " + remoteDstFile[i]);
            destination.controlChannel.write(estoCmd);

            Command eretCmd =
                    new Command("ERET",
                                "P " + remoteSrcFileOffset[i] + " " + remoteSrcFileLength[i]
                                + " " + remoteSrcFile[i]);
            controlChannel.write(eretCmd);
        }

        for (int i = 0; i < remoteSrcFile.length; i++) {
            transferRunSingleThread(destination.controlChannel, mListener);

            if (doneListener != null) {
                MultipleTransferComplete mtc;
                mtc = new MultipleTransferComplete(
                        remoteSrcFile[i],
                        remoteDstFile[i],
                        this,
                        destination,
                        i);

                doneListener.transferComplete(mtc);
            }
        }
    }


    public void
    extendedMultipleTransfer(
            String remoteSrcFile[],
            GridFTPClient destination,
            String remoteDstFile[],
            MarkerListener mListener,
            MultipleTransferCompleteListener doneListener)
            throws IOException, ServerException, ClientException
    {
        // servers support GridFTP?
        checkGridFTPSupport();
        destination.checkGridFTPSupport();
        // all parameters set correctly (or still unset)?
        gSession.matches(destination.gSession);

        //mode E
        if (gSession.transferMode != GridFTPSession.MODE_EBLOCK) {
            throw new ClientException(ClientException.BAD_MODE,
                                      "Extended transfer mode is necessary");
        }

        if (remoteSrcFile.length != remoteDstFile.length) {
            throw new ClientException(ClientException.OTHER,
                                      "All array paremeters must be smae length");
        }

        // if transfer modes have not been defined,
        // set this (source) as active
        if (gSession.serverMode == Session.SERVER_DEFAULT) {
            HostPort hp = destination.setPassive();
            this.setActive(hp);
        }

        /* send them all down the pipe */
        for (int i = 0; i < remoteSrcFile.length; i++) {
            Command estoCmd =
                    new Command("STOR ",
                                remoteDstFile[i]);
            destination.controlChannel.write(estoCmd);

            Command eretCmd =
                    new Command("RETR",
                                remoteSrcFile[i]);

            controlChannel.write(eretCmd);
        }

        for (int i = 0; i < remoteSrcFile.length; i++) {
            transferRunSingleThread(destination.controlChannel, mListener);

            if (doneListener != null) {
                MultipleTransferComplete mtc;
                mtc = new MultipleTransferComplete(
                        remoteSrcFile[i],
                        remoteDstFile[i],
                        this,
                        destination,
                        i);

                doneListener.transferComplete(mtc);
            }
        }
    }


    /**
     * assure that the server supports extended transfer features;
     * throw exception if not
     **/
    protected void checkGridFTPSupport()
            throws IOException,
            ServerException
    {
        FeatureList fl = getFeatureList();
        if (
                !(fl.contains(FeatureList.PARALLEL)
                  && fl.contains(FeatureList.ESTO)
                  && fl.contains(FeatureList.ERET)
                  && fl.contains(FeatureList.SIZE))
                ) {
            throw new ServerException(ServerException.UNSUPPORTED_FEATURE);
        }
    }

    /*
     end 3rd party transfer code
    */

    /**
     * Sets data channel authentication mode (DCAU)
     *
     * @param type for 2-party transfer must be
     *             DataChannelAuthentication.SELF or DataChannelAuthentication.NONE
     **/
    public void setDataChannelAuthentication(DataChannelAuthentication type)
            throws IOException,
            ServerException
    {

        Command cmd = new Command("DCAU", type.toFtpCmdArgument());
        try {

            controlChannel.execute(cmd);

        } catch (UnexpectedReplyCodeException urce) {
            if (!type.toFtpCmdArgument().equals("N")) {
                throw ServerException.embedUnexpectedReplyCodeException(urce);
            }
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }

        this.gSession.dataChannelAuthentication = type;

        gLocalServer.setDataChannelAuthentication(type);
    }

    /**
     * Sets compatibility mode with old GSIFTP server.
     * Locally sets data channel authentication to NONE
     * but does not send the command
     * to the remote server (the server wouldn't understand it)
     **/
    public void setLocalNoDataChannelAuthentication()
    {
        gLocalServer.setDataChannelAuthentication(DataChannelAuthentication.NONE);
    }

    /**
     * Returns data channel authentication mode (DCAU).
     *
     * @return data channel authentication mode
     **/
    public DataChannelAuthentication getDataChannelAuthentication()
    {
        return gSession.dataChannelAuthentication;
    }

    /**
     * Sets data channel protection level (PROT).
     *
     * @param protection should be
     *                   {@link GridFTPSession#PROTECTION_CLEAR CLEAR},
     *                   {@link GridFTPSession#PROTECTION_SAFE SAFE}, or
     *                   {@link GridFTPSession#PROTECTION_PRIVATE PRIVATE}, or
     *                   {@link GridFTPSession#PROTECTION_CONFIDENTIAL CONFIDENTIAL}.
     **/
    public void setDataChannelProtection(int protection)
            throws IOException, ServerException
    {

        String protectionStr = null;
        switch (protection) {
        case GridFTPSession.PROTECTION_CLEAR:
            protectionStr = "C";
            break;
        case GridFTPSession.PROTECTION_SAFE:
            protectionStr = "S";
            break;
        case GridFTPSession.PROTECTION_CONFIDENTIAL:
            protectionStr = "E";
            break;
        case GridFTPSession.PROTECTION_PRIVATE:
            protectionStr = "P";
            break;
        default:
            throw new IllegalArgumentException("Bad protection: " +
                                               protection);
        }

        Command cmd = new Command("PROT", protectionStr);
        try {
            controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }

        this.gSession.dataChannelProtection = protection;

        gLocalServer.setDataChannelProtection(protection);
    }

    /**
     * Returns data channel protection level.
     *
     * @return data channel protection level:
     * {@link GridFTPSession#PROTECTION_CLEAR CLEAR},
     * {@link GridFTPSession#PROTECTION_SAFE SAFE}, or
     * {@link GridFTPSession#PROTECTION_PRIVATE PRIVATE}, or
     * {@link GridFTPSession#PROTECTION_CONFIDENTIAL CONFIDENTIAL}.
     **/
    public int getDataChannelProtection()
    {
        return gSession.dataChannelProtection;
    }

    // basic compatibility API

    @Override
    public void get(String remoteFileName,
                    File localFile)
            throws IOException,
            ClientException,
            ServerException
    {
        if (gSession.transferMode == GridFTPSession.MODE_EBLOCK) {
            DataSink sink =
                    new FileRandomIO(new RandomAccessFile(localFile, "rw"));
            get(remoteFileName, sink, null);
        } else {
            super.get(remoteFileName, localFile);
        }
    }

    @Override
    public void put(File localFile,
                    String remoteFileName,
                    boolean append)
            throws IOException,
            ServerException,
            ClientException
    {
        if (gSession.transferMode == GridFTPSession.MODE_EBLOCK) {
            DataSource source =
                    new FileRandomIO(new RandomAccessFile(localFile, "r"));
            put(remoteFileName, source, null, append);
        } else {
            super.put(localFile, remoteFileName, append);
        }
    }

    /**
     * Sets the checksum values ahead of the transfer
     *
     * @param algorithm the checksume algorithm
     * @param value     the checksum value as hexadecimal number
     * @return nothing
     * @throws ServerException if an error occured.
     */
    public void setChecksum(ChecksumAlgorithm algorithm,
                            String value)
            throws IOException, ServerException
    {
        String arguments = algorithm.toFtpCmdArgument() + " " + value;

        Command cmd = new Command("SCKS", arguments);
        try {
            controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }


    /**
     * Computes and returns a checksum of a file.
     * transferred.
     *
     * @param algorithm the checksume algorithm
     * @param offset    the offset
     * @param length    the length
     * @param file      file to compute checksum of
     * @return the computed checksum
     * @throws ServerException if an error occured.
     */
    public String checksum(ChecksumAlgorithm algorithm,
                           long offset, long length, String file)
            throws IOException, ServerException
    {
        String arguments = algorithm.toFtpCmdArgument() + " " +
                           String.valueOf(offset) + " " +
                           String.valueOf(length) + " " + file;

        Command cmd = new Command("CKSM", arguments);
        Reply reply = null;
        try {
            reply = controlChannel.execute(cmd);
            return reply.getMessage();
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }

    /**
     * Performs a recursive directory listing starting at the given path
     * (or, if path is null, at the current directory of the FTP server).
     * MlsxEntry instances for all of the files in the subtree will be
     * written through the passed MlsxEntryWriter.
     *
     * @param path   path to begin recursive directory listing
     * @param writer sink for created MlsxEntry instances
     * @throws ServerException
     * @throws ClientException
     * @throws IOException
     */
    public void mlsr(String path, MlsxEntryWriter writer)
            throws ServerException, ClientException, IOException
    {
        if (gSession.parallel > 1) {
            throw new ClientException(
                    ClientException.BAD_MODE,
                    "mlsr cannot be called with parallelism");
        }
        Command cmd = (path == null) ?
                      new Command("MLSR") :
                      new Command("MLSR", path);

        MlsxParserDataSink sink = new MlsxParserDataSink(writer);

        performTransfer(cmd, sink);
    }

    private class MlsxParserDataSink implements DataSink
    {

        private final MlsxEntryWriter writer;
        private final byte[] buf = new byte[4096];
        private int pos = 0;

        public MlsxParserDataSink(MlsxEntryWriter w)
        {
            writer = w;
        }

        @Override
        public void write(Buffer buffer) throws IOException
        {
            byte[] data = buffer.getBuffer();
            int len = buffer.getLength();
            int i = 0;

            while (i < len && pos < buf.length) {
                if (data[i] == '\r' || data[i] == '\n') {
                    if (pos > 0) {
                        try {
                            writer.write(new MlsxEntry(new String(buf, 0, pos)));
                        } catch (FTPException ex) {
                            throw new IOException();
                        }
                    }
                    pos = 0;
                    while (i < len && data[i] < ' ') ++i;
                } else {
                    buf[pos++] = data[i++];
                }
            }
        }

        @Override
        public void close() throws IOException
        {
            writer.close();
        }
    }

    /**
     * Change the Unix group membership of a file.
     *
     * @param group the name or ID of the group
     * @param file  the file whose group membership should be changed
     * @throws ServerException if an error occurred.
     */
    public void changeGroup(String group, String file)
            throws IOException, ServerException
    {
        String arguments = group + " " + file;
        Command cmd = new Command("SITE CHGRP", arguments);
        try {
            controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }

    /**
     * Change the modification time of a file.
     *
     * @param year  Modifcation year
     * @param month Modification month (1-12)
     * @param day   Modification day (1-31)
     * @param hour  Modification hour (0-23)
     * @param min   Modification minutes (0-59)
     * @param sec   Modification seconds (0-59)
     * @param file  file whose modification time should be changed
     * @throws IOException
     * @throws ServerException if an error occurred.
     */
    public void changeModificationTime(int year, int month, int day, int hour, int min, int sec, String file)
            throws IOException, ServerException
    {
        DecimalFormat df2 = new DecimalFormat("00");
        DecimalFormat df4 = new DecimalFormat("0000");

        String arguments = df4.format(year) + df2.format(month) + df2.format(day) +
                           df2.format(hour) + df2.format(min) + df2.format(sec) + " " + file;
        Command cmd = new Command("SITE UTIME", arguments);

        try {
            controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }

    /**
     * Create a symbolic link on the FTP server.
     *
     * @param link_target the path to which the symbolic link should point
     * @param link_name   the path of the symbolic link to create
     * @throws IOException
     * @throws ServerException if an error occurred.
     */
    public void createSymbolicLink(String link_target, String link_name)
            throws IOException, ServerException
    {

        String arguments = link_target.replaceAll(" ", "%20") + " " + link_name;
        Command cmd = new Command("SITE SYMLINK", arguments);

        try {
            controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }

}
