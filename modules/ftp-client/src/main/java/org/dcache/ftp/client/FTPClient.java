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

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.File;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.ftp.client.exception.ClientException;
import org.dcache.ftp.client.exception.ServerException;
import org.dcache.ftp.client.exception.FTPReplyParseException;
import org.dcache.ftp.client.exception.UnexpectedReplyCodeException;
import org.dcache.ftp.client.exception.FTPException;
import org.dcache.ftp.client.vanilla.FTPControlChannel;
import org.dcache.ftp.client.vanilla.FTPServerFacade;
import org.dcache.ftp.client.vanilla.BasicClientControlChannel;
import org.dcache.ftp.client.vanilla.Command;
import org.dcache.ftp.client.vanilla.Reply;
import org.dcache.ftp.client.vanilla.TransferMonitor;
import org.dcache.ftp.client.vanilla.TransferState;
import org.dcache.util.PortRange;

/**
 * This is the main user interface for FTP operations.
 * Use this class for client - server or third party transfers
 * that do not require GridFTP extensions.
 * Consult the manual for general usage.
 * <br><b>Note:</b> If using with GridFTP servers operations like
 * {@link #setMode(int) setMode()}, {@link #setType(int) setType()} that
 * affect data channel settings <b>must</b> be called before passive
 * or active data channel mode is set.
 **/
public class FTPClient
{

    private static final Logger logger = LoggerFactory.getLogger(FTPClient.class);

    // represents the state of interaction with remote server
    protected Session session;
    protected FTPControlChannel controlChannel;

    // the local server handles data channels
    protected FTPServerFacade localServer;

    /* needed for last modified command */
    protected SimpleDateFormat dateFormat = null;

    protected String username = null;

    protected PortRange portRange = new PortRange(0);

    /**
     * Whether to use ALLO with put()/asyncPut() or not
     */
    protected boolean useAllo;

    /**
     * List of the checksum algorithms supported by the server as described in
     * {@link http://www.ogf.org/documents/GFD.47.pdf [GridFTP v2 Protocol Description]}
     */
    protected List<String> algorithms;

    /* for subclasses */
    protected FTPClient()
    {
    }

    /**
     * Constructs client and connects it to the remote server.
     *
     * @param host remote server host
     * @param port remote server port
     */
    public FTPClient(String host, int port)
            throws IOException, ServerException
    {
        session = new Session();

        controlChannel = new FTPControlChannel(host, port);
        controlChannel.open();

        localServer = new FTPServerFacade(controlChannel);
        localServer.authorize();
    }

    public PortRange getPortRange()
    {
        return portRange;
    }

    public void setPortRange(PortRange portRange)
    {
        this.portRange = portRange;
    }

    /*
         * @return host
         */
    public String getHost()
    {
        return this.controlChannel.getHost();
    }

    /* 
     * @return port
     */
    public int getPort()
    {
        return this.controlChannel.getPort();
    }

    /**
     * Returns the last reply received from the server. This could
     * be used immediately after the call to the constructor to
     * get the initial server reply
     */
    public Reply getLastReply()
    {
        return this.controlChannel.getLastReply();
    }

    /**
     * Returns the remote file size.
     *
     * @param filename filename get the size for.
     * @return size of the file.
     * @throws ServerException if the file does not exist or
     *                         an error occured.
     */
    public long getSize(String filename)
            throws IOException, ServerException
    {
        if (filename == null) {
            throw new IllegalArgumentException("Required argument missing");
        }
        Command cmd = new Command("SIZE", filename);
        Reply reply = null;
        try {
            reply = controlChannel.execute(cmd);
            return Long.parseLong(reply.getMessage());
        } catch (NumberFormatException e) {
            throw ServerException.embedFTPReplyParseException(
                    new FTPReplyParseException(
                            FTPReplyParseException.MESSAGE_UNPARSABLE,
                            "Could not parse size: " + reply.getMessage()));
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }

    /**
     * Returns last modification time of the specifed file.
     *
     * @param filename filename get the last modification time for.
     * @return the time and date of the last modification.
     * @throws ServerException if the file does not exist or
     *                         an error occured.
     */
    public Date getLastModified(String filename)
            throws IOException, ServerException
    {
        if (filename == null) {
            throw new IllegalArgumentException("Required argument missing");
        }
        Command cmd = new Command("MDTM", filename);
        Reply reply = null;
        try {
            reply = controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused changing transfer mode");
        }

        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        }

        try {
            return dateFormat.parse(reply.getMessage());
        } catch (ParseException e) {
            throw ServerException.embedFTPReplyParseException(
                    new FTPReplyParseException(
                            0,
                            "Invalid file modification time reply: " + reply));
        }
    }

    /**
     * Checks if given file/directory exists on the server.
     *
     * @param filename file or directory name
     * @return true if the file exists, false otherwise.
     */
    public boolean exists(String filename)
            throws IOException, ServerException
    {
        if (filename == null) {
            throw new IllegalArgumentException("Required argument missing");
        }
        try {
            Reply reply =
                    controlChannel.exchange(new Command("RNFR", filename));
            if (Reply.isPositiveIntermediate(reply)) {
                controlChannel.execute(new Command("ABOR"));
                return true;
            } else {
                return false;
            }
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Abort failed");
        }
    }

    /**
     * Changes the remote current working directory.
     */
    public void changeDir(String dir)
            throws IOException, ServerException
    {
        if (dir == null) {
            throw new IllegalArgumentException("Required argument missing");
        }
        Command cmd = new Command("CWD", dir);
        try {
            controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused changing directory");
        }
    }

    /**
     * Deletes the remote directory.
     */
    public void deleteDir(String dir)
            throws IOException, ServerException
    {
        if (dir == null) {
            throw new IllegalArgumentException("Required argument missing");
        }
        Command cmd = new Command("RMD", dir);
        try {
            controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused deleting directory");
        }
    }

    /**
     * Deletes the remote file.
     */
    public void deleteFile(String filename)
            throws IOException, ServerException
    {
        if (filename == null) {
            throw new IllegalArgumentException("Required argument missing");
        }
        Command cmd = new Command("DELE", filename);
        try {
            controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused deleting file");
        }
    }

    /**
     * Creates remote directory.
     */
    public void makeDir(String dir)
            throws IOException, ServerException
    {
        if (dir == null) {
            throw new IllegalArgumentException("Required argument missing");
        }
        Command cmd = new Command("MKD", dir);
        try {
            controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused creating directory");
        }
    }

    /**
     * Renames remote directory.
     */
    public void rename(String oldName, String newName)
            throws IOException, ServerException
    {
        if (oldName == null || newName == null) {
            throw new IllegalArgumentException("Required argument missing");
        }
        Command cmd = new Command("RNFR", oldName);
        try {
            Reply reply = controlChannel.exchange(cmd);
            if (!Reply.isPositiveIntermediate(reply)) {
                throw new UnexpectedReplyCodeException(reply);
            }
            controlChannel.execute(new Command("RNTO", newName));
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused renaming file");
        }
    }

    /**
     * Returns remote current working directory.
     *
     * @return remote current working directory.
     */
    public String getCurrentDir() throws IOException, ServerException
    {
        Reply reply = null;
        try {
            reply = controlChannel.execute(Command.PWD);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused returning current directory");
        }
        String strReply = reply.getMessage();
        if (strReply.length() > 0 && strReply.charAt(0) == '"') {
            return strReply.substring(1, strReply.indexOf('"', 1));
        } else {
            throw ServerException.embedFTPReplyParseException(
                    new FTPReplyParseException(
                            0,
                            "Cannot parse 'PWD' reply: " + reply));
        }
    }

    /**
     * Changes remote current working directory to the higher level.
     */
    public void goUpDir() throws IOException, ServerException
    {
        try {
            controlChannel.execute(Command.CDUP);
            // alternative: changeDir("..");
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused changing current directory");
        }
    }

    private class ByteArrayDataSink implements DataSink
    {

        private final ByteArrayOutputStream received;

        public ByteArrayDataSink()
        {
            this.received = new ByteArrayOutputStream(1000);
        }

        @Override
        public void write(Buffer buffer) throws IOException
        {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "received "
                        + buffer.getLength()
                        + " bytes of directory listing");
            }
            this.received.write(buffer.getBuffer(), 0, buffer.getLength());
        }

        @Override
        public void close() throws IOException
        {
        }

        public ByteArrayOutputStream getData()
        {
            return this.received;
        }
    }

    /**
     * Performs remote directory listing. Sends 'LIST -d *' command.
     * <p>
     * <br><b>Note</b>:<i>
     * This function can only parse Unix ls -d like output. Please
     * note that the LIST output is unspecified in the FTP standard and
     * each server might return slightly different output causing the
     * parsing to fail.
     * Also, if the ftp server does not accept -d option or support
     * wildcards, this method might fail. For example, this command will
     * fail on GridFTP server distributed with GT 4.0.0.
     * It is strongly recommended to use {@link #mlsd() mlsd()}
     * function instead.</i>
     *
     * @return Vector list of {@link FileInfo FileInfo} objects, representing
     * remote files
     * @see #mlsd()
     */
    public Vector list() throws ServerException, ClientException, IOException
    {
        return list("*");
    }

    /**
     * Performs remote directory listing with the specified filter.
     * Sends 'LIST -d &lt;filter&gt;' command.
     * <p>
     * <br><b>Note</b>: <i>
     * This function can only parse Unix ls -d like output. Please
     * note that the LIST output is unspecified in the FTP standard and
     * each server might return slightly different output causing the
     * parsing to fail.
     * Also, if the ftp server does not accept -d option or support
     * wildcards, this method might fail. For example, this command will
     * fail on GridFTP server distributed with GT 4.0.0.
     * It is strongly recommended to use {@link #mlsd(String) mlsd()}
     * function instead. </i>
     *
     * @param filter "*" for example, can be null.
     * @return Vector list of {@link FileInfo FileInfo} objects, representing
     * remote files
     * @see #mlsd(String)
     */
    public Vector list(String filter)
            throws ServerException, ClientException, IOException
    {
        return list(filter, "-d");
    }

    /**
     * Performs remote directory listing with the specified filter and
     * modifier. Sends 'LIST &lt;modifier&gt; &lt;filter&gt;' command.
     * <p>
     * <br><b>Note</b>: <i>
     * This function can only parse Unix ls -d like output. Please
     * note that the LIST output is unspecified in the FTP standard and
     * each server might return slightly different output causing the
     * parsing to fail.
     * Also, please keep in mind that the ftp server might not
     * recognize or support all the different modifiers or filters.
     * In fact, some servers such as GridFTP server distributed with
     * GT 4.0.0 does not support any modifiers or filters
     * (strict RFC 959 compliance).
     * It is strongly recommended to use {@link #mlsd(String) mlsd()}
     * function instead.</i>
     *
     * @param filter   "*" for example, can be null.
     * @param modifier "-d" for example, can be null.
     * @return Vector list of {@link FileInfo FileInfo} objects, representing
     * remote files
     * @see #mlsd(String)
     */
    public Vector list(String filter, String modifier)
            throws ServerException, ClientException, IOException
    {

        ByteArrayDataSink sink = new ByteArrayDataSink();

        list(filter, modifier, sink);

        ByteArrayOutputStream received = sink.getData();

        // transfer done. Data is in received stream.
        // convert it to a vector.

        BufferedReader reader =
                new BufferedReader(new StringReader(received.toString()));

        Vector fileList = new Vector();
        FileInfo fileInfo = null;
        String line = null;

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (logger.isDebugEnabled()) {
                logger.debug("line ->" + line);
            }
            if (line.equals("")) {
                continue;
            }
            if (line.startsWith("total"))
                continue;
            try {
                fileInfo = new FileInfo(line);
            } catch (FTPException e) {
                ClientException ce =
                        new ClientException(
                                ClientException.UNSPECIFIED,
                                "Could not create FileInfo");
                ce.setRootCause(e);
                throw ce;
            }
            fileList.addElement(fileInfo);
        }
        return fileList;
    }

    /**
     * Performs directory listing and writes the result
     * to the supplied data sink.
     * This method is allowed in ASCII mode only.
     * <p>
     * <br><b>Note</b>: <i>
     * Please keep in mind that the ftp server might not
     * recognize or support all the different modifiers or filters.
     * In fact, some servers such as GridFTP server distributed with
     * GT 4.0.0 does not support any modifiers or filters
     * (strict RFC 959 compliance).
     * It is strongly recommended to use {@link #mlsd(String, DataSink)
     * mlsd()} function instead.</i>
     *
     * @param filter   remote list command file filter, eg. "*"
     * @param modifier remote list command modifier, eg. "-d"
     * @param sink     data destination
     **/
    public void list(String filter, String modifier, DataSink sink)
            throws ServerException, ClientException, IOException
    {
        String arg = null;

        if (modifier != null) {
            arg = modifier;
        }
        if (filter != null) {
            arg = (arg == null) ? filter : arg + " " + filter;
        }

        Command cmd = new Command("LIST", arg);

        performTransfer(cmd, sink);
    }

    /**
     * Performs remote directory listing of the current directory.
     * Sends 'NLST' command.
     *
     * @return Vector list of {@link FileInfo FileInfo} objects, representing
     * remote files
     */
    public Vector nlist()
            throws ServerException, ClientException, IOException
    {
        return nlist(null);
    }

    /**
     * Performs remote directory listing on the given path.
     * Sends 'NLST &lt;path&gt;' command.
     *
     * @param path directory to perform listing of. If null, listing
     *             of current directory will be performed.
     * @return Vector list of {@link FileInfo FileInfo} objects, representing
     * remote files
     */
    public Vector nlist(String path)
            throws ServerException, ClientException, IOException
    {

        ByteArrayDataSink sink = new ByteArrayDataSink();

        nlist(path, sink);

        ByteArrayOutputStream received = sink.getData();

        // transfer done. Data is in received stream.
        // convert it to a vector.

        BufferedReader reader =
                new BufferedReader(new StringReader(received.toString()));

        Vector fileList = new Vector();
        FileInfo fileInfo = null;
        String line = null;

        while ((line = reader.readLine()) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("line ->" + line);
            }

            fileInfo = new FileInfo();
            fileInfo.setName(line);
            fileInfo.setFileType(FileInfo.UNKNOWN_TYPE);

            fileList.addElement(fileInfo);
        }

        return fileList;
    }

    /**
     * Performs remote directory listing on the given path.
     * Sends 'NLST &lt;path&gt;' command.
     *
     * @param path directory to perform listing of. If null, listing
     *             of current directory will be performed.
     * @param sink sink to which the listing data will be written.
     */
    public void nlist(String path, DataSink sink)
            throws ServerException, ClientException, IOException
    {
        Command cmd = (path == null) ?
                      new Command("NLST") :
                      new Command("NLST", path);

        performTransfer(cmd, sink);
    }

    /**
     * Get info of a certain remote file in Mlsx format.
     */
    public MlsxEntry mlst(String fileName)
            throws IOException, ServerException
    {
        try {
            Reply reply = controlChannel.execute(new Command("MLST", fileName));
            String replyMessage = reply.getMessage();
            StringTokenizer replyLines =
                    new StringTokenizer(
                            replyMessage,
                            System.getProperty("line.separator"));
            if (replyLines.hasMoreElements()) {
                replyLines.nextElement();
            } else {
                throw new FTPException(FTPException.UNSPECIFIED,
                                       "Expected multiline reply");
            }
            if (replyLines.hasMoreElements()) {
                String line = (String) replyLines.nextElement();
                return new MlsxEntry(line);
            } else {
                throw new FTPException(FTPException.UNSPECIFIED,
                                       "Expected multiline reply");
            }
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused MLST command");
        } catch (FTPException e) {
            ServerException ce =
                    new ServerException(
                            ClientException.UNSPECIFIED,
                            "Could not create MlsxEntry");
            ce.setRootCause(e);
            throw ce;
        }
    }

    /**
     * Performs remote directory listing of the current directory.
     * Sends 'MLSD' command.
     *
     * @return Vector list of {@link MlsxEntry MlsxEntry} objects, representing
     * remote files
     */
    public Vector mlsd()
            throws ServerException, ClientException, IOException
    {
        return mlsd(null);
    }

    /**
     * Performs remote directory listing on the given path.
     * Sends 'MLSD &lt;path&gt;' command.
     *
     * @param path directory to perform listing of. If null, listing
     *             of current directory will be performed.
     * @return Vector list of {@link MlsxEntry MlsxEntry} objects, representing
     * remote files
     */
    public Vector mlsd(String path)
            throws ServerException, ClientException, IOException
    {

        ByteArrayDataSink sink = new ByteArrayDataSink();

        mlsd(path, sink);

        ByteArrayOutputStream received = sink.getData();

        // transfer done. Data is in received stream.
        // convert it to a vector.

        BufferedReader reader =
                new BufferedReader(new StringReader(received.toString()));

        Vector fileList = new Vector();
        MlsxEntry entry = null;
        String line = null;

        while ((line = reader.readLine()) != null) {

            if (logger.isDebugEnabled()) {
                logger.debug("line ->" + line);
            }

            try {
                entry = new MlsxEntry(line);
            } catch (FTPException e) {
                ClientException ce =
                        new ClientException(
                                ClientException.UNSPECIFIED,
                                "Could not create MlsxEntry");
                ce.setRootCause(e);
                throw ce;
            }

            fileList.addElement(entry);

        }
        return fileList;
    }

    /**
     * Performs remote directory listing on the given path.
     * Sends 'MLSD &lt;path&gt;' command.
     *
     * @param path directory to perform listing of. If null, listing
     *             of current directory will be performed.
     * @param sink sink to which the listing data will be written.
     */
    public void mlsd(String path, DataSink sink)
            throws ServerException, ClientException, IOException
    {
        Command cmd = (path == null) ?
                      new Command("MLSD") :
                      new Command("MLSD", path);

        performTransfer(cmd, sink);
    }

    /**
     * check performed at the beginning of list()
     **/
    protected void listCheck() throws ClientException
    {
        if (session.transferType != Session.TYPE_ASCII) {
            throw new ClientException(
                    ClientException.BAD_MODE,
                    "list requires ASCII type");
        }
    }

    protected void checkTransferParamsGet()
            throws ServerException, IOException, ClientException
    {
        checkTransferParams();
    }

    protected void checkTransferParamsPut()
            throws ServerException, IOException, ClientException
    {
        checkTransferParams();
    }

    protected void checkTransferParams()
            throws ServerException, IOException, ClientException
    {
        Session localSession = localServer.getSession();
        session.matches(localSession);

        // if transfer modes have not been defined, 
        // set this (dest) as active
        if (session.serverMode == Session.SERVER_DEFAULT) {
            // resulting HostPort stored in session
            setPassive();
            // HostPort read from session
            setLocalActive();
        }
    }

    protected void performTransfer(Command cmd, DataSink sink)
            throws ServerException, ClientException, IOException
    {
        listCheck();
        checkTransferParamsGet();

        controlChannel.write(cmd);
        localServer.store(sink);

        transferRunSingleThread(localServer.getControlChannel(), null);
    }

    /**
     * Sets transfer type.
     *
     * @param type should be {@link Session#TYPE_IMAGE TYPE_IMAGE},
     *             {@link Session#TYPE_ASCII TYPE_ASCII},
     *             {@link Session#TYPE_LOCAL TYPE_LOCAL},
     *             {@link Session#TYPE_EBCDIC TYPE_EBCDIC}
     **/
    public void setType(int type) throws IOException, ServerException
    {

        localServer.setTransferType(type);

        String typeStr = null;
        switch (type) {
        case Session.TYPE_IMAGE:
            typeStr = "I";
            break;
        case Session.TYPE_ASCII:
            typeStr = "A";
            break;
        case Session.TYPE_LOCAL:
            typeStr = "E";
            break;
        case Session.TYPE_EBCDIC:
            typeStr = "L";
            break;
        default:
            throw new IllegalArgumentException("Bad type: " + type);
        }

        Command cmd = new Command("TYPE", typeStr);
        try {
            controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused changing transfer mode");
        }

        this.session.transferType = type;
    }

    protected String getModeStr(int mode)
    {
        switch (mode) {
        case Session.MODE_STREAM:
            return "S";
        case Session.MODE_BLOCK:
            return "B";
        default:
            throw new IllegalArgumentException("Bad mode: " + mode);
        }
    }

    /**
     * Sets transfer mode.
     *
     * @param mode should be {@link Session#MODE_STREAM MODE_STREAM},
     *             {@link Session#MODE_BLOCK MODE_BLOCK}
     **/
    public void setMode(int mode) throws IOException, ServerException
    {
        actualSetMode(mode, getModeStr(mode));
    }

    protected void actualSetMode(int mode, String modeStr)
            throws IOException, ServerException
    {

        localServer.setTransferMode(mode);
        Command cmd = new Command("MODE", modeStr);
        try {
            controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused changing transfer mode");
        }

        this.session.transferMode = mode;
    }

    /**
     * Sets protection buffer size (defined in RFC 2228)
     *
     * @param size the size of buffer
     */
    public void setProtectionBufferSize(int size)
            throws IOException, ServerException
    {

        if (size <= 0) {
            throw new IllegalArgumentException("size <= 0");
        }

        localServer.setProtectionBufferSize(size);
        try {
            Command cmd = new Command("PBSZ", Integer.toString(size));
            controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused setting protection buffer size");
        }
        this.session.protectionBufferSize = size;
    }

    /**
     * Aborts the current transfer. FTPClient is not thread
     * safe so be careful with using this procedure, which will
     * typically happen in multi threaded environment.
     * Especially during client-server two party transfer,
     * calling abort() may result with exceptions being thrown in the thread
     * that currently perform the transfer.
     **/
    public void abort() throws IOException, ServerException
    {
        // TODO: This might need to be reimplemented to support
        // sending out of bounds urgent TCP messages
        try {
            controlChannel.execute(Command.ABOR);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused changing transfer mode");
        } finally {
            localServer.abort();
        }
    }

    /**
     * Closes connection. Sends QUIT command and closes connection
     * even if the server reply was not positive. Also, closes
     * the local server. This function will block until the server
     * sends a reply to the QUIT command.
     **/
    public void close()
            throws IOException, ServerException
    {
        close(false);
    }

    /**
     * Closes connection. Sends QUIT and closes connection
     * even if the server reply was not positive. Also, closes
     * the local server.
     *
     * @param ignoreQuitReply if true the <code>QUIT</code> command
     *                        will be sent but the client will not wait for the
     *                        server's reply. If false, the client will block
     *                        for the server's reply.
     **/
    public void close(boolean ignoreQuitReply)
            throws IOException, ServerException
    {
        try {
            if (ignoreQuitReply) {
                controlChannel.write(Command.QUIT);
            } else {
                controlChannel.execute(Command.QUIT);
            }
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused closing");
        } finally {
            try {
                controlChannel.close();
            } finally {
                localServer.close();
            }
        }
    }

    /**
     * Returns true if the given feature is supported by remote server,
     * false otherwise.
     *
     * @return true if the given feature is supported by remote server,
     * false otherwise.
     */
    public boolean isFeatureSupported(String feature)
            throws IOException, ServerException
    {
        return getFeatureList().contains(feature);
    }

    /**
     * Returns list of features supported by remote server.
     *
     * @return list of features supported by remote server.
     */
    public FeatureList getFeatureList() throws IOException, ServerException
    {

        if (this.session.featureList != null) {
            return this.session.featureList;
        }

        // TODO: this can also be optimized. Instead of parsing the
        // reply after it is reveiced, we can parse it as it is
        // received.
        Reply featReply = null;
        try {
            featReply = controlChannel.execute(Command.FEAT);

            if (featReply.getCode() != 211) {
                throw ServerException.embedUnexpectedReplyCodeException(
                        new UnexpectedReplyCodeException(featReply),
                        "Server refused returning features");
            }
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused returning features");
        }

        this.session.featureList = new FeatureList(featReply.getMessage());

        return session.featureList;
    }

    /**
     * Sets remote server to passive server mode.
     *
     * @return the address at which the server is listening.
     */
    public HostPort setPassive() throws IOException, ServerException
    {
        Reply reply = null;
        try {
            reply = controlChannel.execute(
                    (controlChannel.isIPv6()) ? Command.EPSV : Command.PASV);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
        String pasvReplyMsg = null;

        pasvReplyMsg = reply.getMessage();

        int openBracket = pasvReplyMsg.indexOf("(");
        int closeBracket = pasvReplyMsg.indexOf(")", openBracket);
        String bracketContent =
                pasvReplyMsg.substring(openBracket + 1, closeBracket);

        this.session.serverMode = Session.SERVER_PASSIVE;

        HostPort hp = null;
        if (controlChannel.isIPv6()) {
            hp = new HostPort6(bracketContent);
            // since host information might be null
            // fill it it
            if (hp.getHost() == null) {
                ((HostPort6) hp).setVersion(HostPort6.IPv6);
                ((HostPort6) hp).setHost(controlChannel.getHost());
            }
        } else {
            hp = new HostPort(bracketContent);
        }

        this.session.serverAddress = hp;
        return hp;
    }

    /**
     * Sets remote server active, telling it to connect to the given
     * address.
     *
     * @param hostPort the address to which the server should connect
     */
    public void setActive(HostPort hostPort)
            throws IOException, ServerException
    {
        Command cmd = new Command((controlChannel.isIPv6()) ? "EPRT" : "PORT",
                                  hostPort.toFtpCmdArgument());
        try {
            controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }

        this.session.serverMode = Session.SERVER_ACTIVE;
    }

    /**
     * Sets remote server active, telling it to connect to the client.
     * setLocalPassive() must be called beforehand.
     **/
    public void setActive()
            throws IOException, ServerException, ClientException
    {
        Session local = localServer.getSession();
        if (local.serverAddress == null) {
            throw new ClientException(ClientException.CALL_PASSIVE_FIRST);
        }
        setActive(local.serverAddress);
    }

    /**
     * Starts local server in active server mode.
     **/
    public void setLocalActive() throws ClientException, IOException
    {
        if (session.serverAddress == null) {
            throw new ClientException(ClientException.CALL_PASSIVE_FIRST);
        }
        try {
            localServer.setActive(session.serverAddress);
        } catch (java.net.UnknownHostException e) {
            throw new ClientException(ClientException.UNKNOWN_HOST);
        }
    }

    /**
     * Starts local server in passive server mode, with default parameters.
     * In other words, behaves like
     * setLocalPassive(FTPServerFacade.ANY_PORT, FTPServerFacade.DEFAULT_QUEUE)
     **/
    public HostPort setLocalPassive() throws IOException
    {
        return localServer.setPassive(portRange, FTPServerFacade.DEFAULT_QUEUE);
    }

    /**
     * Starts the local server in passive server mode.
     *
     * @param range port range at which local server should be listening
     * @param queue max size of queue of awaiting new connection
     *              requests
     * @return the server address
     **/
    public HostPort setLocalPassive(PortRange range, int queue) throws IOException
    {
        return localServer.setPassive(range, queue);
    }

    /**
     * Changes the default client timeout parameters.
     * In the beginning of the transfer, the critical moment is the wait
     * for the initial server reply. If it does not arrive after timeout,
     * client assumes that the transfer could not start for some reason and
     * aborts the operation. Default timeout in miliseconds
     * is Session.DEFAULT_MAX_WAIT. During the waiting period,
     * client polls the control channel once a certain period, which is by
     * default set to Session.DEFAULT_WAIT_DELAY.
     * <br>
     * Use this method to change these parameters.
     *
     * @param maxWait   timeout in miliseconds
     * @param waitDelay polling period
     **/
    public void setClientWaitParams(int maxWait, int waitDelay)
    {
        if (maxWait <= 0 || waitDelay <= 0) {
            throw new IllegalArgumentException("Parameter is less than 0");
        }
        this.session.maxWait = maxWait;
        this.session.waitDelay = waitDelay;
    }

    /**
     * Sets the supplied options to the server.
     */
    public void setOptions(Options opts) throws IOException, ServerException
    {
        Command cmd = new Command("OPTS", opts.toFtpCmdArgument());

        try {
            controlChannel.execute(cmd);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    urce,
                    "Server refused setting options");
        }

        localServer.setOptions(opts);
    }

    /**
     * Sets restart parameter of the next transfer.
     *
     * @param restartData marker to use
     * @throws ServerException if the file does not exist or
     *                         an error occured.
     */
    public void setRestartMarker(RestartData restartData)
            throws IOException, ServerException
    {
        Command cmd = new Command("REST", restartData.toFtpCmdArgument());
        Reply reply = null;
        try {
            reply = controlChannel.exchange(cmd);
        } catch (FTPReplyParseException e) {
            throw ServerException.embedFTPReplyParseException(e);
        }

        if (!Reply.isPositiveIntermediate(reply)) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    new UnexpectedReplyCodeException(reply));
        }
    }

    /**
     * Performs user authorization with specified
     * user and password.
     *
     * @param user     username
     * @param password user password
     * @throws ServerException on server refusal
     */
    public void authorize(String user, String password)
            throws IOException, ServerException
    {

        Reply userReply = null;
        try {
            userReply = controlChannel.exchange(new Command("USER", user));
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }

        if (Reply.isPositiveIntermediate(userReply)) {
            Reply passReply = null;

            try {
                passReply =
                        controlChannel.exchange(new Command("PASS", password));
            } catch (FTPReplyParseException rpe) {
                throw ServerException.embedFTPReplyParseException(rpe);
            }

            if (!Reply.isPositiveCompletion(passReply)) {
                throw ServerException.embedUnexpectedReplyCodeException(
                        new UnexpectedReplyCodeException(passReply),
                        "Bad password.");
            }

            // i'm logged in

        } else if (Reply.isPositiveCompletion(userReply)) {

            // i'm logged in 

        } else {
            throw ServerException.embedUnexpectedReplyCodeException(
                    new UnexpectedReplyCodeException(userReply),
                    "Bad user.");
        }
        this.session.authorized = true;
        this.username = user;
    }

    public String getUserName()
    {
        return this.username;
    }

    /**
     * Retrieves the file from the remote server.
     *
     * @param remoteFileName remote file name
     * @param sink           sink to which the data will be written
     * @param mListener      restart marker listener (currently not used)
     */
    public void get(String remoteFileName,
                    DataSink sink,
                    MarkerListener mListener)
            throws IOException, ClientException, ServerException
    {
        checkTransferParamsGet();

        localServer.store(sink);
        controlChannel.write(new Command("RETR", remoteFileName));

        transferRunSingleThread(localServer.getControlChannel(), mListener);
    }

    /**
     * Retrieves the file from the remote server.
     *
     * @param remoteFileName remote file name
     * @param sink           sink to which the data will be written
     * @param mListener      restart marker listener (currently not used)
     */
    public TransferState asynchGet(String remoteFileName,
                                   DataSink sink,
                                   MarkerListener mListener)
            throws IOException, ClientException, ServerException
    {
        checkTransferParamsGet();

        localServer.store(sink);
        controlChannel.write(new Command("RETR", remoteFileName));

        return transferStart(localServer.getControlChannel(), mListener);
    }

    /**
     * Stores file at the remote server.
     *
     * @param remoteFileName remote file name
     * @param source         data will be read from here
     * @param mListener      restart marker listener (currently not used)
     */
    public void put(String remoteFileName,
                    DataSource source,
                    MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {
        put(remoteFileName, source, mListener, false);
    }

    /**
     * Stores file at the remote server.
     *
     * @param remoteFileName remote file name
     * @param source         data will be read from here
     * @param mListener      restart marker listener (currently not used)
     * @param append         append to the end of file or overwrite
     */
    public void put(String remoteFileName,
                    DataSource source,
                    MarkerListener mListener,
                    boolean append)
            throws IOException, ServerException, ClientException
    {
        checkTransferParamsPut();
        if (useAllo && source.totalSize() != -1) {
            allocate(source.totalSize());
        }
        localServer.retrieve(source);
        if (append) {
            controlChannel.write(new Command("APPE", remoteFileName));
        } else {
            controlChannel.write(new Command("STOR", remoteFileName));
        }

        transferRunSingleThread(localServer.getControlChannel(), mListener);
    }

    /**
     * Stores file at the remote server.
     *
     * @param remoteFileName remote file name
     * @param source         data will be read from here
     * @param mListener      restart marker listener (currently not used)
     */
    public TransferState asynchPut(String remoteFileName,
                                   DataSource source,
                                   MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {
        return asynchPut(remoteFileName, source, mListener, false);
    }

    /**
     * Stores file at the remote server.
     *
     * @param remoteFileName remote file name
     * @param source         data will be read from here
     * @param mListener      restart marker listener (currently not used)
     * @param append         append to the end of file or overwrite
     */
    public TransferState asynchPut(String remoteFileName,
                                   DataSource source,
                                   MarkerListener mListener,
                                   boolean append)
            throws IOException, ServerException, ClientException
    {
        checkTransferParamsPut();
        if (useAllo && source.totalSize() != -1) {
            allocate(source.totalSize());
        }
        localServer.retrieve(source);
        if (append) {
            controlChannel.write(new Command("APPE", remoteFileName));
        } else {
            controlChannel.write(new Command("STOR", remoteFileName));
        }

        return transferStart(localServer.getControlChannel(), mListener);
    }

    /**
     * Performs third-party transfer between two servers.
     *
     * @param remoteSrcFile source filename
     * @param destination   another client connected to destination server
     * @param remoteDstFile destination filename
     * @param append        enables append mode; if true,
     *                      data will be appened to the remote file, otherwise
     *                      file will be overwritten.
     * @param mListener     marker listener.
     *                      Can be set to null.
     */
    public void transfer(String remoteSrcFile,
                         FTPClient destination,
                         String remoteDstFile,
                         boolean append,
                         MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {

        session.matches(destination.session);

        // if transfer modes have not been defined, 
        // set this (source) as active
        if (session.serverMode == Session.SERVER_DEFAULT) {

            HostPort hp = destination.setPassive();
            setActive(hp);

        }

        destination.controlChannel.write(
                new Command((append) ? "APPE" : "STOR", remoteDstFile));

        controlChannel.write(new Command("RETR", remoteSrcFile));

        transferRunSingleThread(destination.controlChannel, mListener);
    }

    /**
     * Actual transfer management.
     * Transfer is controlled by two new threads listening
     * to the two servers.
     **/
    protected void transferRun(BasicClientControlChannel other,
                               MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {
        TransferState transferState = transferBegin(other, mListener);
        transferWait(transferState);
    }

    protected TransferState transferBegin(BasicClientControlChannel other,
                                          MarkerListener mListener)
    {
        // this structure will contain up to date information
        // about the state of transfer at both sides
        TransferState transferState = new TransferState();

        // thread monitoring our server during transfer
        // (that is, the server associated with this FTPClient)
        TransferMonitor ourMonitor =
                new TransferMonitor(
                        controlChannel,
                        transferState,
                        mListener,
                        session.maxWait,
                        session.waitDelay,
                        TransferMonitor.LOCAL);

        // thread monitoring other server during transfer
        // (that is, the server associated with the other FTPClient)
        TransferMonitor otherMonitor =
                new TransferMonitor(
                        other,
                        transferState,
                        mListener,
                        session.maxWait,
                        session.waitDelay,
                        TransferMonitor.REMOTE);

        ourMonitor.setOther(otherMonitor);
        otherMonitor.setOther(ourMonitor);

        // start two threads controling the transfer
        ourMonitor.start(false);
        otherMonitor.start(false);

        return transferState;
    }

    protected TransferState transferStart(BasicClientControlChannel other,
                                          MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {
        TransferState transferState = transferBegin(other, mListener);
        transferState.waitForStart();
        return transferState;
    }

    protected void transferWait(TransferState transferState)
            throws IOException, ServerException, ClientException
    {
        transferState.waitForEnd();
    }

    protected void transferRunSingleThread(BasicClientControlChannel other,
                                           MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {
        // this structure will contain up to date information
        // about the state of transfer at both sides
        TransferState transferState = new TransferState();

        // thread monitoring our server during transfer
        // (that is, the server associated with this FTPClient)
        TransferMonitor ourMonitor =
                new TransferMonitor(
                        controlChannel,
                        transferState,
                        mListener,
                        session.maxWait,
                        session.waitDelay,
                        TransferMonitor.LOCAL);

        // thread monitoring other server during transfer
        // (that is, the server associated with the other FTPClient)
        TransferMonitor otherMonitor =
                new TransferMonitor(
                        other,
                        transferState,
                        mListener,
                        session.maxWait,
                        session.waitDelay,
                        TransferMonitor.REMOTE);

        ourMonitor.setOther(otherMonitor);
        otherMonitor.setOther(ourMonitor);

        // controling the other connection - non-blocking
        otherMonitor.start(false);
        // control this connection - blocking
        ourMonitor.start(true);

        transferState.waitForEnd();
    }

    /**
     * Executes arbitrary operation on the server.
     * <p>
     * <br><b>Note</b>: <i>This is potentially dangerous operation.
     * Depending on the command executed it might put the server in a
     * different state from the state the client is expecting.</i>
     *
     * @param command command to execute
     * @return the Reply to the operation.
     * @throws IOException     in case of I/O error.
     * @throws ServerException if operation failed.
     */
    public Reply quote(String command) throws IOException, ServerException
    {
        Command cmd = new Command(command);
        return doCommand(cmd);
    }

    /**
     * Executes site-specific operation (using the SITE command).
     * <p>
     * <br><b>Note</b>: <i>This is potentially dangerous operation.
     * Depending on the command executed it might put the server in a
     * different state from the state the client is expecting.</i>
     *
     * @param args parameters for the SITE operation.
     * @return the Reply to the operation.
     * @throws IOException     in case of I/O error
     * @throws ServerException if operation failed.
     */
    public Reply site(String args) throws IOException, ServerException
    {
        Command cmd = new Command("SITE", args);
        return doCommand(cmd);
    }

    private Reply doCommand(Command cmd) throws IOException, ServerException
    {
        try {
            return controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }

    /**
     * Reserve sufficient storage to accommodate the new file to be
     * transferred.
     *
     * @param size the amount of space to reserve
     * @throws ServerException if an error occured.
     */
    public void allocate(long size) throws IOException, ServerException
    {
        Command cmd = new Command("ALLO", String.valueOf(size));
        Reply reply = null;
        try {
            reply = controlChannel.execute(cmd);
        } catch (UnexpectedReplyCodeException urce) {
            throw ServerException.embedUnexpectedReplyCodeException(urce);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }

    // basic compatibility API

    public long size(String filename) throws IOException, ServerException
    {
        return getSize(filename);
    }

    public Date lastModified(String filename)
            throws IOException, ServerException
    {
        return getLastModified(filename);
    }

    public void get(String remoteFileName, File localFile)
            throws IOException, ClientException, ServerException
    {
        DataSink sink = new DataSinkStream(new FileOutputStream(localFile));
        get(remoteFileName, sink, null);
    }

    public void put(File localFile, String remoteFileName, boolean append)
            throws IOException, ServerException, ClientException
    {
        DataSource source =
                new DataSourceStream(new FileInputStream(localFile));
        put(remoteFileName, source, null, append);
    }

    /**
     * Enables/disables passive data connections.
     *
     * @param passiveMode if true passive connections will be
     *                    established. If false, they will not.
     */
    public void setPassiveMode(boolean passiveMode)
            throws IOException, ClientException, ServerException
    {
        if (passiveMode) {
            setPassive();
            setLocalActive();
        } else {
            setLocalPassive();
            setActive();
        }
    }

    public boolean isPassiveMode()
    {
        return (this.session.serverMode == Session.SERVER_PASSIVE);
    }


    //////////////////////////////////////////////////////////////////////
    // Implementation of GFD.47 compliant GETPUT support. The reason
    // why this is implemented in FTPClient rather than GridFTPClient
    // is, that GFD.47 support is detected via feature strings and is
    // thus independent of GSI authentication.


    /**
     * Throws ServerException if GFD.47 GETPUT is not supported or
     * cannot be used.
     */
    protected void checkGETPUTSupport()
            throws ServerException, IOException
    {
        if (!isFeatureSupported(FeatureList.GETPUT)) {
            throw new ServerException(ServerException.UNSUPPORTED_FEATURE);
        }

        if (controlChannel.isIPv6()) {
            throw new ServerException(ServerException.UNSUPPORTED_FEATURE,
                                      "Cannot use GridFTP2 with IP 6");
        }
    }

    /**
     * Regular expression for matching the port information of a
     * GFD.47 127 reply.
     */
    public static final Pattern portPattern =
            Pattern.compile("\\d+,\\d+,\\d+,\\d+,\\d+,\\d+");

    /**
     * Reads a GFD.47 compliant 127 reply and extracts the port
     * information from it.
     */
    protected HostPort get127Reply()
            throws ServerException, IOException, FTPReplyParseException
    {
        Reply reply = controlChannel.read();

        if (Reply.isTransientNegativeCompletion(reply)
            || Reply.isPermanentNegativeCompletion(reply)) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    new UnexpectedReplyCodeException(reply), reply.getMessage());
        }

        if (reply.getCode() != 127) {
            throw new ServerException(ServerException.WRONG_PROTOCOL,
                                      reply.getMessage());
        }

        Matcher matcher = portPattern.matcher(reply.getMessage());
        if (!matcher.find()) {
            throw new ServerException(ServerException.WRONG_PROTOCOL,
                                      "Cannot parse 127 reply: "
                                      + reply.getMessage());
        }

        return new HostPort(matcher.group());
    }

    /**
     * Writes a GFD.47 compliant GET or PUT command to the control
     * channel.
     *
     * @param command Either "GET" or "PUT", depending on the command to issue
     * @param passive True if the "pasv" parameter should be used
     * @param port    If passive is false, this is the port for
     *                the "port" parameter
     * @param mode    The value for the "mode" parameter, or 0 if the
     *                parameter should not be specified
     * @param path    The value for the "path" parameter
     */
    private void issueGETPUT(String command,
                             boolean passive,
                             HostPort port,
                             int mode,
                             String path)
            throws IOException
    {
        Command cmd =
                new Command(command,
                            (passive
                             ? "pasv"
                             : ("port=" + port.toFtpCmdArgument())
                            ) + ";" +
                            "path=" + path + ";" +
                            (mode > 0
                             ? "mode=" + getModeStr(mode) + ";"
                             : ""));
        controlChannel.write(cmd);
    }

    /**
     * Retrieves a file using the GFD.47 (a.k.a GridFTP2) GET command.
     * <p>
     * Notice that as a side effect this method may change the local
     * server facade passive/active mode setting. The caller should
     * not rely on this setting after call to get2.
     * <p>
     * Even though the active/passive status of the current session is
     * ignored for the actual transfer, it still has to be in a
     * consistent state prior to calling gridftp2Get.
     *
     * @param remoteFileName file to retrieve
     * @param passive        whether to configure the server to be passive
     * @param sink           data sink to store the file
     * @param mListener      marker listener
     **/
    public void get2(String remoteFileName,
                     boolean passive,
                     DataSink sink,
                     MarkerListener mListener)
            throws IOException,
            ClientException,
            ServerException
    {
        int serverMode = session.serverMode;
        HostPort serverAddress = session.serverAddress;

        try {
            // Can we use GETPUT?
            checkGETPUTSupport();

            // Check sanity of arguments
            if (session.transferMode == GridFTPSession.MODE_EBLOCK && passive) {
                throw new IllegalArgumentException("Sender must be active in extended block mode");
            }

            // All parameters set correctly (or still unset)?
            Session localSession = localServer.getSession();
            session.matches(localSession);

            // Connection setup depends a lot on whether we use
            // passive or active mode. The passive party needs to be
            // configured before the active party.
            if (passive) {
                issueGETPUT("GET", true, null, 0, remoteFileName);
                session.serverMode = Session.SERVER_PASSIVE;
                session.serverAddress = get127Reply();
                setLocalActive();
                localServer.store(sink);
            } else {
                HostPort hp = setLocalPassive();
                localServer.store(sink);
                issueGETPUT("GET", false, hp, 0, remoteFileName);
                session.serverMode = Session.SERVER_ACTIVE;
            }

            transferRunSingleThread(localServer.getControlChannel(),
                                    mListener);

        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } finally {
            session.serverMode = serverMode;
            session.serverAddress = serverAddress;
        }
    }


    /**
     * Retrieves a file asynchronously using the GFD.47 (a.k.a
     * GridFTP2) GET command.
     * <p>
     * Notice that as a side effect this method may change the local
     * server facade passive/active mode setting. The caller should
     * not rely on this setting after call to gridftp2Get.
     * <p>
     * Even though the active/passive status of the current session is
     * ignored for the actual transfer, it still has to be in a
     * consistent state prior to calling gridftp2Get.
     *
     * @param remoteFileName file to retrieve
     * @param passive        whether to configure the server to be passive
     * @param sink           data sink to store the file
     * @param mListener      marker listener
     **/
    public TransferState asynchGet2(String remoteFileName,
                                    boolean passive,
                                    DataSink sink,
                                    MarkerListener mListener)
            throws IOException,
            ClientException,
            ServerException
    {
        int serverMode = session.serverMode;
        HostPort serverAddress = session.serverAddress;

        try {

            // Can we use GETPUT?
            checkGETPUTSupport();

            // Check sanity of arguments
            if (session.transferMode == GridFTPSession.MODE_EBLOCK && passive) {
                throw new IllegalArgumentException("Sender must be active in extended block mode");
            }

            // All parameters set correctly (or still unset)?
            Session localSession = localServer.getSession();
            session.matches(localSession);

            // Connection setup depends a lot on whether we use
            // passive or active mode. The passive party needs to be
            // configured before the active party.
            if (passive) {
                issueGETPUT("GET", true, null, 0, remoteFileName);
                session.serverMode = Session.SERVER_PASSIVE;
                session.serverAddress = get127Reply();
                setLocalActive();
                localServer.store(sink);
            } else {
                HostPort hp = setLocalPassive();
                localServer.store(sink);
                issueGETPUT("GET", false, hp, 0, remoteFileName);
                session.serverMode = Session.SERVER_ACTIVE;
            }

            return transferStart(localServer.getControlChannel(), mListener);

        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } finally {
            // This might not be the most elegant or correct
            // solution. On the other hand, these parameters do not
            // seem to be used after transferStart() and it is much
            // easier to restore the old values now rather than when
            // the transfer completes.
            session.serverMode = serverMode;
            session.serverAddress = serverAddress;
        }
    }

    /**
     * Stores a file at the remote server using the GFD.47 (a.k.a
     * GridFTP2) PUT command.
     * <p>
     * Notice that as a side effect this method may change the local
     * server facade passive/active mode setting. The caller should
     * not rely on this setting after call to gridftp2Get.
     * <p>
     * Even though the active/passive status of the current session is
     * ignored for the actual transfer, it still has to be in a
     * consistent state prior to calling gridftp2Get.
     *
     * @param remoteFileName file to retrieve
     * @param passive        whether to configure the server to be passive
     * @param source         data will be read from here
     * @param mListener      marker listener
     **/
    public void put2(String remoteFileName,
                     boolean passive,
                     DataSource source,
                     MarkerListener mListener)
            throws IOException,
            ClientException,
            ServerException
    {

        int serverMode = session.serverMode;
        HostPort serverAddress = session.serverAddress;

        try {
            // Can we use GETPUT?
            checkGETPUTSupport();

            // Check sanity of arguments
            if (session.transferMode == GridFTPSession.MODE_EBLOCK && !passive) {
                throw new IllegalArgumentException("Sender must be active in extended block mode");
            }

            // All parameters set correctly (or still unset)?
            Session localSession = localServer.getSession();
            session.matches(localSession);

            // Connection setup depends a lot on whether we use
            // passive or active mode. The passive party needs to be
            // configured before the active party.
            if (passive) {
                issueGETPUT("PUT", true, null, 0, remoteFileName);
                session.serverMode = Session.SERVER_PASSIVE;
                session.serverAddress = get127Reply();
                setLocalActive();
                localServer.retrieve(source);
            } else {
                HostPort hp = setLocalPassive();
                localServer.retrieve(source);
                issueGETPUT("PUT", false, hp, 0, remoteFileName);
                session.serverMode = Session.SERVER_ACTIVE;
            }

            transferRunSingleThread(localServer.getControlChannel(),
                                    mListener);

        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } finally {
            session.serverMode = serverMode;
            session.serverAddress = serverAddress;
        }
    }


    /**
     * Stores a file at the remote server using the GFD.47 (a.k.a
     * GridFTP2) PUT command.
     * <p>
     * Notice that as a side effect this method may change the local
     * server facade passive/active mode setting. The caller should
     * not rely on this setting after call to gridftp2Get.
     * <p>
     * Even though the active/passive status of the current session is
     * ignored for the actual transfer, it still has to be in a
     * consistent state prior to calling gridftp2Get.
     *
     * @param remoteFileName file to retrieve
     * @param passive        whether to configure the server to be passive
     * @param source         data will be read from here
     * @param mListener      marker listener
     **/
    public TransferState asynchPut2(String remoteFileName,
                                    boolean passive,
                                    DataSource source,
                                    MarkerListener mListener)
            throws IOException,
            ClientException,
            ServerException
    {
        int serverMode = session.serverMode;
        HostPort serverAddress = session.serverAddress;

        try {

            // Can we use GETPUT?
            checkGETPUTSupport();

            // Check sanity of arguments
            if (session.transferMode == GridFTPSession.MODE_EBLOCK && !passive) {
                throw new IllegalArgumentException("Sender must be active in extended block mode");
            }

            // All parameters set correctly (or still unset)?
            Session localSession = localServer.getSession();
            session.matches(localSession);

            // Connection setup depends a lot on whether we use
            // passive or active mode. The passive party needs to be
            // configured before the active party.
            if (passive) {
                issueGETPUT("PUT", true, null, 0, remoteFileName);
                session.serverMode = Session.SERVER_PASSIVE;
                session.serverAddress = get127Reply();
                setLocalActive();
                localServer.retrieve(source);
            } else {
                HostPort hp = setLocalPassive();
                localServer.retrieve(source);
                issueGETPUT("PUT", false, hp, 0, remoteFileName);
                session.serverMode = Session.SERVER_ACTIVE;
            }

            return transferStart(localServer.getControlChannel(), mListener);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        } finally {
            // This might not be the most elegant or correct
            // solution. On the other hand, these parameters do not
            // seem to be used after transferStart() and it is much
            // easier to restore the old values now rather than when
            // the transfer completes.
            session.serverMode = serverMode;
            session.serverAddress = serverAddress;
        }
    }

    /**
     * Performs third-party transfer between two servers. If possibly,
     * GFD.47 (a.k.a GridFTP2) GET and PUT commands are used.
     *
     * @param destination   client connected to source server
     * @param remoteSrcFile source filename
     * @param destination   client connected to destination server
     * @param remoteDstFile destination filename
     * @param mode          data channel mode or 0 to use the current mode
     * @param mListener     marker listener.
     *                      Can be set to null.
     */
    public static void transfer(FTPClient source,
                                String remoteSrcFile,
                                FTPClient destination,
                                String remoteDstFile,
                                int mode,
                                MarkerListener mListener)
            throws IOException, ServerException, ClientException
    {
        try {
            // Although neither mode nor passive setting from in the
            // session is used, we still perform this check, since
            // other things may be checked as well.
            source.session.matches(destination.session);

            HostPort hp;
            if (destination.isFeatureSupported(FeatureList.GETPUT)) {
                destination.issueGETPUT("PUT", true, null,
                                        mode, remoteDstFile);
                hp = destination.get127Reply();
            } else {
                if (mode > 0) {
                    destination.setMode(mode);
                }
                hp = destination.setPassive();
                destination.controlChannel.write(new Command("STOR", remoteDstFile));
            }

            if (source.isFeatureSupported(FeatureList.GETPUT)) {
                source.issueGETPUT("GET", false, hp, mode, remoteSrcFile);
            } else {
                if (mode > 0) {
                    source.setMode(mode);
                }
                source.setActive(hp);
                source.controlChannel.write(new Command("RETR", remoteSrcFile));
            }

            source.transferRunSingleThread(destination.controlChannel, mListener);
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(rpe);
        }
    }


    public boolean isActiveMode()
    {
        return (this.session.serverMode == Session.SERVER_ACTIVE);
    }

    /**
     * Controls whether the client attempts to send an ALLO command
     * before a STOR request during the put/asyncPut calls. This is
     * disabled by default in the FTP client and enabled by default
     * in the GridFTP client. This setting will apply to all
     * subsequent transfers.
     *
     * @param useAllo <code>true</code> if the client should try
     *                to send an ALLO command before a STOR request
     */
    public void setUseAllo(boolean useAllo)
    {
        this.useAllo = useAllo;
    }

    /**
     * Determines whether this client is configured to send an ALLO
     * command before a STOR request in the put/asyncPut methods.
     */
    public boolean getUseAllo()
    {
        return this.useAllo;
    }


    /**
     * According to
     * {@link http://www.ogf.org/documents/GFD.47.pdf [GridFTP v2 Protocol Description]}
     * checksum feature has the following syntax:
     * <pre>
     * CKSUM <algorithm>[, …]
     * </pre>
     * getSupportedCksumAlgorithms parses checsum feauture parms and form a
     * list of checksum algorithms supported by the server
     *
     * @return a list of checksum algorithms supported by the server in the order
     * specified by the server
     * @throws ClientException
     * @throws org.dcache.ftp.client.exception.ServerException
     * @throws java.io.IOException
     */
    public List<String> getSupportedCksumAlgorithms()
            throws ClientException, ServerException, IOException
    {

        if (algorithms != null) {
            return algorithms;
        }

        // check if the CKSUM algorithm is supported by the server
        List<FeatureList.Feature> cksumFeature =
                getFeatureList().getFeature(FeatureList.CKSUM);
        if (cksumFeature == null) {
            algorithms = Collections.emptyList();
            return algorithms;
        }

        algorithms = new ArrayList();
        for (FeatureList.Feature feature : cksumFeature) {
            String[] parms = feature.getParms().split(",");
            Collections.addAll(algorithms, parms);
        }
        return algorithms;
    }

    public boolean isCksumAlgorithmSupported(String algorithm)
            throws ClientException, ServerException, IOException
    {
        return getSupportedCksumAlgorithms().contains(algorithm.toUpperCase());
    }

    private void checkCksumSupport(String algorithm)
            throws ClientException, ServerException, IOException
    {

        // check if the CKSUM is supported by the server
        if (!isFeatureSupported(FeatureList.CKSUM)) {
            throw new ClientException(
                    ClientException.OTHER,
                    FeatureList.CKSUM + " is not supported by server");
        }

        // check if the CKSUM algorithm is supported by the server
        if (!isCksumAlgorithmSupported(algorithm)) {
            throw new ClientException(
                    ClientException.OTHER,
                    FeatureList.CKSUM + " algorithm " + algorithm +
                    " is not supported by server");
        }

    }

    /**
     * implement GridFTP v2 CKSM command from
     * {@link http://www.ogf.org/documents/GFD.47.pdf [GridFTP v2 Protocol Description]}
     * <pre>
     * 5.1 CKSM
     * This command is used by the client to request checksum calculation over a portion or
     * whole file existing on the server. The syntax is:
     * CKSM <algorithm> <offset> <length> <path> CRLF
     * Server executes this command by calculating specified type of checksum over
     * portion of the file starting at the offset and of the specified length. If length is –1,
     * the checksum will be calculated through the end of the file. On success, the server
     * replies with
     * 2xx <checksum value>
     * Actual format of checksum value depends on the algorithm used, but generally,
     * hexadecimal representation should be used.
     * </pre>
     *
     * @param algorithm ckeckum alorithm
     * @param offset
     * @param length
     * @param path
     * @return ckecksum value returned by the server
     * @throws ClientException
     * @throws org.dcache.ftp.client.exception.ServerException
     * @throws java.io.IOException
     */
    public String getChecksum(String algorithm,
                              long offset,
                              long length,
                              String path)
            throws ClientException, ServerException, IOException
    {

        // check if we the cksum commands and specific algorithm are supported
        checkCksumSupport(algorithm);

        // form CKSM command
        String parameters = String.format("%s %d %d %s", algorithm, offset, length, path);
        Command cmd = new Command("CKSM", parameters);

        // transfer command, obtain reply
        Reply cksumReply = doCommand(cmd);

        // check for error
        if (!Reply.isPositiveCompletion(cksumReply)) {
            throw new ServerException(ServerException.SERVER_REFUSED,
                                      cksumReply.getMessage());
        }

        return cksumReply.getMessage();
    }

    /**
     * GridFTP v2 CKSM command for the whole file
     *
     * @param algorithm ckeckum alorithm
     * @param path
     * @return ckecksum value returned by the server
     * @throws ClientException
     * @throws org.dcache.ftp.client.exception.ServerException
     * @throws java.io.IOException
     */
    public String getChecksum(String algorithm,
                              String path)
            throws ClientException, ServerException, IOException
    {
        return getChecksum(algorithm, 0, -1, path);
    }

    /**
     * implement GridFTP v2 SCKS command as described in
     * {@link http://www.ogf.org/documents/GFD.47.pdf [GridFTP v2 Protocol Description]}
     * <pre>
     * 5.2 SCKS
     * This command is sent prior to upload command such as STOR, ESTO, PUT. It is used
     * to convey to the server that the checksum value for the file which is about to be
     * uploaded. At the end of transfer, server will calculate checksum for the received file,
     * and if it does not match, will consider the transfer to have failed. Syntax of the
     * command is:
     * SCKS <algorithm> <value> CRLF
     * Actual format of checksum value depends on the algorithm used, but generally,
     * hexadecimal representation should be used.
     * </pre>
     *
     * @param algorithm
     * @param value
     * @throws ClientException
     * @throws org.dcache.ftp.client.exception.ServerException
     * @throws java.io.IOException
     */
    public void setChecksum(String algorithm, String value)
            throws ClientException, ServerException, IOException
    {

        // check if we the cksum commands and specific algorithm are supported
        checkCksumSupport(algorithm);

        // form CKSM command
        String parameters = String.format("%s %s", algorithm, value);
        Command cmd = new Command("SCKS", parameters);

        // transfer command, obtain reply
        Reply cksumReply = doCommand(cmd);

        // check for error
        if (!Reply.isPositiveCompletion(cksumReply)) {
            throw new ServerException(ServerException.SERVER_REFUSED,
                                      cksumReply.getMessage());
        }

    }

} //FTPClient
