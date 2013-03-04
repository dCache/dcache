// $Id$

package org.dcache.srm.util;

import org.globus.ftp.Buffer;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.DataSink;
import org.globus.ftp.DataSource;
import org.globus.ftp.FeatureList;
import org.globus.ftp.GridFTPClient;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.HostPort;
import org.globus.ftp.RetrieveOptions;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.vanilla.Reply;
import org.globus.gsi.CredentialException;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
/**
 * THE CLASS IS NOT THREAD SAVE
 * DO ONLY ONE OPERATION (READ / WRITE) AT A TIME
 */

public class GridftpClient
{
    private static final Logger logger =
            LoggerFactory.getLogger(GridftpClient.class);

    private final static long DEFAULT_FIRST_BYTE_TIMEOUT=TimeUnit.HOURS.toMillis(1);
    private final static long DEFAULT_NEXT_BYTE_TIMEOUT=TimeUnit.MINUTES.toMillis(10);

    private long firstByteTimeout=DEFAULT_FIRST_BYTE_TIMEOUT;
    private long nextByteTimeout=DEFAULT_NEXT_BYTE_TIMEOUT;

    private final GridFTPClient _client;
    private final String _host;
    private String _cksmType;
    private String _cksmValue;

    private int _streamsNum = 10;
    private int _tcpBufferSize = 1024*1024;
    private int _bufferSize = 1024*1024;

    private volatile IDiskDataSourceSink _current_source_sink;
    private long _last_transfer_time = System.currentTimeMillis();

    private long _transferred;
    private boolean _closed;

    private static List<String> cksmTypeList =
            Arrays.asList("ADLER32","MD5","MD4");

    public GridftpClient(String host, int port,
                         int tcpBufferSize,
                         GSSCredential cred)
        throws IOException, ServerException, ClientException,
               CredentialException, GSSException
    {
        this(host, port, tcpBufferSize, 0, cred);
    }

    public GridftpClient(String host, int port,
                         int tcpBufferSize,
                         int bufferSize,
                         GSSCredential cred)
        throws IOException, ServerException, ClientException,
               CredentialException, GSSException
    {
        if(bufferSize >0) {
            _bufferSize = bufferSize;
            logger.debug("memory buffer size is set to "+bufferSize);
        }
        if(tcpBufferSize > 0)
            {
                _tcpBufferSize = tcpBufferSize;
                logger.debug("tcp buffer size is set to "+tcpBufferSize);
            }
        if(cred == null) {
            X509Credential gcred = X509Credential.getDefaultCredential();
            cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        }
        _host = host;
        logger.debug("connecting to "+_host+" on port "+port);

        _client  = new GridFTPClient(_host, port);
        _client.setLocalTCPBufferSize(_tcpBufferSize);
        logger.debug("gridFTPClient tcp buffer size is set to "+_tcpBufferSize);
        _client.authenticate(cred); /* use credentials */
        _client.setType(GridFTPSession.TYPE_IMAGE);
    }

    public void setFirstByteTimeout(long timeout) {
        firstByteTimeout = timeout;
    }

    public void setNextByteTimeout(long timeout) {
        nextByteTimeout = timeout;
    }

    public long getFirstByteTimeout() {
        return firstByteTimeout;
    }

    public long getNextByteTimeout() {
        return nextByteTimeout;
    }

    public long getLastTransferTime()
    {
        //do local copy to avoid null pointer exception
        IDiskDataSourceSink source_sink = _current_source_sink;
        if (source_sink != null) {
            _last_transfer_time = source_sink.getLast_transfer_time();
        }
        return _last_transfer_time;
    }

    public long getTransfered()
    {
        //do local copy to avoid null pointer exception
        IDiskDataSourceSink source_sink = _current_source_sink;
        if (source_sink != null) {
            _transferred = source_sink.getTransfered();
        }
        return _transferred;
    }

    public static long getAdler32(ReadableByteChannel fileChannel)
        throws IOException
    {
        Adler32 java_addler = new Adler32();
        byte [] buffer = new byte[4096] ;
        ByteBuffer bb = ByteBuffer.wrap(buffer);

        while(true){
            bb.clear();
            int rc = fileChannel.read(bb);
            if( rc <=0 ) {
                break;
            }

            java_addler.update(buffer , 0 , rc ) ;
        }
        return java_addler.getValue();
    }

    public static String getCksmValue(ReadableByteChannel fileChannel,String type)
        throws IOException,NoSuchAlgorithmException
    {
        if (type.equalsIgnoreCase("adler32")) {
            return long32bitToHexString(getAdler32(fileChannel));
        }

        MessageDigest md = MessageDigest.getInstance(type);
        ByteBuffer bb = ByteBuffer.allocate(4096);

        while(true){
            bb.clear();

            int rc = fileChannel.read(bb) ;
            if( rc <=0 ) {
                break;
            }

            bb.flip();
            md.update(bb) ;
        }
        return printbytes(md.digest());
    }


    public String list(String directory,boolean serverPassive)
        throws IOException, ClientException, ServerException
    {
        setCommonOptions(false,serverPassive);
        _client.changeDir(directory);
        if (serverPassive) {
            _client.setPassive();
            _client.setLocalActive();
        } else {
            _client.setLocalPassive();
            _client.setActive();
        }
        final ByteArrayOutputStream received = new ByteArrayOutputStream(1000);

        // unnamed DataSink subclass will write data channel content
        // to "received" stream.

        DataSink sink = new DataSink() {
                @Override
                public void write(Buffer buffer) throws IOException {
                    received.write(buffer.getBuffer(), 0, buffer.getLength());
                }
                @Override
                public void close() throws IOException {
                }
        };

        _client.list(" "," ",sink);
        return received.toString();
    }

    public long getSize(String ftppath)
        throws IOException, ServerException
    {
        return _client.getSize(ftppath);
    }

    private void setCommonOptions(boolean emode,
                                  boolean passive_server_mode)
        throws IOException, ClientException, ServerException
    {
        if (_client.isFeatureSupported("DCAU")) {
            _client.setDataChannelAuthentication(DataChannelAuthentication.NONE);
        }
        logger.debug("set local data channel authentication mode to None");
        _client.setLocalNoDataChannelAuthentication();

        if(emode) {
            _client.setMode(GridFTPSession.MODE_EBLOCK);
            // adding parallelism
            logger.debug("parallelism: " + _streamsNum);
            _client.setOptions(new RetrieveOptions(_streamsNum));
        }
        else {
            _client.setMode(GridFTPSession.MODE_STREAM);
            logger.debug("stream mode transfer");

            if (!_client.isFeatureSupported("GETPUT")) {
                if(passive_server_mode){
                    logger.debug("server is passive");
                    HostPort serverHostPort = _client.setPassive();
                    logger.debug("serverHostPort="+serverHostPort.getHost()+":"+serverHostPort.getPort());
                    _client.setLocalActive();
                }else{
                    logger.debug("server is active");
                    _client.setLocalPassive();
                    _client.setActive();
                }
            }
        }

        //wait for ~ 24 days before timing out, poll every minute
        _client.setClientWaitParams(Integer.MAX_VALUE,1000);
    }

    /**
     * THIS IS A TEMPORARY CODE TO MAKE NCSA GSIFTP SERVER
     * TO STAGE FILES INSTEAD OF FAILING TRANSFERS
     * THIS REALLY IS @##@%$^
     */
    private void sendNCSAWaitCommand()
        throws IOException, ServerException, FTPReplyParseException,
               UnexpectedReplyCodeException
    {
        logger.debug(" sending wait command to ncsa host " + _host);
        Reply reply = _client.quote("SITE WAIT");
        logger.debug("Reply is "+reply);
        if(Reply.isPositiveCompletion( reply)) {
            logger.debug("sending wait command successful");
        } else {
            logger.error("WARNING: sending wait command failed");
        }
    }

    // setChecksum will set cksmType and cksmValue for the FTP session
    // If both values are set the write and read are verified using supplied information
    // If only the type is set, value is calculated from the file's copy on the disk
    // If send_checksum is used in the write methods,  type and value will be set
    // to adler32 and dynamically calcualted upon completion of the transfer by the client side
    public void setChecksum(String cksmType,String cksmValue){
        _cksmType = cksmType;
        _cksmValue = cksmValue;
    }

    public String getChecksumValue() {
        return _cksmValue;
    }

    public String getChecksumType() {
        return _cksmType;
    }

    public void gridFTPRead(String sourcepath,
                            String destinationfilepath,
                            boolean emode,
                            boolean passive_server_mode)
        throws FileNotFoundException, IOException,
               ClientException, ServerException,
               FTPReplyParseException,UnexpectedReplyCodeException,
               InterruptedException, NoSuchAlgorithmException
    {
        FileChannel fileChannel = null;
        try {
            fileChannel = new RandomAccessFile(destinationfilepath,"rw").getChannel();
            gridFTPRead(sourcepath,fileChannel, emode,passive_server_mode);
        } finally {
            try {
                if(fileChannel != null) {
                    fileChannel.close();
                }
            } catch(IOException e) {
                logger.error(" closing of file "+destinationfilepath+" failed",e);
            }
        }
    }

    public void gridFTPRead(String sourcepath,
                            FileChannel fileChannel,
                            boolean emode,boolean passive_server_mode)
        throws IOException, ClientException, ServerException,
               FTPReplyParseException, UnexpectedReplyCodeException,
               InterruptedException, NoSuchAlgorithmException
    {

        DiskDataSourceSink sink =
            new DiskDataSourceSink(fileChannel,_bufferSize,false);
        gridFTPRead(sourcepath,sink, emode,passive_server_mode);
    }

    public void gridFTPRead(String sourcepath,
                            IDiskDataSourceSink sink,
                            boolean emode)
        throws IOException, ClientException, ServerException,
               FTPReplyParseException, UnexpectedReplyCodeException,
               InterruptedException, NoSuchAlgorithmException
    {
        gridFTPRead(sourcepath,sink,emode,true);
    }

    public void gridFTPRead(String sourcepath,
                            IDiskDataSourceSink sink,
                            boolean emode,
                            boolean passive_server_mode)
        throws IOException, ClientException, ServerException,
               FTPReplyParseException, UnexpectedReplyCodeException,
               InterruptedException, NoSuchAlgorithmException
    {
        logger.debug("gridFTPRead started");
        // size of the file
        setCommonOptions(emode,passive_server_mode);
        if(_host.toLowerCase().indexOf("ncsa") != -1) {
            sendNCSAWaitCommand();
        }

        final long size = _client.getSize(sourcepath);
        _current_source_sink = sink;
        TransferThread getter = new TransferThread(_client,sourcepath,sink,emode,passive_server_mode,true,size);
        getter.start();
        getter.waitCompletion(firstByteTimeout, nextByteTimeout);

        if(size - sink.getTransfered() >0) {
            logger.error("we wrote less then file size!!!");
            throw new IOException("we wrote less then file size!!!");
        }
        else if(size - sink.getTransfered() <0) {
            logger.error("we wrote more then file size!!!");
            throw new IOException("we wrote more then file size!!!");
        }

        logger.debug("gridFTPWrite() wrote "+sink.getTransfered()+"bytes");

        try {
          if ( _cksmType != null ) {
              verifyCksmValue(_current_source_sink, sourcepath);
          }
        } catch ( ChecksumNotSupported ex){
          logger.error("Checksum is not supported:"+ex.toString());
        } catch ( ChecksumValueFormatException cvfe) {
          logger.error("Checksum format is not valid:"+cvfe.toString());
        }

        //make these remeber last values
        getTransfered();
        getLastTransferTime();
        _current_source_sink = null;
    }

    public void gridFTPWrite(String sourcefilepath,
                             String destinationpath,
                             boolean emode,
                             boolean use_chksum)
        throws InterruptedException, ClientException, ServerException,
               IOException, NoSuchAlgorithmException
    {
        gridFTPWrite(sourcefilepath,destinationpath,emode,use_chksum,true);
    }

    public void gridFTPWrite(String sourcefilepath,
                             String destinationpath,
                             boolean emode,
                             boolean use_chksum,
                             boolean passive_server_mode )
        throws InterruptedException, ClientException, ServerException,
               IOException, NoSuchAlgorithmException
    {
        FileChannel fileChannel = null;
        try {
            fileChannel = new RandomAccessFile(sourcefilepath,"r").getChannel();
            gridFTPWrite(fileChannel, destinationpath, emode, use_chksum,passive_server_mode);
        } finally {
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                }
            } catch(IOException ioe) {
                logger.error(" closing of file "+sourcefilepath+" failed",ioe);
            }
        }
    }

    public void gridFTPWrite(FileChannel fileChannel,
                             String destinationpath,
                             boolean emode,
                             boolean use_chksum)
        throws InterruptedException, ClientException, ServerException,
               IOException, NoSuchAlgorithmException
    {
        gridFTPWrite(
                     fileChannel,
                     destinationpath,
                     emode,
                     use_chksum,
                     true);
    }

    public void gridFTPWrite(FileChannel fileChannel,
                             String destinationpath,
                             boolean emode,
                             boolean use_chksum,
                             boolean passive_server_mode)
        throws InterruptedException, ClientException, ServerException,
               IOException, NoSuchAlgorithmException
    {
        fileChannel.position(0);
        DiskDataSourceSink source = new DiskDataSourceSink(fileChannel,_bufferSize,true);
        gridFTPWrite( source, destinationpath, emode, use_chksum,passive_server_mode);

    }

    public void gridFTPWrite(IDiskDataSourceSink source,
                             String destinationpath,
                             boolean emode,
                             boolean use_chksum)
        throws InterruptedException, ClientException, ServerException,
               IOException, NoSuchAlgorithmException
    {
        gridFTPWrite(source,
                     destinationpath,
                     emode,
                     use_chksum,
                     true);

    }

    public void gridFTPWrite(IDiskDataSourceSink source,
                             String destinationpath,
                             boolean emode,
                             boolean use_chksum,
                             boolean passive_server_mode)
        throws InterruptedException, ClientException, ServerException,
               IOException, NoSuchAlgorithmException
    {
        logger.debug("gridFTPWrite started, destination path is "+destinationpath);

        setCommonOptions(emode,passive_server_mode);

        if(use_chksum || _cksmType != null) {

            sendCksmValue(source);
        }

        _current_source_sink = source;
        long diskFileLength = source.length();
        TransferThread putter = new TransferThread(_client,destinationpath,source,emode,passive_server_mode,false,diskFileLength);
        putter.start();
        putter.waitCompletion(firstByteTimeout, nextByteTimeout);

        if(diskFileLength > source.getTransfered() ) {
            logger.error("we read less then file size!!!");
            throw new IOException("we read less then file size!!!");
        }
        else if(diskFileLength < source.getTransfered() ) {
            logger.error("we read more then file size!!!");
            throw new IOException("we read more then file size!!!");
        }

        logger.debug("gridFTPWrite() wrote "+source.getTransfered()+"bytes");
        getTransfered();
        getLastTransferTime();
        _current_source_sink = null;
    }

    private void sendCksmValue(IDiskDataSourceSink source)
        throws IOException,NoSuchAlgorithmException,
            ClientException, ServerException {

        String commonCksumAlogorithm = getCommonChecksumAlgorithm();

        if ( commonCksumAlogorithm == null) {
            return;
        }

        try {
            if ( commonCksumAlogorithm.equals(_cksmType) && _cksmValue != null) {
                _client.setChecksum(_cksmType,_cksmValue);
            } else {
                String checkusValue =  source.getCksmValue(commonCksumAlogorithm);
                _client.setChecksum(commonCksumAlogorithm,checkusValue);
            }
        } catch ( Exception ex ){
            // send cksm error is often expected for non dCache sites
            logger.debug("Was not able to send checksum value:"+ex.toString());
        }
    }

    public String getCommonChecksumAlgorithm() throws IOException,
            ClientException, ServerException {

        if (!_client.isFeatureSupported(FeatureList.CKSUM)) {
            return null;
        }

        List<String> algorithms = _client.getSupportedCksumAlgorithms();



        if(_cksmType == null || _cksmType.equals("negotiate") ) {
            List<String> supportedByClientAndServer = new ArrayList(cksmTypeList);
            supportedByClientAndServer.retainAll(algorithms);

            //exit if no common algorithms are supported
            if(supportedByClientAndServer.isEmpty()) {
                return null;
            }

            return supportedByClientAndServer.get(0);
        }

        if( algorithms.contains(_cksmType)) {
            // checksum type is specified, but is not supported by the server
            return _cksmType;
        }

        return null;
    }

   public Checksum negotiateCksm(String path)
   throws IOException,
       ServerException,
       ClientException,
       ChecksumNotSupported,
       ChecksumValueFormatException
   {
        String commonAlgorithm = getCommonChecksumAlgorithm();
        if(commonAlgorithm == null) {
           throw new ChecksumNotSupported("Checksum is not supported : " +
                   "couldn't negotiate type value",0);

        }
        String serverCksmValue = _client.getChecksum(commonAlgorithm, path);
        return new Checksum(commonAlgorithm,serverCksmValue);
   }

    private void verifyCksmValue(IDiskDataSourceSink source,String remotePath)
        throws IOException,
        ServerException,
        ClientException,
        NoSuchAlgorithmException,
        ChecksumNotSupported,
        ChecksumValueFormatException
    {
        Checksum serverChecksum;
        if ( _cksmType == null ) {
            throw new IllegalArgumentException("verifyCksmValue: expected cksm type");
        }

        if ( _cksmType.equals("negotiate") ) {
            serverChecksum = negotiateCksm(remotePath);
        } else {
            serverChecksum = new Checksum(_cksmType,
                    _client.getChecksum(_cksmType, remotePath));
        }

        if ( _cksmValue == null ) {
            _cksmValue = source.getCksmValue(serverChecksum.type);
        }

        if ( !_cksmValue.equals(serverChecksum.value) ) {
            throw new IOException("Server side checksum:" + serverChecksum.value + " does not match client side checksum:" + _cksmValue);
        }
       // send gridftp message
    }

    public void close()
        throws IOException, ServerException
    {
        synchronized(this)
            {
                if(_closed) {
                    return;
                }
                else
                    {
                        _closed = true;
                    }
            }
        logger.debug("closing client : {}:{}", _client.getHost(), _client.getPort());
	try {
		_client.close(false);
	}
	catch (IOException e) {
	}
        logger.debug("closed client");

    }

    @Override
    protected void finalize() throws Throwable
    {
        try {
            close();
        }
        catch(Exception e) {
        }
        super.finalize();
    }

    /** Getter for property streamsNum.
     * @return Value of property streamsNum.
     *
     */
    public int getStreamsNum() {
        return _streamsNum;
    }

    /** Setter for property streamsNum.
     * @param streamsNum New value of property streamsNum.
     *
     */
    public void setStreamsNum(int streamsNum) {
        _streamsNum = streamsNum;
    }

    /** Getter for property tcpBufferSize.
     * @return Value of property tcpBufferSize.
     *
     */
    public int getTcpBufferSize() {
        return _tcpBufferSize;
    }

    /** Setter for property tcpBufferSize.
     * @param tcpBufferSize New value of property tcpBufferSize.
     *
     */
    public void setTcpBufferSize(int tcpBufferSize)
        throws ClientException
    {
        if(tcpBufferSize > 0)
            {
                _tcpBufferSize = tcpBufferSize;
                _client.setLocalTCPBufferSize(tcpBufferSize);
            }

    }

    /** Getter for property bufferSize.
     * @return Value of property bufferSize.
     *
     */
    public int getBufferSize() {
        return _bufferSize;
    }

    /** Setter for property bufferSize.
     * @param bufferSize New value of property bufferSize.
     *
     */
    public void setBufferSize(int bufferSize) {
        _bufferSize = bufferSize;
    }

    /** Setter to support checksum type negotiation
     * @param types List of checksum type names which will be tried by the checksum negotiation algo
     *
     */
    public static void setSupportedChecksumTypes(String[] types){

         cksmTypeList = new ArrayList();
         for(String algorithm: types){
             cksmTypeList.add(algorithm.toUpperCase());
         }
    }

    public static final void main( String[] args ) throws Exception {
        if(args.length <5 || args.length > 11) {
            System.err.println(
                               "usage:\n" +
                               "       gridftpcopy <source gridftp/file url> <dest gridftp/file url>  \n"+
                               "                     <memoryBufferSize> <tcpBufferSize> <parallel streams>\n"+
                               "                     <use emode(true or false)> [ <send checksum (true or false)>] [ <server-mode(active or passive)> ] \n"+
                               "                     [--checksumType=<value>] [--checksumValue=<value>] [--checksumPrint=true|false]\n" +
                               "  example:" +
                               "       gridftpcopy gsiftp://host1:2811//file1 file://localhost//tmp/file1 4194304 4194304 11 true false");
            System.exit(1);
            return;
        }
        String source = args[0];
        String dest   = args[1];
        int bs = Integer.parseInt(args[2]);
        int tcp_bs = Integer.parseInt(args[3]);
        int streams = Integer.parseInt(args[4]);
        boolean emode=true;
	String server_mode="active";

        if(args.length > 5) {
            if(args[5].equals("true")){
                emode=true;
            }else{
                emode=false;
            }
        }
	//if emode is false, then it means stream mode and server could be in active or passive mode
	if((emode == false ) && (args.length >= 8)){
            server_mode = args[7];
        }
        boolean send_checksum = true;
        if(args.length > 6) {
            send_checksum = args[6].equalsIgnoreCase("true");
        }

        OptionMap<String> sMap = new OptionMap<>(new OptionMap.StringFactory(),args);

        String chsmType  = sMap.get("checksumType");
        String chsmValue = sMap.get("checksumValue");
        String cksmPrint = sMap.get("checksumPrint");

        GlobusURL src_url = new GlobusURL(source);
        GlobusURL dst_url = new GlobusURL(dest);
        GSSCredential credential = null;

        if( ( src_url.getProtocol().equals("gsiftp") ||
              src_url.getProtocol().equals("gridftp") ) &&
            dst_url.getProtocol().equals("file")) {
            GridftpClient client;

            client = new GridftpClient(src_url.getHost(),
                                       src_url.getPort(), tcp_bs, bs,credential);
            client.setStreamsNum(streams);
            client.setChecksum(chsmType,chsmValue);
            try {
                client.gridFTPRead(src_url.getPath(),dst_url.getPath(), emode,
                                   server_mode.equalsIgnoreCase("passive"));
            }
            finally {
                client.close();
            }
            return;
        }

        if(  src_url.getProtocol().equals("file") &&
             ( dst_url.getProtocol().equals("gsiftp") ||
               dst_url.getProtocol().equals("gridftp") )
             ) {
            GridftpClient client;
            client = new GridftpClient(dst_url.getHost(),
                                       dst_url.getPort(), tcp_bs, bs,credential);
            client.setStreamsNum(streams);
            try {
                client.setChecksum(chsmType,chsmValue);
                client.gridFTPWrite(src_url.getPath(),dst_url.getPath(), emode, send_checksum,
                                    server_mode.equalsIgnoreCase("passive"));
            }
            finally {
                client.close();
            }
            return;
        }
        System.err.println("only \"file to gridftp\" and \"gridftp to file\" transfers are supported");
        System.exit(1);
    }

    private  class TransferThread implements Runnable {
        private boolean _done;
        private final boolean _emode;
        private final boolean _passive_server_mode;
        private final GridFTPClient _client;
        private Exception _throwable;
        private final String _path;
        private final IDiskDataSourceSink _source_sink;
        private final boolean _read;
        private long _size;
        private Thread _runner;

        public TransferThread(GridFTPClient client,
                              String path,
                              IDiskDataSourceSink source_sink,
                              boolean emode,
                              boolean passive_server_mode,
                              boolean read,
                              long size) {
            _client = client;
            _path = path;
            _source_sink = source_sink;
            _emode = emode;
            _passive_server_mode = passive_server_mode;
            _read = read;
            _size = size;
        }

        public void start()
        {
            _runner = new Thread(this);
            _runner.start();
        }
        /**
         * @param  FirstByteTimeout timeout before first byte
         *         arrives/leaves in milliseconds
         * @param NextByteTimeout timeout before next bytes arrive/leave
         */
        public void waitCompletion(long FirstByteTimeout,
                                   long NextByteTimeout)
            throws InterruptedException, ClientException, ServerException,
                   IOException
        {
            long timeout = FirstByteTimeout;

            logger.debug("waiting for completion of transfer");
            boolean timedout = false;
            boolean interrupted = false;
            while(true ) {
                try {
                    waitCompleteion(timeout);
                    if(isDone()) {
                        break;
                    }
                    if( (System.currentTimeMillis() - _source_sink.getLast_transfer_time())
                        > timeout) {
                        timedout = true;
                        break;
                    }
                    timeout= NextByteTimeout;
                }
                catch(InterruptedException ie) {
                    _runner.interrupt();
                    interrupted = true;
                    break;
                }
            }

            if(timedout ||interrupted ) {
                _runner.interrupt();
                String error = "transfer timedout or interrupted";
                logger.error(error);
                throw new InterruptedException(error);
            }

            Exception e = getThrowable();
            if (e != null) {
                logger.error(" transfer exception",e);
                if (e instanceof ClientException) {
                    throw (ClientException)e;
                } else if (e instanceof ServerException) {
                    throw (ServerException)e;
                } else if (e instanceof IOException) {
                    throw (IOException)e;
                } else {
                    throw new RuntimeException("Unexpected exception", e);
                }
            }
        }

        private void waitCompleteion() throws InterruptedException {
            while(true) {
                synchronized(this) {
                    wait(1000);
                    if(_done) {
                        return;
                    }
                }
            }
        }

        private void waitCompleteion(long timeout) throws InterruptedException {
            synchronized(this) {
                wait(timeout);
            }
        }

        public  synchronized void done() {
            _done = true;
            notifyAll();
        }

        @Override
        public void run() {
            try {
                if(_read) {
                    logger.debug("starting a transfer from "+_path);
                    if(_client.isFeatureSupported("GETPUT")) {
                        _client.get2(_path, (_emode ? false: _passive_server_mode),
                                    _source_sink, null);
                    } else {
                        _client.get(_path,_source_sink,null);
                    }
                }
                else {
                    logger.debug("starting a transfer to "+_path);
                    if(_client.isFeatureSupported("GETPUT")) {
                        _client.put2(_path, (_emode ? true : _passive_server_mode),
                                    _source_sink, null);
                    } else {
                        _client.put(_path,_source_sink,null);
                    }
                }
            } catch (IOException | ClientException | ServerException e) {
                logger.error(e.toString());
                _throwable = e;
            } finally {
                done();
            }
        }

        /** Getter for property done.
         * @return Value of property done.
         *
         */
        public synchronized boolean isDone() {
            return _done;
        }


        public Exception getThrowable() {
            return _throwable;
        }

    }

    public static class Checksum {
         public Checksum(String type,String value){ this.type = type; this.value = value; }
         public String type;
         public String value;
    }

    public static class ChecksumNotSupported extends Exception {
          private static final long serialVersionUID = -8698077375537138426L;

          public ChecksumNotSupported(String msg,int code){ super(msg); this.code = code; }
          public int getCode(){ return code; }
          private int code;
    }

   public static class ChecksumValueFormatException extends Exception {
          private static final long serialVersionUID = -8714275697272157959L;

          public ChecksumValueFormatException(String msg){
              super(msg);
          }
    }

    public interface IDiskDataSourceSink extends  DataSink ,DataSource {
        /**
         * file postions should be reset to 0 if IDiskDataSourceSink is a wrapper
         * around random access disk file
         */
        public long getAdler32() throws IOException;
        public String getCksmValue(String type)
            throws IOException,NoSuchAlgorithmException;
        public long getLast_transfer_time();
        public long getTransfered();
        public long length() throws IOException;
    }

    private  class DiskDataSourceSink implements IDiskDataSourceSink {
        private final FileChannel _fileChannel;
        private final int _buf_size;
        private volatile long _last_transfer_time = System.currentTimeMillis();
        private long _transferred;
        private final boolean _source;

        public DiskDataSourceSink(FileChannel fileChannel, int buf_size,boolean source) {
            _fileChannel = fileChannel;
            _buf_size = buf_size;
            _source = source;
        }

        @Override
        public synchronized void write(Buffer buffer)
            throws IOException {
            if(_source) {
                String error = "DiskDataSourceSink is source and write is called";
                logger.error(error);
                throw new IllegalStateException(error);
            }

            _last_transfer_time    = System.currentTimeMillis() ;
            int read = buffer.getLength();
            long offset = buffer.getOffset();
            if (offset >= 0) {
                _fileChannel.position(offset);
            }
            ByteBuffer bb = ByteBuffer.wrap(buffer.getBuffer(), 0, buffer.getLength());
            _fileChannel.write(bb);
            _transferred +=read;
        }

        @Override
        public void close()
            throws IOException {
            logger.debug("DiskDataSink.close() called");
            _last_transfer_time    = System.currentTimeMillis() ;
        }

        /** Specified in org.globus.ftp.DataSource. */
        @Override
        public long totalSize() throws IOException
        {
            return _source ? _fileChannel.size() : -1;
        }

        /** Getter for property last_transfer_time.
         * @return Value of property last_transfer_time.
         *
         */
        @Override
        public long getLast_transfer_time() {
            return _last_transfer_time;
        }

        /** Getter for property transfered.
         * @return Value of property transfered.
         *
         */
        @Override
        public synchronized long getTransfered() {
            return _transferred;
        }

        @Override
        public synchronized Buffer read() throws IOException {
            if(!_source) {
                String error = "DiskDataSourceSink is sink and read is called";
                logger.error(error);
                throw new IllegalStateException(error);
            }

            _last_transfer_time    = System.currentTimeMillis() ;
            byte[] bytes = new byte[_buf_size];
            ByteBuffer bb = ByteBuffer.wrap(bytes);

            int read = _fileChannel.read(bb);
            if(read == -1) {
                return null;
            }
            Buffer buffer = new Buffer(bytes,read,_transferred);
            _transferred  += read;
            return buffer;
        }

        @Override
        public long getAdler32() throws IOException{
            _fileChannel.position(0);
            long adler32 = GridftpClient.getAdler32(_fileChannel);
            _fileChannel.position(0);
            return adler32;
        }

        @Override
        public String getCksmValue(String type)
            throws IOException,NoSuchAlgorithmException
        {
            _fileChannel.position(0);
            String v = GridftpClient.getCksmValue(_fileChannel,type);
            _fileChannel.position(0);
            return v;
        }

        @Override
        public long length() throws IOException{
            return _fileChannel.size();
        }

    }

    public static String long32bitToHexString(long value){
        value |=0x100000000L;
        value &=0x1ffffffffL;
        String svalue = Long.toHexString(value);
        svalue = svalue.substring(1);
        if(svalue.length() != 8) {
            throw new IllegalStateException("32 bit integer hext string  length is not 8 bytes");
        }
        return svalue;
    }
    public static String printbytes(byte[] bs)
    {
        StringBuilder sb= new StringBuilder();
        for (byte b : bs) {
            byteToHexString(b, sb);
        }
        return sb.toString();
    }

    private static final char [] __map =
    { '0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f' } ;

    private static void byteToHexString( byte b, StringBuilder sb ) {

        int x = ( b < 0 ) ? ( 256 + (int)b ) : (int)b ;
        sb.append(__map[ ((int)b >> 4 ) & 0xf ]);
        sb.append(__map[ ((int)b      ) & 0xf ]);
    }


}
