// $Id: GridftpClient.java,v 1.13 2007-10-23 07:38:45 behrmann Exp $
// $Log: not supported by cvs2svn $
// Revision 1.12  2007/10/08 20:45:51  abaranov
//
//
// Client side changes to enable verification of the server and client side checksums
//
// Revision 1.11  2006/01/26 04:40:59  timur
// updated client to use no data authentication in server passive mode
//
// Revision 1.10  2005/12/13 22:24:07  timur
// use boolean instead of a string for passive_mode option
//
// Revision 1.9  2005/12/01 00:08:25  timur
// make old signature gridftp writes avail in GridftClient.java
//
// Revision 1.8  2005/11/23 22:25:22  neha
// gridftp copy in stream mode support added
//
// Revision 1.7  2005/11/08 00:55:50  timur
// read vs write log
//
// Revision 1.6  2005/05/24 20:32:01  timur
// fixed a checksum to string coversion bug
//
// Revision 1.5  2005/05/12 22:18:02  timur
// couple new clients, correct string representation of adler32
//
// Revision 1.4  2005/04/27 19:55:06  timur
// added gridftp list
//
// Revision 1.3  2005/04/14 18:29:03  timur
// accept buffer size ony if it is positive
//
// Revision 1.2  2005/04/12 22:02:56  timur
// one more form of store / restore functions, taking sourceSink interface as arguments
//
// Revision 1.1  2005/01/14 23:07:16  timur
// moving general srm code in a separate repository
//
// Revision 1.7  2004/11/19 23:17:52  timur
// added the num of streams options
//
// Revision 1.6  2004/11/19 23:14:13  timur
// added the tcp buffer size and buffer size command line options
//
// Revision 1.5  2004/10/30 04:19:08  timur
// Fixed a problem related to the restoration of the job from database
//
// Revision 1.4  2004/08/06 19:35:27  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.3.2.2  2004/07/29 22:17:30  timur
// Some functionality for disk srm is working
//
// Revision 1.3.2.1  2004/06/30 20:37:25  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.3  2004/02/06 23:10:51  timur
// a lot of changes, added checksum retreval for getFileMetaData, more correct work with srm_root, better error handling in 3rd party client
//
// Revision 1.2  2003/12/16 00:05:33  cvs
// addded synchronization to getTransfered() and read() funtions
//
// Revision 1.1  2003/12/04 20:53:38  cvs
// timur: add functionality to GridftClient, move it to util package, rewrite movers/RemoteGsiftpTransferProtocol_1.java to use this client
//
// Revision 1.2  2003/11/09 19:51:29  cvs
// first alfa version of srm v2  space reservation functions is complete
//
package org.dcache.srm.util;

import java.io.* ;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.TrustedCertificates;

import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;

import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.auth.Authorization;

import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSCredential;

import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;

import org.globus.ftp.GridFTPClient;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.Session;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.RetrieveOptions;
import org.globus.ftp.DataSource;
import org.globus.ftp.DataSink;
import org.globus.ftp.Buffer;
import org.globus.ftp.FileRandomIO;
import org.globus.ftp.extended.GridFTPControlChannel;
import org.globus.ftp.HostPort;

import org.globus.util.GlobusURL;

import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.SSLUtil;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.GlobusCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSName;

import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.globus.gsi.gssapi.net.GssSocket;

import java.io.OutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.net.ServerSocket;

import org.dcache.srm.Logger;
/**
 * THE CLASS IS NOT THREAD SAVE
 * DO ONLY ONE OPERATION (READ / WRITE) AT A TIME 
 */

public class GridftpClient {
    
    private Logger logger;
    
    public  void say(String s) {
        if(logger != null)
        {
            logger.log("GridftpClient: "+s);
        }
    }
    public void esay(String s) {
        if(logger != null)
        {
            logger.elog("GridftpClient: "+s);
        }
    }
    public  void esay(Throwable t) {
        if(logger != null)
        {
            logger.elog(t);
        }

    }
    private boolean use_chksum;
    
    private final FnalGridFTPClient client;
    private final String host;
    private String cksmType;
    private String cksmValue;

    public GridftpClient(String host, int port, 
        int tcpBufferSize,
        GSSCredential cred,
        Logger logger) 
    throws Exception {
        this(host,port,tcpBufferSize,0,cred,logger);
    }
    
    public GridftpClient(String host, int port, 
        int tcpBufferSize,
        int bufferSize,
        GSSCredential cred,
        Logger logger) throws Exception {
            
            this.logger = logger;
            if(bufferSize >0) {
                this.bufferSize = bufferSize;
                say("memory buffer size is set to "+bufferSize);
            }
            if(tcpBufferSize > 0)
            {
                this.tcpBufferSize = tcpBufferSize;
                say("tcp buffer size is set to "+tcpBufferSize);
            }
             if(cred == null) {
                GlobusCredential gcred = GlobusCredential.getDefaultCredential();
                cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
            }
            this.host = host;
            say("connecting to "+host+" on port "+port);

            client  = new FnalGridFTPClient(host, port);
            client.setLocalTCPBufferSize(this.tcpBufferSize);
            say("gridFTPClient tcp buffer size is set to "+tcpBufferSize);
            client.authenticate(cred); /* use redentials */
            client.setType(GridFTPSession.TYPE_IMAGE);
           
            
    }
    
    private int streamsNum=10;
    private int tcpBufferSize=1024*1024;
    private int bufferSize=1024*1024;
    private int FirstByteTimeout=60*60; //one hour
    private int NextByteTimeout=60*10; //10 minutes
    
    private volatile IDiskDataSourceSink current_source_sink;
    private long last_transfer_time = System.currentTimeMillis();
    public long getLastTransferTime()
    {
        //do local copy to avoid null pointer exception
        IDiskDataSourceSink source_sink = current_source_sink;
        if(source_sink != null)
        {
            last_transfer_time = source_sink.getLast_transfer_time();
        }
        return last_transfer_time;
    }
    
    private long transfered = 0;
    public long getTransfered()
    {
        //do local copy to avoid null pointer exception
        IDiskDataSourceSink source_sink = current_source_sink;
        if(source_sink != null)
        {
            transfered = source_sink.getTransfered();
        }
        return transfered;
    }
    
    public static long getAdler32(java.io.RandomAccessFile diskFile) throws java.io.IOException {
        java.util.zip.Adler32 java_addler = null;
        
        java_addler = new java.util.zip.Adler32();
        diskFile.seek(0);
        byte [] buffer = new byte[1024] ;
        long sum=0L;
        while(true){
            int rc = diskFile.read( buffer , 0 , buffer.length ) ;
            
            if( rc <=0 )break ;
            sum += rc ;
            java_addler.update(buffer , 0 , rc ) ;
        }
        return java_addler.getValue();
    }

    public static String getCksmValue(java.io.RandomAccessFile diskFile,String type) throws java.io.IOException,java.security.NoSuchAlgorithmException {

        if ( type.equals("adler32") )
           return long32bitToHexString(getAdler32(diskFile));

        java.security.MessageDigest md = java.security.MessageDigest.getInstance(type);
        diskFile.seek(0);
        byte [] buffer = new byte[1024] ;
        long sum=0L;
        while(true){
            int rc = diskFile.read( buffer , 0 , buffer.length ) ;

            if( rc <=0 )break ;
            sum += rc ;
            md.update(buffer , 0 , rc ) ;
        }
        return printbytes(md.digest());
    }
    
    
    public String list(String directory,boolean serverPassive) throws Exception {
        setCommonOptions(false,serverPassive);
        client.changeDir(directory);
        if(serverPassive) {
            client.setPassive();
            client.setLocalActive();
        }
        else {
            client.setLocalPassive();
            client.setActive();
        }
        final ByteArrayOutputStream received = new ByteArrayOutputStream(1000);

        // unnamed DataSink subclass will write data channel content
        // to "received" stream.

        DataSink sink = new DataSink() {
                public void write(Buffer buffer) throws IOException {
                        received.write(buffer.getBuffer(), 0, buffer.getLength());
                }
                public void close() throws IOException {
                };
        };

        client.list(" "," ",sink);
        return received.toString();
    }
    public long getSize(String ftppath) throws Exception
    {
        return client.getSize(ftppath);
    }
    
    private void setCommonOptions(boolean emode,
        boolean passive_server_mode ) throws Exception
    {
        if (client.isFeatureSupported("DCAU")) {
            client.setDataChannelAuthentication(DataChannelAuthentication.NONE);
        }
        say("set local data channel authentication mode to None");
        client.setLocalNoDataChannelAuthentication();
        
        if(emode) {
            client.setMode(GridFTPSession.MODE_EBLOCK);
            // adding parallelism
            say("parallelism: " + streamsNum);
            client.setOptions(new RetrieveOptions(streamsNum));
        }
        else {
            client.setMode(GridFTPSession.MODE_STREAM);
            say("stream mode transfer");

            if (!client.isFeatureSupported("GETPUT")) {
                if(passive_server_mode){
                    say("server is passive");
                    HostPort serverHostPort = client.setPassive();
                    say("serverHostPort="+serverHostPort.getHost()+":"+serverHostPort.getPort());
                    client.setLocalActive();
                }else{
                    say("server is active");
                    client.setLocalPassive();
                    client.setActive();
                }
            }
        }
        
        //wait for ~ 24 days before timing out, poll every minute
        client.setClientWaitParams(Integer.MAX_VALUE,1000);

    }
    
    
        /**
         * THIS IS A TEMPORARY CODE TO MAKE NCSA GSIFTP SERVER
         * TO STAGE FILES INSTEAD OF FAILING TRANSFERS
         * THIS REALLY IS @##@%$^
         */
    private void sendNCSAWaitCommand() throws Exception
    {
        say(" sending wait command to ncsa host " + host);
        GridFTPControlChannel channel = client.getControlChannel();
            org.globus.ftp.vanilla.Reply reply =
            channel.execute(new org.globus.ftp.vanilla.Command("SITE","WAIT"));
            say("Reply is "+reply);
            if(org.globus.ftp.vanilla.Reply.isPositiveCompletion( reply)) {
                say("sending wait command successful");
            } else {
                esay("WARNING: sending wait command failed");
            }
    }
    
    // setChecksum will set cksmType and cksmValue for the FTP session
    // If both values are set the write and read are verified using supplied information
    // If only the type is set, value is calculated from the file's copy on the disk
    // If send_checksum is used in the write methods,  type and value will be set
    // to adler32 and dynamically calcualted upon completion of the transfer by the client side
    public void setChecksum(String cksmType,String cksmValue){
       this.cksmType = cksmType;
       this.cksmValue = cksmValue;
    }

    public String getChecksumValue() {
       return cksmValue;
    }

    public String getChecksumType() {
       return cksmType;
    }

    public void gridFTPRead(    String sourcepath,
    String destinationfilepath,
    boolean emode,
    boolean passive_server_mode) throws Exception {
        java.io.RandomAccessFile diskFile =null;
        try
        {
            diskFile =
            new java.io.RandomAccessFile(destinationfilepath,"rw");
            gridFTPRead(sourcepath,diskFile, emode,passive_server_mode);
        }
        finally
        {
            try
            {
                if(diskFile != null)
                {
                    diskFile.close();
                }
            }
            catch(IOException ioe) {
                esay(" closing of file "+destinationfilepath+" failed");
                esay(ioe);
            }
        }
        
        
    }
    
    public void gridFTPRead(String sourcepath,
        java.io.RandomAccessFile destinationDiskFile,
        boolean emode,boolean passive_server_mode) throws Exception
    {
        
        DiskDataSourceSink sink = new DiskDataSourceSink(
        destinationDiskFile,bufferSize,false);
        gridFTPRead(sourcepath,sink, emode,passive_server_mode);
    }
    
    public void gridFTPRead(String sourcepath,
        IDiskDataSourceSink sink,
        boolean emode) throws Exception
    {
       gridFTPRead(sourcepath,sink,emode,true);
    }

    public void gridFTPRead(String sourcepath,
        IDiskDataSourceSink sink,
        boolean emode,
	boolean passive_server_mode) throws Exception
    {
        say("gridFTPRead started");
        // size of the file
        setCommonOptions(emode,passive_server_mode);
        if(host.toLowerCase().indexOf("ncsa") != -1) {
            sendNCSAWaitCommand();
        }
        
        int read = 0;
        final long size = client.getSize(sourcepath);
        current_source_sink = sink;
        TransferThread getter = new TransferThread(client,sourcepath,sink,emode,passive_server_mode,true,size);
        getter.start();
        getter.waitCompletion(FirstByteTimeout, NextByteTimeout);
        
        if(size - sink.getTransfered() >0) {
            esay("we wrote less then file size!!!");
            throw new java.io.IOException("we wrote less then file size!!!");
        }
        else if(size - sink.getTransfered() <0) {
            esay("we wrote more then file size!!!");
            throw new java.io.IOException("we wrote more then file size!!!");
        }
        
        say("gridFTPWrite() wrote "+sink.getTransfered()+"bytes");

        if ( this.cksmType != null )
             verifyCksmValue(current_source_sink,sourcepath);

        //make these remeber last values
        getTransfered();
        getLastTransferTime();
        current_source_sink = null;
    }

    public void gridFTPWrite(
    String sourcefilepath,
    String destinationpath,
    boolean emode,
    boolean use_chksum) throws Exception {
        gridFTPWrite( sourcefilepath,destinationpath,emode,use_chksum,true);
   }

    public void gridFTPWrite(
    String sourcefilepath,
    String destinationpath,
    boolean emode,
    boolean use_chksum,
    boolean passive_server_mode ) throws Exception {
        java.io.RandomAccessFile diskFile = null;
        try {
            diskFile = new java.io.RandomAccessFile(sourcefilepath,"r");
            gridFTPWrite(diskFile, destinationpath, emode, use_chksum,passive_server_mode);
        }
        finally
        {
            try
            {
                if(diskFile != null)
                {
                    diskFile.close();
                }
            }
            catch(IOException ioe) {
                esay(" closing of file "+sourcefilepath+" failed");
                esay(ioe);
            }
        }

    }    
    public void gridFTPWrite(
    java.io.RandomAccessFile sourceDiskFile,
    String destinationpath,
    boolean emode,
    boolean use_chksum) throws Exception {
         gridFTPWrite(
             sourceDiskFile,
             destinationpath,
             emode,
             use_chksum,
             true);
    }
    
    public void gridFTPWrite(
    java.io.RandomAccessFile sourceDiskFile,
    String destinationpath,
    boolean emode,
    boolean use_chksum,
    boolean passive_server_mode ) throws Exception {
        
        say("gridFTPWrite started, source file is "+sourceDiskFile+ 
        " destination path is "+destinationpath);
        
        
        sourceDiskFile.seek(0);
        DiskDataSourceSink source = new DiskDataSourceSink(
        sourceDiskFile,bufferSize,true);
        gridFTPWrite( source, destinationpath, emode, use_chksum,passive_server_mode);
        
    }
    
    public void gridFTPWrite(
    IDiskDataSourceSink source,
    String destinationpath,
    boolean emode,
    boolean use_chksum) throws Exception {
         gridFTPWrite(
             source,
             destinationpath,
             emode,
             use_chksum,
             true);
        
    }
    public void gridFTPWrite(
    IDiskDataSourceSink source,
    String destinationpath,
    boolean emode,
    boolean use_chksum,
    boolean passive_server_mode) throws Exception {
        
        say("gridFTPWrite started, destination path is "+destinationpath);
        
        setCommonOptions(emode,passive_server_mode);
       
        if(use_chksum || cksmType != null) {

            sendCksmValue(source);
            /*
            try {
                   sendAddler32Checksum(source.getAdler32());
            }
            catch(Exception e) {
                say("could not set addler 32 "+e.toString());
            }
            */
        }
        
        current_source_sink = source;
        long diskFileLength = source.length();
        TransferThread putter = new TransferThread(client,destinationpath,source,emode,passive_server_mode,false,diskFileLength);
        putter.start();
        putter.waitCompletion(FirstByteTimeout, NextByteTimeout);
        
        if(diskFileLength > source.getTransfered() ) {
            esay("we read less then file size!!!");
            throw new java.io.IOException("we read less then file size!!!");
        }
        else if(diskFileLength < source.getTransfered() ) {
            esay("we read more then file size!!!");
            throw new java.io.IOException("we read more then file size!!!");
        }
        
        say("gridFTPWrite() wrote "+source.getTransfered()+"bytes");
        getTransfered();
        getLastTransferTime();
        current_source_sink = null;
    }

    private void sendCksmValue(IDiskDataSourceSink source) throws Exception
    {
        if ( cksmType == null )
            cksmType = "adler32";

        if ( cksmValue == null )
           cksmValue = source.getCksmValue(cksmType);

        client.sendCksmValue(cksmType,cksmValue);
        // send gridftp message
    }

   private void verifyCksmValue(IDiskDataSourceSink source,String remotePath) throws Exception
    {
        if ( cksmType == null )
             throw new IllegalArgumentException("verifyCksmValue: expected cksm type");

        if ( cksmValue == null )
            cksmValue = source.getCksmValue(cksmType);

        String serverCksmValue = client.getCksmValue(cksmType,remotePath);

        if ( !cksmValue.equals(serverCksmValue) )
             throw new java.io.IOException("Server side checksum:"+serverCksmValue+" does not match client side checksum:"+cksmValue);
       // send gridftp message
    }


    private boolean closed =false;
    public  void close() throws IOException, org.globus.ftp.exception.ServerException {
        synchronized(this)
        {
            if(closed) {
                return;
            }
            else
            {
                closed = true;
            }
        }
        say("closing client : "+client);
        client.close(true);
        say("closed client");
        
    }
    
    public void finalize() {
        try {
            close();
        }
        catch(Exception e) {
        }
    }
    /** Getter for property streamsNum.
     * @return Value of property streamsNum.
     *
     */
    public int getStreamsNum() {
        return streamsNum;
    }
    
    /** Setter for property streamsNum.
     * @param streamsNum New value of property streamsNum.
     *
     */
    public void setStreamsNum(int streamsNum) {
        this.streamsNum = streamsNum;
    }
    
    /** Getter for property tcpBufferSize.
     * @return Value of property tcpBufferSize.
     *
     */
    public int getTcpBufferSize() {
        return tcpBufferSize;
    }
    
    /** Setter for property tcpBufferSize.
     * @param tcpBufferSize New value of property tcpBufferSize.
     *
     */
    public void setTcpBufferSize(int tcpBufferSize) throws org.globus.ftp.exception.ClientException{
        if(tcpBufferSize > 0)
        {
            this.tcpBufferSize = tcpBufferSize;
            client.setLocalTCPBufferSize(tcpBufferSize);
        }
        
    }
    
    /** Getter for property bufferSize.
     * @return Value of property bufferSize.
     *
     */
    public int getBufferSize() {
        return bufferSize;
    }
    
    /** Setter for property bufferSize.
     * @param bufferSize New value of property bufferSize.
     *
     */
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
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

        OptionMap<String> sMap = new OptionMap<String>(new OptionMap.StringFactory(),args);

        String chsmType  = sMap.get("checksumType");
        String chsmValue = sMap.get("checksumValue");
        String cksmPrint = sMap.get("checksumPrint");

        GlobusURL src_url = new GlobusURL(source);
        GlobusURL dst_url = new GlobusURL(dest);
        GSSCredential credential = null;
        
             Logger logger =   new org.dcache.srm.Logger()
            {
                public synchronized void log(String s)
                {
                    System.out.println(new java.util.Date().toString()+": "+ s);
                }
                public synchronized void elog(String s)
                {
                    System.err.println(new java.util.Date().toString()+": "+ s);
                }
                public synchronized void elog(Throwable t)
                {
                    t.printStackTrace();
                }
            };

        if( ( src_url.getProtocol().equals("gsiftp") ||
        src_url.getProtocol().equals("gridftp") ) &&
        dst_url.getProtocol().equals("file")) {
            GridftpClient client;

            client = new GridftpClient(src_url.getHost(),
            src_url.getPort(), tcp_bs, bs,credential,logger);
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
            dst_url.getPort(), tcp_bs, bs,credential,logger);
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
        private boolean done =false;
        private boolean emode;
        private boolean passive_server_mode;
        private FnalGridFTPClient client;
        private Exception throwable = null;
        private String path;
        private IDiskDataSourceSink source_sink;
        private boolean read;
        private long size;
        public TransferThread(FnalGridFTPClient client,
        String path,
        IDiskDataSourceSink source_sink,
        boolean emode,
        boolean passive_server_mode,
        boolean read,
        long size) {
            this.client = client;
            this.path = path;
            this.source_sink = source_sink;
            this.emode = emode;
            this.passive_server_mode = passive_server_mode;
            this.read = read;
            this.size = size;
        }
        
        private Thread runner;
        public void start()
        {
            runner = new Thread(this);
            runner.start();
        }
        /**
         * @param  FirstByteTimeout timeout before first byte 
         *         arrives/leaves in seconds
         * @param NextByteTimeout timeout before next bytes arrive/leave
         */
        public void waitCompletion(int FirstByteTimeout,int  NextByteTimeout)
         throws Exception
        {
            long timeout = FirstByteTimeout*1000;

            say("waiting for completion of transfer");
            boolean timedout = false;
            boolean interrupted = false;
            while(true ) {
                try {
                       waitCompleteion(timeout);
                    if(isDone()) {
                        break;
                    }
                    if( (System.currentTimeMillis() - source_sink.getLast_transfer_time())
                    > timeout) {
                        timedout = true;
                        break;
                    }
                    timeout= NextByteTimeout*1000;
                }
                catch(InterruptedException ie) {
                    runner.interrupt();
                    interrupted = true;
                    break;
                }
            }

            if(timedout ||interrupted ) {
                runner.interrupt();
                String error = "transfer timedout or interrupted";
                esay(error);
                throw new InterruptedException(error);
            }

            if(getThrowable() !=null) {
                esay(" transfer exception");
                esay(getThrowable());
                throw getThrowable();
            }

        }
        
        private void waitCompleteion() throws InterruptedException {
            while(true) {
                synchronized(this) {
                    wait(1000);
                    if(done) {
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
            done = true;
            notifyAll();
        }
        
        public void run() {
            try {
                if(read) {
                    say("starting a transfer from "+path);
                    if(client.isFeatureSupported("GETPUT")) {
                        client.get2(path, (emode ? false: passive_server_mode),
                                    source_sink, null);
                    } else {
                        client.get(path,source_sink,null);
                    }
                }
                else {
                    say("starting a transfer to "+path);
                    if(client.isFeatureSupported("GETPUT")) {
                        client.put2(path, (emode ? true : passive_server_mode),
                                    source_sink, null);
                    } else {
                        client.put(path,source_sink,null);
                    }
                }
                done();
            }
            catch(Exception e) {
                esay(e);
                throwable = e;
                done();
            }
            catch(Throwable t) {
                esay(t);
                throwable = new IllegalStateException(t.toString());
                done();
            }
        }
        
        /** Getter for property done.
         * @return Value of property done.
         *
         */
        public synchronized boolean isDone() {
            return done;
        }
        
        
        public Exception getThrowable() {
            return throwable;
        }
        
    }
    
    public interface IDiskDataSourceSink extends  DataSink ,DataSource {
         /** 
          * file postions should be reset to 0 if IDiskDataSourceSink is a wrapper
          * around random access disk file
          */ 
         public long getAdler32() throws IOException;
         public String getCksmValue(String type) throws IOException,java.security.NoSuchAlgorithmException;
         public long getLast_transfer_time();
         public long getTransfered();
         public long length() throws IOException;
    }

        private  class DiskDataSourceSink implements IDiskDataSourceSink {
            private final java.io.RandomAccessFile diskFile;
            private final int buf_size;
            private volatile long last_transfer_time = System.currentTimeMillis();
            private long transfered = 0;
            private boolean source;

            public DiskDataSourceSink(java.io.RandomAccessFile diskFile, int buf_size,boolean source) {
                this.diskFile = diskFile;
                this.buf_size = buf_size;
                this.source = source;
            }

            public synchronized void write(Buffer buffer)
            throws IOException {
                if(source) {
                    String error = "DiskDataSourceSink is source and write is called";
                    esay(error);
                    throw new IllegalStateException(error);
                }
                //say("DiskDataSourceSink.write()");

                last_transfer_time    = System.currentTimeMillis() ;
                int read = buffer.getLength();
                long offset = buffer.getOffset();
                if (offset >= 0) {
                    diskFile.seek(offset);
                }
                diskFile.write(buffer.getBuffer(), 0, read);
                transfered +=read;
            }

            public void close()
            throws IOException {
                say("DiskDataSink.close() called");
                last_transfer_time    = System.currentTimeMillis() ;
            }

            /** Getter for property last_transfer_time.
             * @return Value of property last_transfer_time.
             *
             */
            public long getLast_transfer_time() {
                return last_transfer_time;
            }

            /** Getter for property transfered.
             * @return Value of property transfered.
             *
             */
            public synchronized long getTransfered() {
                return transfered;
            }

            public synchronized Buffer read() throws IOException {
                if(!source) {
                    String error = "DiskDataSourceSink is sink and read is called";
                    esay(error);
                    throw new IllegalStateException(error);
                }
                //say("DiskDataSourceSink.read()");

                last_transfer_time    = System.currentTimeMillis() ;
                byte[] bytes = new byte[buf_size];

                int read = diskFile.read(bytes);
                //say("DiskDataSourceSink.read() read "+read+" bytes");
                if(read == -1) {
                    return null;
                }
                Buffer buffer = new Buffer(bytes,read,transfered);
                transfered  += read;
                return buffer;
            }

            public long getAdler32() throws IOException{
                long adler32 = GridftpClient.getAdler32(diskFile);
                say("adler 32 for file "+diskFile+" is "+adler32);
                diskFile.seek(0);
                return adler32;
            }

            public String getCksmValue(String type) throws IOException,java.security.NoSuchAlgorithmException {
                String v = GridftpClient.getCksmValue(diskFile,type);
                say(type+" for file "+diskFile+" is "+v);
                diskFile.seek(0);
                return v;
            }

            public long length() throws IOException{
                return diskFile.length();
            }

        }

        private static class FnalGridFTPClient extends org.globus.ftp.GridFTPClient {
        public FnalGridFTPClient(String host, int port)  throws IOException,org.globus.ftp.exception.ServerException {
            super(host,port);
        }

        public GridFTPControlChannel getControlChannel() {
            return (GridFTPControlChannel)controlChannel;
        }

        private void sendAddler32Checksum(String adler32String) throws Exception
        {
            org.globus.ftp.vanilla.Reply reply =
            quote("SITE CHKSUM "+ adler32String);
            if(org.globus.ftp.vanilla.Reply.isPositiveCompletion( reply)) {
            } else {
                throw new java.io.IOException(reply.getMessage());
            }

       }

        public void sendCksmValue(String type,String value) throws Exception {


            try {
                 org.globus.ftp.vanilla.Reply reply = quote("SCKS "+type+" "+value);

                 if ( !org.globus.ftp.vanilla.Reply.isPositiveCompletion(reply) ){
                      if ( type.toLowerCase().equals("adler32") ){
                          sendAddler32Checksum(value);
                          return;
                      }
                      throw new java.io.IOException(reply.getMessage());
                 }
            } catch ( org.globus.ftp.exception.ServerException ex ){
                      if ( type.toLowerCase().equals("adler32") ){
                          sendAddler32Checksum(value);
                          return;
                      }
              throw ex;
            }
        }

        public String getCksmValue(String type,String path) throws Exception {
            org.globus.ftp.vanilla.Reply reply = quote("CKSM "+type+" 0 -1 "+path);
            if ( !org.globus.ftp.vanilla.Reply.isPositiveCompletion(reply) )
                throw new java.io.IOException(reply.getMessage());
            return reply.getMessage();
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
       String out="";
       for ( int i = 0; i < bs.length; ++i)
           out += byteToHexString(bs[i]);
       return out;
    }

    private static final String [] __map =
       { "0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f" } ;

    static public String byteToHexString( byte b ) {

       int x = ( b < 0 ) ? ( 256 + (int)b ) : (int)b ;

       return __map[ ((int)b >> 4 ) & 0xf ] +
              __map[ ((int)b      ) & 0xf ] ;
    }

    
}
