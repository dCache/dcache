// $Id: FtpProtocol_1.java,v 1.12 2003-06-11 09:19:32 cvs Exp $

package org.dcache.pool.movers;
import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;
import  dmg.cells.nucleus.*;
import  java.io.*;
import  java.net.*;

import org.apache.log4j.Logger;

import org.dcache.pool.repository.Allocator;

public class FtpProtocol_1
    implements MoverProtocol           {

    private final static Logger _log = Logger.getLogger(FtpProtocol_1.class);
    private final static Logger _logSpaceAllocation = Logger.getLogger("logger.dev.org.dcache.poolspacemonitor." + FtpProtocol_1.class.getName());

    private final CellEndpoint  _cell;
    private long _bytesTransferred = -1;
    private long _transferStarted  = 0;
    private long _transferTime     = 0;
    private static final int INC_SPACE  =  (50*1024*1024);
    private long    _spaceUsed       = 0;
    private long    _spaceAllocated  = 0;
    private int     _allocationSpace = INC_SPACE;
    private String  _status          = "None";
    private long    _lastTransferred = System.currentTimeMillis();
    private boolean _wasChanged      = false;

    public FtpProtocol_1(CellEndpoint cell){
        _cell = cell;
        say("FtpProtocol_1 created");
    }
    private void say(String str){
        _log.info(str);
    }
    private void esay(String str){
        _log.error(str);
    }
    public String toString(){
        return "SU="+_spaceUsed+";SA="+_spaceAllocated+";S="+_status;
    }
    public void setAttribute(String name, Object attribute){}
    public Object getAttribute(String name){ return null; }
    public void runIO(RandomAccessFile diskFile,
                      ProtocolInfo protocol,
                      StorageInfo  storage,
                      PnfsId       pnfsId ,
                      Allocator    allocator,
                      int          access)

        throws Exception {

        _lastTransferred = System.currentTimeMillis();
        if((access & MoverProtocol.WRITE) != 0){
            _wasChanged = true;
            runRemoteToDisk(diskFile, protocol, storage, pnfsId.toString(), allocator);
        }else{
            runDiskToRemote(diskFile, protocol, storage, pnfsId.toString() );
        }


    }

    public void runRemoteToDisk(RandomAccessFile diskFile,
                                ProtocolInfo protocol,
                                StorageInfo  storage,
                                String       pnfsId ,
                                Allocator allocator)

        throws Exception {

        if(! (protocol instanceof FtpProtocolInfo)){
            throw new
                CacheException(44, "protocol info not FtpProtocolInfo");

        }
        FtpProtocolInfo ftp = (FtpProtocolInfo)protocol;
	int    port = ftp.getPort();
	String host = ftp.getHost();
	Socket       dataSocket = new Socket(host, port);
	InputStream  istream    = dataSocket.getInputStream();
        say("Connected to "+host+"("+port+")");
        byte [] data = new byte[128*1024];
        int nbytes;
        SysTimer sysTimer = new SysTimer();
        _transferStarted  = System.currentTimeMillis();
        _bytesTransferred = 0;
        sysTimer.getDifference();
        try{
            while(! Thread.currentThread().isInterrupted()){
                nbytes = istream.read(data);
                if(nbytes <= 0) break;
                while((_spaceUsed + nbytes) > _spaceAllocated){
                    _status = "WaitingForSpace("+_allocationSpace+")";
                    _logSpaceAllocation.debug("ALLOC: " + pnfsId + " : " + _allocationSpace);
                    allocator.allocate(_allocationSpace);
                    _spaceAllocated += _allocationSpace;
                    _status = "";
                }
                diskFile.write(data, 0, nbytes);
                _lastTransferred = System.currentTimeMillis();
                _bytesTransferred += nbytes;
                _spaceUsed        += nbytes;
            }
        }finally{
            try{ dataSocket.close(); }catch(Exception xe){}
            ftp.setBytesTransferred(_bytesTransferred);
            _transferTime = System.currentTimeMillis() -
                _transferStarted;
            ftp.setTransferTime(_transferTime);
            say("Transfer finished : "+
                 _bytesTransferred+" bytes in "+
                 (_transferTime/1000) +" seconds ");
            if(_transferTime > 0){
                double rate =
                    ((double)_bytesTransferred)/((double)_transferTime)/1024.*1000./1024.;
                say("TransferRate : "+rate+" MBytes/sec");
            }
            say("SysTimer : "+sysTimer.getDifference().toString());
        }
    }

    public void runDiskToRemote(RandomAccessFile  diskFile,
                                ProtocolInfo protocol,
                                StorageInfo  storage,
                                String       pnfsId   )

        throws Exception {

        if(! (protocol instanceof FtpProtocolInfo)){
            throw new
                CacheException(44, "protocol info not FtpProtocolInfo");

        }
        FtpProtocolInfo ftp = (FtpProtocolInfo)protocol;
	int    port = ftp.getPort();
	String host = ftp.getHost();
        say("Connecting to "+host+"("+port+")");
	Socket       dataSocket = new Socket(host, port);
	OutputStream ostream    = dataSocket.getOutputStream();
        say("Connected to "+host+"("+port+")");
        long    fileSize        = storage.getFileSize();
        byte [] data            = new byte[128*1024];
        say("Expected filesize is "+fileSize+" bytes");

        String  x = storage.getKey("dummyRead");
        boolean dummyRead  = (fileSize > 0) &&
            (x != null) &&
            (x.equals("yes") || x.equals("on")) ;

        say("Using "+(dummyRead?"DummyRead":"RealRead"));

        x = storage.getKey("dummySize");
        if( x != null ){

            try{
                fileSize = Long.parseLong(x);
                say("Dummy Size set to "+fileSize);
            }catch(Exception ee){
                say("File size not changed ("+x+")");
            }
        }
        long waitTime = 0;
        x = storage.getKey("waitTime");
        if(x != null){
            try{
                waitTime = Long.parseLong(x);
                say("waitTime set to "+waitTime);
            }catch(Exception ee){
                say("No wait cycles");
            }
        }
        int nbytes;
        _transferStarted  = System.currentTimeMillis();
        _bytesTransferred = 0;
        SysTimer sysTimer = new SysTimer();
        sysTimer.getDifference();
        try{
            if(dummyRead){
                long rest = fileSize;
                while(! Thread.currentThread().isInterrupted()){
                    if(Thread.currentThread().isInterrupted())
                        throw new
                            InterruptedException("Transfer interrupted");
                    nbytes = rest > data.length ? data.length : ((int)rest);
                    if(nbytes <= 0) break;
                    ostream.write(data, 0, nbytes);
                    rest -= nbytes;
                    _bytesTransferred += nbytes;
                    if(waitTime > 0){
                        Thread.currentThread().sleep(0,10);
                    }
                }
            }else{
                while(! Thread.currentThread().isInterrupted()){
                    if(Thread.currentThread().isInterrupted())
                        throw new
                            InterruptedException("Transfer interrupted");
                    nbytes = diskFile.read(data);
                    //	       say("Transferring "+nbytes+" bytes");
                    if(nbytes <= 0) break;
                    ostream.write(data, 0, nbytes);
                    _lastTransferred = System.currentTimeMillis();
                    _bytesTransferred += nbytes;
                }
            }
        }finally{
            try{ dataSocket.close(); }catch(Exception xe){}
            ftp.setBytesTransferred(_bytesTransferred);
            _transferTime = System.currentTimeMillis() -
                _transferStarted;
            ftp.setTransferTime(_transferTime);

            say("Transfer finished : "+
                 _bytesTransferred+" bytes in "+
                 (_transferTime/1000) +" seconds ");
            if(_transferTime > 0){
                double rate =
                    ((double)_bytesTransferred)/((double)_transferTime)/1024.*1000./1024.;
                say("TransferRate : "+rate+" MBytes/sec");
            }
            say("SysTimer : "+sysTimer.getDifference().toString());
        }


    }

    public long getLastTransferred() { return System.currentTimeMillis(); }
    public long getBytesTransferred(){ return _bytesTransferred ; }
    public long getTransferTime(){
        return _transferTime < 0 ?
            System.currentTimeMillis() - _transferStarted :
            _transferTime ;
    }
    public boolean wasChanged(){ return _wasChanged; }
}
