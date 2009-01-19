// $Id: DCapProtocol_3.java,v 1.20 2006-12-15 15:38:03 tigran Exp $

package diskCacheV111.movers ;
import  diskCacheV111.vehicles.* ;
import  diskCacheV111.util.* ;
import  diskCacheV111.repository.* ;

import  dmg.cells.nucleus.* ;
import  dmg.util.*;

import  java.io.* ;
import  java.net.* ;
import  java.util.* ;
import  java.lang.reflect.* ;

public class DCapProtocol_3 implements MoverProtocol {

   private static final int INC_SPACE  =  (50*1024*1024) ;
   //
   // <init>( CellAdapter cell ) ;
   //
   private Args          _args      = null ;
   private Dictionary    _context   = null ;
   private CellAdapter      _cell   = null ;

   private long _bytesTransferred   = -1 ;
   private long _transferStarted    = 0 ;
   private long _transferTime       = -1 ;
   private long _lastTransferred    = System.currentTimeMillis() ;

   private byte [] _bigBuffer       = null ;
   private boolean _debug           = false ;
   private long    _spaceUsed       = 0 ;
   private long    _spaceAllocated  = 0 ;
   private int     _allocationSpace = INC_SPACE ;
   private String  _status          = "None" ;
   private long    _crash           = -1 ;
   private String  _crashType       = null ;
   private boolean _io_ok           = true ;
   private long    _ioError         = -1 ;
   private boolean _wasChanged      = false ;

   private MoverIoBuffer _defaultBufferSize = new MoverIoBuffer( 256 * 1024 , 256 * 1024 , 256 * 1024 ) ;
   private MoverIoBuffer _maxBufferSize     = new MoverIoBuffer( 1024 * 1024 , 1024 * 1024 , 1024 * 1024 ) ;

   public DCapProtocol_3( CellAdapter cell ){
       _cell    = cell ;
       _args    = _cell.getArgs() ;
       _context = _cell.getDomainContext() ;
       //
       _cell.say( "DCapProtocol_3 created" ) ;
       //
       // we are created for each request. So our data
       // is not shared.
       //
       _defaultBufferSize.setBufferSize(
          getParameterInt( "defaultSendBufferSize" , _defaultBufferSize.getSendBufferSize() ) ,
          getParameterInt( "defaultRecvBufferSize" , _defaultBufferSize.getRecvBufferSize() ) ,
          getParameterInt( "defaultIoBufferSize"   , _defaultBufferSize.getIoBufferSize() )
                                        ) ;
       _maxBufferSize.setBufferSize(
          getParameterInt( "maxSendBufferSize" , _maxBufferSize.getSendBufferSize() ) ,
          getParameterInt( "maxRecvBufferSize" , _maxBufferSize.getRecvBufferSize() ) ,
          getParameterInt( "maxIoBufferSize"   , _maxBufferSize.getIoBufferSize() )
                                        ) ;
       say("Setup : Defaults Buffer Sizes  : "+_defaultBufferSize ) ;
       say("Setup : Max Buffer Sizes       : "+_maxBufferSize ) ;

   }
   private Socket getSocket( InetAddress host , int port ) throws Exception {
      return new Socket( host , port ) ;
   }
   private synchronized int getParameterInt( String name , int defaultValue ){
       String stringValue = (String)_context.get("dCap3-"+name);
       stringValue = stringValue == null ? (String)_args.getOpt(name) : stringValue ;
       try{
          return stringValue == null ? defaultValue : Integer.parseInt(stringValue);
       }catch(Exception ee ){
          return defaultValue ;
       }
   }
   private void debug( String str ){
      if(_debug)_cell.say( "(DCap_3) "+str ) ;
   }
   private void say( String str ){
      _cell.say( "(DCap_3) "+str ) ;
   }
   private void esay( String str ){
      _cell.esay( "(DCap_3) "+str ) ;
   }
   private void esay( Exception e ){
      _cell.esay( e ) ;
   }
   public String toString(){
      return "SU="+_spaceUsed+";SA="+_spaceAllocated+";S="+_status ;
   }
   public void runIO(
                  RandomAccessFile  diskFile ,
                  ProtocolInfo protocol ,
                  StorageInfo  storage ,
                  PnfsId       pnfsId ,
                  SpaceMonitor spaceMonitor ,
                  int          access   )

          throws Exception {

        if( ! ( protocol instanceof DCapProtocolInfo ) ){
           throw new
           CacheException( 44 , "protocol info not DCapProtocolInfo" ) ;

        }
        Exception ioException = null ;

        try{
          String crash = storage.getKey("crash") ;
          if( crash != null ){
             _crash     = crash.length() == 0 ? 654321L : Long.parseLong( crash ) ;
             _crashType = storage.getKey("crashType");
              say( "Options : crash = "+crash+" ; type = "+ _crashType ) ;
          }
        }catch(Exception e){}

        try{
          String allocation = storage.getKey("alloc-space") ;
          if( allocation != null ){
             _allocationSpace = Integer.parseInt( allocation ) ;
              say( "Options : alloc-space = "+_allocationSpace ) ;
          }
        }catch(Exception e){}
        say("crash       = "+_crash ) ;
        say("crashType   = "+_crashType ) ;
        say("alloc-space = "+_allocationSpace ) ;

        try{
          String debug = storage.getKey("debug") ;
          if( debug != null )say( "Options : debug = "+debug ) ;
          if( debug != null ){
              if( debug.length() == 0 )_debug = true ;
              else{
                _debug =  Integer.parseInt( debug ) > 0 ;
              }
          }
        }catch(Exception e){}
        say("debug = "+_debug ) ;

        try{
          String io = storage.getKey("io-error") ;
          if( io != null )_ioError = Long.parseLong( io ) ;
        }catch(Exception e){}
        say("ioError = "+_ioError ) ;


        MoverIoBuffer bufferSize = new MoverIoBuffer( _defaultBufferSize ) ;

        {
           String tmp     = null ;
           int    tmpSize = 0 ;

           try{
              tmp = storage.getKey("send") ;
              if( tmp != null )
                 bufferSize.setSendBufferSize(
                       Math.min(Integer.parseInt(tmp),_maxBufferSize.getSendBufferSize())
                                             );
           }catch(Exception e){}
           try{
              tmp = storage.getKey("receive") ;
              if( tmp != null )
                 bufferSize.setRecvBufferSize(
                       Math.min(Integer.parseInt(tmp),_maxBufferSize.getRecvBufferSize())
                                             );
           }catch(Exception e){}
           try{
              tmp = storage.getKey("bsize") ;
              if( tmp != null )
                 bufferSize.setIoBufferSize(
                       Math.min(Integer.parseInt(tmp),_maxBufferSize.getIoBufferSize())
                                             );
           }catch(Exception e){}

        }

        say("Client : Buffer Sizes : "+bufferSize) ;

        try{
            _bigBuffer = _bigBuffer == null ? new byte[bufferSize.getIoBufferSize()] : _bigBuffer ;
        }catch(OutOfMemoryError om){
            _bigBuffer = new byte[32*1024] ;
        }


        DCapProtocolInfo dcap = (DCapProtocolInfo)protocol ;
        int        sessionId  = dcap.getSessionId() ;
	int        port       = dcap.getPort() ;
	String []  hosts      = dcap.getHosts() ;
        String     host       = null ;
	Socket     dataSocket = null ;
        Exception  se         = null ;
        //
        // try to connect to the client, scan the list.
        //
        for( int i  = 0 ; i < hosts.length ; i++ ){
           try{
             host = hosts[i] ;
             dataSocket = getSocket( InetAddress.getByName(host) , port ) ;
           }catch( Exception ee ){
              esay( "Can't connect to "+host );
              se = ee ;
              continue ;
           }
           break ;
        }
        if( dataSocket == null )throw se ;

        dataSocket.setReceiveBufferSize( bufferSize.getRecvBufferSize() ) ;
        dataSocket.setSendBufferSize( bufferSize.getSendBufferSize() ) ;

        say("Using : Buffer Sizes (send/recv/io) : "+
               dataSocket.getSendBufferSize()+"/"+
               dataSocket.getReceiveBufferSize()+"/"+
               _bigBuffer.length ) ;

        String  x = null ;
        boolean dummyRead  =
              ( ( x = storage.getKey("dummyRead") ) != null ) &&
              (   x.equals( "yes" ) ||  x.equals( "on" )       )  ;

	DCapDataOutputStream ostream   =
                new DCapDataOutputStream( dataSocket.getOutputStream() ) ;
        DataInputStream  istream =
                new DataInputStream( dataSocket.getInputStream() ) ;

        DCapDataOutputStream cntOut = ostream ;
        DataInputStream      cntIn  = istream ;

        say( "Connected to "+host+"("+port+")" ) ;
        //
        // send the sessionId and our (for now) 0 byte security challenge.
        //
        cntOut.writeInt( sessionId ) ;
        cntOut.writeInt( 0 ) ;
        cntOut.flush() ;
        //
        //
        _transferStarted  = System.currentTimeMillis() ;
        _lastTransferred  = System.currentTimeMillis() ;
        _bytesTransferred = 0 ;
        //
        // GO into the command loop
        //
        int     commandSize , commandCode ;
        boolean notDone  = true ;
        int     minSize  = 0 ;
        try{
           while( notDone ){
//              say( "PostPosition : "+diskFile.getFilePointer() ) ;
           if( Thread.currentThread().interrupted() )
              throw new
              InterruptedException("Interrupted By Operator");
              try{
                 commandSize = cntIn.readInt() ;
              }catch(EOFException eofe ){
                 //
                 // this is not an error
                 //
                 esay( "Dataconnection closed by peer : "+eofe ) ;
                 throw eofe;
              }
              if( commandSize < 4 )
                throw new
                CacheException(44,"Protocol Violation (cl<4)");

              _lastTransferred    = System.currentTimeMillis() ;

//              say( "PrePosition : "+diskFile.getFilePointer() ) ;

              commandCode = cntIn.readInt() ;
              switch( commandCode ){
                 //-------------------------------------------------------------
                 //
                 //                     The Write
                 //
                 case DCapConstants.IOCMD_WRITE :
                    //
                    // no further arguments (yet)
                    //
                    minSize = 4 ;
                    if( commandSize > minSize )
                        cntIn.skipBytes(commandSize-minSize);

                    if( ! _io_ok ){

                       String errmsg = "WRITE denied (IO not ok)" ;
                       esay(errmsg ) ;
                       cntOut.writeACK(DCapConstants.IOCMD_WRITE,CacheRepository.ERROR_IO_DISK,errmsg) ;

                    }else if( dcap.isWriteAllowed() ){

                       //
                       //   The 'REQUEST ACK'
                       //
                       cntOut.writeACK(DCapConstants.IOCMD_WRITE) ;
                       //
                       doTheWrite( diskFile ,
                                   cntOut ,
                                   istream ,
                                   spaceMonitor ) ;
                       //
                       //
                       if( _io_ok ){
                          cntOut.writeFIN(DCapConstants.IOCMD_WRITE) ;
                       }else{
                          esay( "Reporting IO problem to client" ) ;
                          cntOut.writeFIN(DCapConstants.IOCMD_WRITE,CacheRepository.ERROR_IO_DISK,"[2]Problem in writing") ;
                       }

                    }else{

                       String errmsg = "WRITE denied (not allowed)" ;
                       esay(errmsg ) ;
                       cntOut.writeACK(DCapConstants.IOCMD_WRITE,CacheRepository.ERROR_IO_DISK,errmsg) ;

                    }
                 break ;
                 //-------------------------------------------------------------
                 //
                 //                     The Read
                 //
                 case DCapConstants.IOCMD_READ :
                    //
                    //
                    minSize = 12 ;
                    if( commandSize < minSize )
                       throw new
                       CacheException(45,"Protocol Violation (clREAD<8)");

                    long blockSize = cntIn.readLong() ;

                    if( commandSize > minSize )
                        cntIn.skipBytes(commandSize-minSize);

                    debug( "READ byte="+blockSize+(dummyRead?" (dummy)":" (read)") ) ;

                    if( _io_ok ){

                       cntOut.writeACK(DCapConstants.IOCMD_READ) ;
                       if( dummyRead ){
                          doDummyRead( diskFile , cntOut , ostream , blockSize ) ;
                       }else{
                          doTheRead( diskFile , cntOut , ostream , blockSize ) ;
                       }

                       if( _io_ok ){
                          cntOut.writeFIN(DCapConstants.IOCMD_READ) ;
                       }else{
                          String errmsg = "FIN : READ failed (IO not ok)" ;
                          esay(errmsg ) ;
                          cntOut.writeFIN(DCapConstants.IOCMD_READ,CacheRepository.ERROR_IO_DISK,errmsg) ;
                       }
                    }else{

                       String errmsg = "ACK : READ denied (IO not ok)" ;
                       esay(errmsg ) ;
                       cntOut.writeACK(DCapConstants.IOCMD_READ,CacheRepository.ERROR_IO_DISK,errmsg) ;

                    }

                 break ;
                 //-------------------------------------------------------------
                 //
                 //                     The Seek
                 //
                 case DCapConstants.IOCMD_SEEK :

                    minSize = 16 ;
                    if( commandSize < minSize )
                       throw new
                       CacheException(46,"Protocol Violation (clSEEK<16)");

                    long offset = cntIn.readLong() ;
                    int  whence = cntIn.readInt() ;

                    if( commandSize > minSize )
                        cntIn.skipBytes(commandSize-minSize);

                    doTheSeek( diskFile , whence , offset , spaceMonitor , dcap.isWriteAllowed()) ;

                    if( _io_ok ){

                      cntOut.writeACK( diskFile.getFilePointer() ) ;

                    }else{

                       String errmsg = "SEEK failed : IOError ";
                       esay(errmsg);
                       cntOut.writeACK(DCapConstants.IOCMD_SEEK,6,errmsg) ;

                    }

                 break ;
                 //-------------------------------------------------------------
                 //
                 //                     The IOCMD_SEEK_AND_READ
                 //
                 case DCapConstants.IOCMD_SEEK_AND_READ :

                    minSize = 24 ;

                    if( commandSize < minSize )
                       throw new
                       CacheException(46,"Protocol Violation (DCapConstants.IOCMD_SEEK_AND_READ<16)");

                    offset    = cntIn.readLong() ;
                    whence    = cntIn.readInt() ;
                    blockSize = cntIn.readLong() ;

                    if( commandSize > minSize )
                        cntIn.skipBytes(commandSize-minSize);

                    if( _io_ok){

                       cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_READ) ;

                       doTheSeek( diskFile , whence , offset , spaceMonitor , dcap.isWriteAllowed()) ;

                       if( _io_ok )doTheRead( diskFile , cntOut , ostream , blockSize ) ;

                       if( _io_ok ){
                          cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_READ) ;
                       }else{
                          String errmsg = "FIN : SEEK_READ failed (IO not ok)" ;
                          esay(errmsg ) ;
                          cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_READ,CacheRepository.ERROR_IO_DISK,errmsg) ;
                       }

                    }else{
                       String errmsg = "SEEK_AND_READ denied : IOError "  ;
                       esay(errmsg);
                       cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_READ,CacheRepository.ERROR_IO_DISK,errmsg) ;
                    }
                 break ;
                 //-------------------------------------------------------------
                 //
                 //                     The IOCMD_SEEK_AND_WRITE
                 //
                 case DCapConstants.IOCMD_SEEK_AND_WRITE :

                    minSize = 16 ;
                    if( commandSize < minSize )
                       throw new
                       CacheException(46,"Protocol Violation (SEEK_AND_WRITE<16)");

                    offset    = cntIn.readLong() ;
                    whence    = cntIn.readInt() ;

                    if( commandSize > minSize )
                        cntIn.skipBytes(commandSize-minSize);

                    if( _io_ok ){

                       cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_WRITE) ;

                       doTheSeek( diskFile , whence , offset , spaceMonitor , dcap.isWriteAllowed()) ;

                       if( _io_ok )
                       doTheWrite( diskFile ,
                                   cntOut ,
                                   istream ,
                                   spaceMonitor ) ;

                       if( _io_ok ){
                          cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_WRITE) ;
                       }else{
                          String errmsg = "SEEK_AND_WRITE failed : IOError" ;
                          esay( errmsg ) ;
                          cntOut.writeFIN(DCapConstants.IOCMD_SEEK_AND_WRITE,CacheRepository.ERROR_IO_DISK,errmsg) ;
                       }

                    }else{
                       String errmsg = "SEEK_AND_WRITE denied : IOError" ;
                       esay(errmsg);
                       cntOut.writeACK(DCapConstants.IOCMD_SEEK_AND_WRITE,CacheRepository.ERROR_IO_DISK,errmsg) ;
                    }
                 break ;
                 //-------------------------------------------------------------
                 //
                 //                     The IOCMD_CLOSE
                 //
                 case DCapConstants.IOCMD_CLOSE :
                    minSize = 4 ;
                    if( commandSize > minSize )
                        cntIn.skipBytes(commandSize-minSize);

                    if( _io_ok ){
                       cntOut.writeACK(DCapConstants.IOCMD_CLOSE) ;
                    }else{
                       cntOut.writeACK(DCapConstants.IOCMD_CLOSE,CacheRepository.ERROR_IO_DISK,"IOError") ;
                    }
                    notDone = false ;
                 break ;
                 //-------------------------------------------------------------
                 //
                 //                     The IOCMD_LOCATE
                 //
                 case DCapConstants.IOCMD_LOCATE :
                    minSize = 4 ;
                    if( commandSize > minSize )
                        cntIn.skipBytes(commandSize-minSize);
                    try{
                       long size     = diskFile.getFilePointer() ;
                       long location = diskFile.length() ;
                       debug( "LOCATE : size="+size+";position="+location) ;
                       cntOut.writeACK( location , size ) ;
                    }catch(Exception e){
                       cntOut.writeACK(DCapConstants.IOCMD_LOCATE,-1,e.toString()) ;
                    }
                 break ;
                 default :
                    cntOut.writeACK(666, 9,"Invalid mover command : "+commandCode) ;


              }

          }
       }catch(Exception e ){
          //
          // this is an error
          //
          esay( "Problem in command block : "+e ) ;
          esay(e);
          ioException = e ;
       }finally{

          try{ dataSocket.close() ; }catch(Exception xe){}

          dcap.setBytesTransferred( _bytesTransferred ) ;

          _transferTime = System.currentTimeMillis() -
                          _transferStarted ;
          dcap.setTransferTime( _transferTime ) ;

          say( "(Transfer finished : "+
                    _bytesTransferred+" bytes in "+
                    ( _transferTime/1000 ) +" seconds) " ) ;

          long diskFileSize = diskFile.length() ;
          if( spaceMonitor != null ){
             long freeIt = _spaceAllocated - _spaceUsed ;
             debug("Returning "+freeIt+" bytes");
             if( freeIt > 0 )spaceMonitor.freeSpace( freeIt) ;
          }
          //
          // final check
          //
          if( ( spaceMonitor != null ) &&
              (_spaceAllocated > 0   ) &&
              ( diskFileSize != _spaceUsed   ) ){

             esay( "Seems to be an IO error : diskFileSize ("+diskFileSize+") != _spaceUsed("+_spaceUsed+")" ) ;
             //
             // we have to correct the _spaceUsed here although we will remove
             // the file later because when remove the file, the system
             // return the filesize and not _spaceUsed.
             //
             long freeIt = _spaceUsed - diskFileSize  ;
             if( freeIt > 0 ){
                esay("Releasing "+freeIt+" bytes" ) ;
                spaceMonitor.freeSpace( freeIt ) ;
             }else if( freeIt < 0 ){
                esay("Releasing "+( - freeIt) +" bytes" ) ;
                spaceMonitor.allocateSpace( - freeIt ) ;
             }
             _io_ok = false ;
          }

          //
          // if we got an EOF from the inputstream
          // we cancel the request but we don't want to
          // disable the pool.
          //
          if( ( ioException != null ) &&
              ( ioException instanceof EOFException ) )throw ioException ;

          if( ! _io_ok )
             throw new
             CacheException(
                  CacheRepository.ERROR_IO_DISK ,
                  "Disk I/O Error"      ) ;


       }

   }
   private void doTheSeek( RandomAccessFile diskFile , int whence , long offset ,
                           SpaceMonitor spaceMonitor , boolean writeAllowed )
           throws Exception {

       try{
         long eofSize   = diskFile.length() ;
         long position  = diskFile.getFilePointer() ;
	 long newOffset = 0L ;
         switch( whence ){
            case DCapConstants.IOCMD_SEEK_SET :
               debug( "SEEK "+offset+" SEEK_SET" ) ;
               //
               // this should reset the io state
               //
               if( offset == 0L )_io_ok = true ;
               //
	       newOffset = offset ;
            break ;
            case DCapConstants.IOCMD_SEEK_CURRENT :
               debug( "SEEK "+offset+" SEEK_CURRENT" ) ;
	       newOffset = position + offset ;
            break ;
            case DCapConstants.IOCMD_SEEK_END :
               debug( "SEEK "+offset+" SEEK_END" ) ;
	       newOffset = eofSize + offset ;
            break ;
            default :
               throw new IllegalArgumentException("Invalid seek mode : "+whence ) ;
         }
	 if( ( newOffset > eofSize ) && ! writeAllowed )
	       throw new
	       IOException("Seek beyond EOF not allowed (write not allowed)") ;

         //
         // if there is a space monitor, we use it
         //
         if( ( spaceMonitor != null ) && ( newOffset > eofSize ) ){
               while( newOffset > _spaceAllocated){
        	  _status = "WaitingForSpace("+_allocationSpace+")" ;
        	  debug( "Allocating new space : "+_allocationSpace ) ;
        	  spaceMonitor.allocateSpace( _allocationSpace ) ;
        	  _spaceAllocated += _allocationSpace ;
        	  debug( "Allocated new space : "+_allocationSpace ) ;
        	  _status = "" ;
               }
         }
         diskFile.seek( newOffset) ;
	 _spaceUsed = Math.max(newOffset,_spaceUsed) ;
      }catch(Exception ee ){
//
//          don't disable pools because of this.
//
//         _io_ok = false ;
         esay( "Problem in seek : "+ee ) ;
      }


   }
   private void doTheWrite( RandomAccessFile     diskFile ,
                            DCapDataOutputStream cntOut ,
                            DataInputStream      istream  ,
                            SpaceMonitor         spaceMonitor  ) throws Exception{

        int     rest ;
        byte [] data = _bigBuffer ;
        int     size = 0 , rc = 0 ;

        int len = istream.readInt() ;
        int com = istream.readInt() ;
        if( com != DCapConstants.IOCMD_DATA )
          throw new
          IOException( "Expecting : "+DCapConstants.IOCMD_DATA+" ; got : "+com ) ;

        while( ! Thread.currentThread().isInterrupted()){

           _status = "WaitingForSize" ;
           int nextBlockSize = rest = istream.readInt() ;
           debug( "Next data block : "+rest+" bytes" ) ;
           //
           // if there is a space monitor, we use it
           //
           long position = diskFile.getFilePointer() ;
           if( spaceMonitor != null ){
              while( ( position + rest ) > _spaceAllocated ){
                 _status = "WaitingForSpace("+_allocationSpace+")" ;
                 debug( "Allocating new space : "+_allocationSpace ) ;
                 spaceMonitor.allocateSpace( _allocationSpace ) ;
                 _spaceAllocated += _allocationSpace ;
                 debug( "Allocated new space : "+_allocationSpace ) ;
                 _status = "" ;
              }
           }
           //
           // we take whatever we get from the client
           // and at the end we tell'em that something went
           // terribly wrong.
           //
           _wasChanged = true ;
           if( rest == 0 )continue ;
           if( rest < 0 )break ;
           while( rest > 0  ){
              size = data.length > rest ?
                     rest : data.length ;
              _status = "WaitingForInput" ;
              rc = istream.read( data , 0 , size ) ;
              if( rc <= 0 )break ;
              if( _io_ok ){
                 try{
                     _status = "WaitingForWrite" ;
                     diskFile.write( data , 0 , rc ) ;
                 }catch(IOException ioe){
                     esay( "IOException in writing data to disk : "+ioe ) ;
                     _io_ok = false ;
                 }
              }
              rest -= rc ;
              _bytesTransferred += rc ;
              if( ( _ioError > 0L ) &&
                  ( _bytesTransferred > _ioError ) ){ _io_ok = false ; }
           }
           _spaceUsed = Math.max( position + nextBlockSize , _spaceUsed ) ;
           debug( "Block Done" ) ;
        }
        _status = "Done" ;

        return ;

   }
   private void doTheRead( RandomAccessFile  diskFile ,
                           DCapDataOutputStream  cntOut ,
                           DCapDataOutputStream  ostream ,
                           long              blockSize ) throws Exception{

        //
        // REQUEST WRITE
        //
        cntOut.writeDATA_HEADER() ;
        //
        //
        if( blockSize == 0 ){
           cntOut.writeInt(0) ;
           cntOut.writeInt(-1) ;
           return ;
        }
        long    rest = blockSize ;
        byte [] data = _bigBuffer ;
        int     size = 0 , rc = 0  ;

        while( ! Thread.currentThread().isInterrupted()){
           size = ((long)data.length) > rest ?
                  (int)rest : data.length ;

           try{
              rc = diskFile.read( data , 0 , size ) ;
              if( rc <= 0 )break ;
           }catch(Exception ee ){
              _io_ok = false ;
              break ;
           }
           ostream.writeDATA_BLOCK( data , 0 , rc  ) ;
           rest -= rc ;
           _bytesTransferred += rc ;
           if( ( _ioError > 0L ) && ( _bytesTransferred > _ioError ) ){
              _io_ok = false ;
              break ;
           }
           if( rest <= 0 )break ;
        }
        //
        // data chain delimiter
        //
        ostream.writeDATA_TRAILER() ;

        return ;
   }
   private void doDummyRead( RandomAccessFile     diskFile ,
                             DCapDataOutputStream cntOut ,
                             DCapDataOutputStream ostream ,
                             long                 blockSize ) throws Exception{

        //
        // REQUEST ACK
        //
        cntOut.writeACK(DCapConstants.IOCMD_READ) ;
        //
        // REQUEST WRITE
        //
        cntOut.writeDATA_HEADER() ;
        //
        //
        long fileLength = diskFile.length() ;
        long position   = diskFile.getFilePointer() ;
        long fileRest   = Math.max( fileLength - position , 0 ) ;

        if( blockSize == 0 ){
           cntOut.writeInt(0) ;
           cntOut.writeInt(-1) ;
           return ;
        }
        long    rest = blockSize ;
        byte [] data = _bigBuffer ;
        int     size = 0 , rc = 0 ;
        long    transferred = 0L ;
        try{
           while( ! Thread.currentThread().isInterrupted()){
              size = ((long)data.length) > rest ?
                     (int)rest : data.length ;

              rc = (long)size > fileRest ? (int)fileRest : size ;
              if( rc <= 0 )break ;
              ostream.writeDATA_BLOCK( data , 0 , rc  ) ;
              rest              -= rc ;
              _bytesTransferred += rc ;
              transferred       += (long)rc ;
              fileRest          -= (long)rc ;
              if( rest <= 0 )break ;
           }
        }catch(Exception  e ){
           esay("Exception in doTheRead : "+e ) ;
           throw e ;
        }
        diskFile.seek( position + transferred ) ;
        //
        // data chain delimiter
        //
        ostream.writeDATA_TRAILER() ;


        return ;
   }
   public long getLastTransferred() { return _lastTransferred ; }
   public long getBytesTransferred(){ return _bytesTransferred  ; }
   public long getTransferTime(){
       return _transferTime < 0 ?
              System.currentTimeMillis() - _transferStarted :
              _transferTime  ;
   }
   //
   //   attributes
   //
   public synchronized void setAttribute( String name , Object attribute ){
      if( name.equals( "allocationSpace" ) ){
         if( attribute instanceof Integer )_allocationSpace = ((Integer)attribute).intValue() ;
         else{
            _allocationSpace = Integer.parseInt( attribute.toString() ) ;
         }
      }
   }
   public synchronized Object getAttribute( String name ){
     if( name.equals( "allocationSpace" ) )return Integer.valueOf(_allocationSpace) ;
     throw new
     IllegalArgumentException( "Couldn't find "+name ) ;
   }
   public boolean wasChanged(){ return _wasChanged ; }
}
