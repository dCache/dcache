// $Id: DirectoryLookUpPool.java,v 1.5 2005-12-14 10:00:24 tigran Exp $


package diskCacheV111.pools;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;
import  diskCacheV111.movers.* ;
import  diskCacheV111.repository.* ;
import  diskCacheV111.util.event.* ;

import  dmg.cells.nucleus.*;
import  dmg.util.*;
import  dmg.cells.services.* ;

import  java.util.*;
import  java.io.*;
import  java.net.*;
import  java.lang.reflect.* ;

public class DirectoryLookUpPool extends CellAdapter {
    
    
    
    private String      _poolName = null;
    private Args        _args     = null;
    private CellNucleus _nucleus  = null;
    
    private PnfsHandler            _pnfs         = null ;    
    private String _pnfsManagerName   = "PnfsManager";
    private String _rootDir = null ;
    
    public DirectoryLookUpPool(String poolName, String args) throws Exception {
        super( poolName , DirectoryLookUpPool.class.getName(), args , false );
        
        
        _poolName = poolName;
        _args     = getArgs();
        _nucleus  = getNucleus() ;                
        
        int argc = _args.argc();
        say("Lookup Pool "+poolName +" starting");
        
        try {
            
            if (argc < 1)
                throw new
                IllegalArgumentException("no base dir specified");
            
            _rootDir      = _args.argv(0);
            
            
            _pnfs = new PnfsHandler( this ,
            new CellPath( _pnfsManagerName ) ,
            _poolName  ) ;
            
            
        } catch (Exception e){
            say("Exception occurred on startup: "+e);
            start();
            kill();
            throw e;
        }
        
        
        useInterpreter( true );
        _nucleus.export();
        start() ;
        
    }
    
    public void log(String str  ){ say( str ) ; }
    public void elog(String str ){ esay( str ) ; }
    public void plog(String str ){ esay( "PANIC : "+str ) ; }
    public void getInfo( PrintWriter pw ){
        pw.println("Root directory    : "+_rootDir);
        pw.println("Revision          : [$Id: DirectoryLookUpPool.java,v 1.5 2005-12-14 10:00:24 tigran Exp $]" ) ;
    }
    
    public void say( String str ){
        pin( str );
        super.say( str );
    }
    public void esay( String str ){
        pin( str ) ;
        super.esay( str ) ;
    }
    
    public void messageToForward( CellMessage cellMessage ){
           messageArrived( cellMessage ) ;
    }
    
    public void messageArrived( CellMessage cellMessage ){
  
        
        
        esay("DirectoryLookUp: message arrived: " + cellMessage);
        
        Object messageObject  = cellMessage.getMessageObject();
        
        if (! (messageObject instanceof Message) ){
            say("Unexpected message class 1 "+messageObject.getClass());
            return;
        }
        
        Message poolMessage = (Message)messageObject ;
        
        if( poolMessage instanceof PoolIoFileMessage){
            
            PoolIoFileMessage msg = (PoolIoFileMessage)poolMessage ;
            
            ioFile( (PoolIoFileMessage)poolMessage,  cellMessage );
            
            return ;
            
        }
        
        
    }
    
    // commands
    public String hh_ls_$_1 = "ls <path>";
    public String ac_ls_$_1( Args args ){
        String path = _rootDir + args.argv(0);
        StringBuffer sb =  new StringBuffer();
        File f = new File(path);
        
        if( !f.exists() ) {
            sb.append("Path " + path + " do not exist.");
        }else{
            
            if( f.isDirectory() ) {
                String[] list = f.list() ;
                if( list != null ) {
                    for( int i = 0; i < list.length; i++) {
                        File ff = new File(path, list[i] );
                        try {
                            PnfsFile pnfsFile   = new PnfsFile( f, list[i] ) ;
                            sb.append( pnfsFile.getPnfsId().toString() );
                        }catch( Exception e) { continue ;}
                        if( ff.isDirectory() ) {
                            sb.append(":d:");
                        }else{
                            sb.append(":f:");
                        }
                        sb.append(list[i].length()).append(':').append(list[i]).append('\n');
                    }
                }
            }else{
                if( f.isFile() ) {
                    sb.append(path).append(" : ").append( f.length() );
                }
            }
        }
        
        return sb.toString();
        
    }
    
    
    ////////////////////////////////////////////////////////////////
    //
    //     The io File  Part
    //
    //
    private void ioFile(
    PoolIoFileMessage poolMessage,
    CellMessage       cellMessage        ){
        
        cellMessage.revertDirection() ;
        
        ProtocolInfo protocolInfo = poolMessage.getProtocolInfo();
        String protocol = protocolInfo.getProtocol() ;
                
        try {
           _nucleus.newThread( new  DirectoryService(poolMessage, cellMessage ), "dir").start() ;
        }catch( Exception e) {
            esay(e);
        }
                
        
    }
    
    
    
    // this is a actual mover
    private class DirectoryService implements Runnable {
        
        
        private DCapProtocolInfo dcap;
        private int sessionId;

        
        private DCapDataOutputStream ostream;
        private DataInputStream  istream;
        private DCapDataOutputStream cntOut = null ;
        private DataInputStream      cntIn  = null ;
        private String _path = null;

        
        
        DirectoryService( PoolIoFileMessage poolMessage,
        CellMessage originalCellMessage  ) throws IOException {
            
            dcap = (DCapProtocolInfo)poolMessage.getProtocolInfo() ;
            
            PnfsId pnfsId = poolMessage.getPnfsId() ;            
            _path = PnfsFile.pathfinder( new File(_rootDir) , pnfsId.toString() );
            String rootPrefix = PnfsFile.pathfinder(new File(_rootDir) , PnfsFile.getMountId( new File(_rootDir) ).toString() );            
            _path = _rootDir + _path.substring( rootPrefix.length() );            
            
            sessionId = dcap.getSessionId();
                        
        }
        
        public void run() {
            
            boolean done = false;
            int  commandSize;
            int commandCode ;
            int minSize;
            String dirList = createDirEnt( _path );
            int index = 0;
            
            
            this.connectToClinet() ;
            
            while( !done && !Thread.currentThread().isInterrupted() ) {
                
                try {
                    commandSize = cntIn.readInt() ;
                    
                    if( commandSize < 4 )
                        throw new
                        CacheException(44,"Protocol Violation (cl<4)");
                    
                    commandCode = cntIn.readInt() ;
                    switch( commandCode ){
                        //-------------------------------------------------------------
                        //
                        //                     The IOCMD_CLOSE
                        //
                        case DCapConstants.IOCMD_CLOSE :
                            cntOut.writeACK(DCapConstants.IOCMD_CLOSE) ;
                            done = true ;
                            break ;
                            
                            //-------------------------------------------------------------
                            //
                            //                     The ReadDir
                            //
                        case DCapConstants.IOCMD_READ :
                            //
                            //
                            minSize = 12 ;
                            if( commandSize < minSize )
                                throw new
                                CacheException(45,"Protocol Violation (clREAD<8)");
                            
                            long numberOfEntries = cntIn.readLong() ;
                            esay("requested " + numberOfEntries + " bytes");
                            
                            cntOut.writeACK(DCapConstants.IOCMD_READ) ;
                            index += doReadDir(cntOut , ostream, dirList, index, numberOfEntries);
                            cntOut.writeFIN(DCapConstants.IOCMD_READ) ;
                            
                            
                            
                            break;
                        default :
                            cntOut.writeACK(1717, 9,"Invalid mover command : "+commandCode) ;
                            break;
                    }
                    
                    
                    
                }catch( Exception e ) { esay(e) ; return ;}
            }
            
        }
        
        
        void connectToClinet() {
            
            int        port       = dcap.getPort() ;
            String []  hosts      = dcap.getHosts() ;
            String     host       = null ;
            Exception  se         = null ;
            
            
            Socket dataSocket = null ;
            
            
            //
            // try to connect to the client, scan the list.
            //
            for( int i  = 0 ; i < hosts.length ; i++ ){
                try{
                    host = hosts[i] ;
                    dataSocket = new Socket(host, port );
                }catch( Exception e) {
                    se = e;
                    continue;
                }
                break;
            }
            
            
            if( dataSocket == null ) esay(se);
            
            try {
                
                ostream  =  new DCapDataOutputStream( dataSocket.getOutputStream() ) ;
                istream  =  new DataInputStream( dataSocket.getInputStream() ) ;
                esay( "Connected to "+host+"("+port+")" ) ;
                //
                // send the sessionId and our (for now) 0 byte security challenge.
                //
                
                
                cntOut = ostream ;
                cntIn  = istream ;                
                
                cntOut.writeInt( sessionId ) ;
                cntOut.writeInt( 0 ) ;
                cntOut.flush() ;
                
                
            } catch ( IOException ioException ) {
                esay("failed to connect to " + host+"("+port+")");
            }
            
        }
        
        
        
        private int doReadDir(DCapDataOutputStream  cntOut ,  DCapDataOutputStream  ostream , String dirList, int  index , long len)
        throws Exception {
            
            long rc = 0;
            byte data [] = null;
            
            
            if( index <  dirList.length() ) {
                data = dirList.getBytes();
                rc = len > dirList.length() - index ? dirList.length() - index : len;
            }
            
            
            cntOut.writeDATA_HEADER() ;
                        
            
            
            ostream.writeDATA_BLOCK( data , index , (int)rc  ) ;
            
            
            ostream.writeDATA_TRAILER() ;
            
            return (int)rc;
        }
        
        private String createDirEnt( String path ){
                            
        File f = new File(path);
        StringBuffer sb = new StringBuffer();
        
        if( !f.exists() ) {
            sb.append("Path " + path + " do not exist.");
        }else{
            
            if( f.isDirectory() ) {
                String[] list = f.list() ;
                if( list != null ) {
                    for( int i = 0; i < list.length; i++) {
                        File ff = new File(path, list[i] );
                        try {
                            PnfsFile pnfsFile   = new PnfsFile( f, list[i] ) ;
                            sb.append( pnfsFile.getPnfsId().toString() );
                        }catch( Exception e) { continue ;}
                        if( ff.isDirectory() ) {
                            sb.append(":d:");
                        }else{
                            sb.append(":f:");
                        }
                        sb.append(list[i].length()).append(':').append(list[i]).append('\n');
                    }
                }
            }else{
                if( f.isFile() ) {
                    sb.append(path).append(" : ").append( f.length() );
                }
            }
        }
        
        esay(sb.toString());
        return sb.toString();
        
    }
        
        
    } // end of private class
    
}  // end of MultiProtocolPool
