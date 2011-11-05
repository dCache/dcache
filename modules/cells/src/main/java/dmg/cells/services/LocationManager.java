package dmg.cells.services ;

import java.net.* ;
import java.io.* ;
import java.util.* ;

import dmg.cells.nucleus.* ;
import dmg.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationManager extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(LocationManager.class);

   /**
     */
   private DatagramSocket _socket  = null ;
   private Server         _server  = null ;
   private Client         _client  = null ;
   private Args           _args    = null ;
   private CellNucleus    _nucleus = null ;
   //   Server Options : -strict[=yes|on|off|no]
   //                    -perm=<helpFilename>
   //                    -setupmode=write|rdonly|auto
   //                    -setup=<setupFile>
   //
   //   Client Options : -noboot
   //
   public class Server implements Runnable {
      private class NodeInfo {
          private String   _domainName = null ;
          private HashSet  _list    = new HashSet() ;
          private String   _default = null ;
          private boolean  _listen  = false ;
          private String   _address = null ;
          private boolean  _defined = true ;
          private int      _port    = 0 ;
          private String   _sec     = null ;

          private NodeInfo( String domainName ){
             _domainName = domainName ;
          }
          private NodeInfo( String domainName , boolean defined ){
             _domainName = domainName ;
             _defined    = defined ;
          }
          private boolean isDefined(){ return _defined ; }
          private String getDomainName(){ return _domainName ; }
          private synchronized void setDefault( String defaultNode ){
             _default = defaultNode ;
          }
          private int getConnectionCount(){ return _list.size() ; }
          private synchronized void add( String nodeName ){
             _list.add(nodeName);
          }
          private synchronized void remove( String nodeName ){
             _list.remove( nodeName ) ;
             return;
          }
          private void setListenPort( int port ){ _port = port ; }
          private void setSecurity( String sec ){ _sec = sec ; }
          private void setListen( boolean listen ){ _listen = listen ; }
          private void setAddress( String address ){ _listen=true; _address = address ; }

          private String getAddress(){ return _address ; }
          private String   getDefault(){ return _default ; }
          private Iterator connections(){ return _list.iterator() ; }
          private boolean  mustListen(){ return _listen ; }
          private String   getSecurity(){ return _sec ; }

          public String toWhatToDoReply( boolean strict ){

             StringBuffer sb = new StringBuffer() ;
             sb.append(_domainName).append(" ") ;
             if( _listen ){

                 sb.append( "\"l:" );
                 if( _port > 0 )sb.append(_port) ;
                 sb.append(":");
                 if( _sec != null )sb.append(_sec) ;
                 sb.append(":");
                 sb.append('"');
                 if( ( ! strict ) && ( _address != null ) )sb.append(" (").append(_address).append(")") ;

             }else{
                 sb.append("nl");
             }
             if( _default != null )sb.append( " d:" ).append( _default ) ;
             Iterator i = connections() ;
             while( i.hasNext() )sb.append(" c:").append(i.next().toString()) ;
             return sb.toString() ;
          }

          @Override
          public String toString(){
             return toWhatToDoReply(false);
          }

      }
      private final Map<String, NodeInfo>        _nodeDb = new HashMap<String, NodeInfo>() ;
      private int            _port   = 0 ;
      private DatagramSocket _socket = null ;
      private Thread         _worker = null ;
      private boolean        _strict = true ;
      private int _requestsReceived = 0 ;
      private int _repliesSent      = 0 ;
      private int _totalExceptions  = 0 ;
      /**
        *   Server
        *      -strict=yes|no         # 'yes' allows any client to register
        *      -setup=<setupFile>     # full path of setupfile
        *      -setupmode=rdonly|rw|auto   # write back the setup [def=rw]
        *      -perm=<filename>       # store registry information
        */
      private final static int SETUP_NONE   = -2 ;
      private final static int SETUP_ERROR  = -1 ;
      private final static int SETUP_AUTO   = 0 ;
      private final static int SETUP_WRITE  = 1 ;
      private final static int SETUP_RDONLY = 2 ;
      private int _setupMode = SETUP_NONE ;
      private String _setupFileName = null ;
      private File   _setupFile     = null ;
      private File   _permFile      = null ;

      private Server( int port , Args args ) throws Exception {
         _port = port ;
         addCommandListener(this);

         String strict = args.getOpt("strict") ;
         if( strict == null ){
            _strict = true ;
         }else{
            if( strict.equals("off") || strict.equals("no") )
               _strict = false ;
         }

         prepareSetup( args.getOpt("setup") , args.getOpt("setupmode") ) ;
         if( ( _setupMode == SETUP_WRITE ) || ( _setupMode == SETUP_RDONLY ) )
            execSetupFile( _setupFile ) ;

         preparePersistentMap( args.getOpt( "perm" ) ) ;

         try{ loadPersistentMap() ; }catch(Exception dd ){}
         _socket = new DatagramSocket( _port ) ;
         _worker = _nucleus.newThread(this,"Server") ;
         _worker.start() ;
      }
      private void preparePersistentMap( String permFileName ) throws Exception {
         if( ( permFileName == null ) || ( permFileName.length() < 1 ) )return ;

         File permFile  = new File( permFileName ) ;

         if( permFile.exists() ){
            if( ! permFile.canWrite() )
               throw new
               IllegalArgumentException( "Can't write to : "+permFileName ) ;
            _permFile = permFile ;
//            loadPersistentMap() ;
         }else{
            if( ! permFile.createNewFile() )
               throw new
               IllegalArgumentException( "Can't create : "+permFileName ) ;
            _permFile = permFile ;
         }
         _log.info("Persistent map file set to : "+_permFile);
         return;
      }
      private synchronized void loadPersistentMap() throws Exception {
         if( _permFile == null )return ;
         ObjectInputStream in = new ObjectInputStream(
                                      new FileInputStream( _permFile ) ) ;
         Map<String, String> hm = null ;
         _log.info("Loading persistent map file");
         try{
             hm = (HashMap)in.readObject() ;

             _log.info("Persistent map : "+hm);

             for(Map.Entry<String, String> node_and_address: hm.entrySet()) {

            	 String node = node_and_address.getKey();
            	 String address = node_and_address.getValue();

            	 NodeInfo info = getInfo( node , true ) ;
            	 if( info == null )continue ;
            	 info.setAddress( node ) ;
            	 _log.info( "Updated : <"+node+"> -> "+address ) ;
             }

         }catch(Exception ee){
             _log.warn("Problem reading persistent map "+ee.getMessage() );
             _permFile.delete() ;
         }finally{
            try{ in.close() ; }catch(IOException ee){}
         }

      }
      private synchronized void savePersistentMap() throws Exception {
         if( _permFile == null )return ;

         Map<String, String> hm = new HashMap<String, String>() ;

         for( NodeInfo info: _nodeDb.values() ){
            String address = info.getAddress() ;
            if( ( address != null ) && info.mustListen() )
                hm.put( info.getDomainName() , info.getAddress() ) ;
         }
         ObjectOutputStream out = null;

         try{
        	out = new ObjectOutputStream( new FileOutputStream( _permFile ) ) ;
            out.writeObject( hm ) ;
         }catch(Exception e){
             _log.warn("Problem writing persistent map "+e.getMessage() );
             _permFile.delete() ;
         }finally{
            if(out != null) try{ out.close() ; }catch(Exception ee){}
         }
         return ;
      }
      private void prepareSetup( String setupFile , String setupMode ) throws Exception {

         if( ( _setupFileName = setupFile ) == null ){
            _setupMode = SETUP_NONE ;
            return ;
         }
         String tmp = setupMode ;

         _setupMode = tmp == null          ? SETUP_AUTO :
                      tmp.equals("rw")     ? SETUP_WRITE :
                      tmp.equals("rdonly") ? SETUP_RDONLY :
                      tmp.equals("auto")   ? SETUP_AUTO :
                      SETUP_ERROR ;

         if( _setupMode == SETUP_ERROR )
            throw new
            IllegalArgumentException(
               "Setup error, don't understand : "+_setupMode);

         _setupFile     = new File( _setupFileName ) ;

         boolean fileExists = _setupFile.exists();
         boolean canWrite = _setupFile.canWrite();
         boolean canRead = _setupFile.canRead();
         if (fileExists && !_setupFile.isFile()) {
            throw new IllegalArgumentException("Not a file: " + _setupFileName);
         }

         if (_setupMode == SETUP_AUTO) {
             if (fileExists) {
                 _setupMode = canWrite ? SETUP_WRITE : SETUP_RDONLY;
             } else {
                 try {
                     _setupFile.createNewFile();
                     _setupMode = SETUP_WRITE ;
                 } catch (IOException e) {
                     /* This is usually a permission error.
                      */
                     _log.debug("Failed to create {}: {}", _setupFile, e);
                     _setupMode = SETUP_NONE;
                 }
             }
         }

         switch (_setupMode) {
         case SETUP_WRITE:
             if (fileExists) {
                 if (!canWrite) {
                     throw new IllegalArgumentException("File not writeable: " +
                                                        _setupFileName);
                 }
             } else {
                 _setupFile.createNewFile();
             }
             break;
         case SETUP_RDONLY:
             if (!fileExists) {
                 _setupMode = SETUP_NONE;
             } else if (!canRead) {
                 throw new IllegalArgumentException("Setup file not readable: " +
                                                    _setupFileName);
             }
             break;
         }

         if (_setupMode == SETUP_NONE) {
            _setupFileName = null;
         }
      }

      private void execSetupFile( File setupFile )throws Exception {
          BufferedReader br = new BufferedReader( new FileReader( setupFile ) ) ;
          String line = null ;
          try{
             while( ( line = br.readLine() ) != null ){
                if( line.length() < 1 )continue ;
                if( line.charAt(0) == '#' )continue ;
                _log.info("Exec : "+line) ;
                command( new Args(line) ) ;
             }
          }catch( EOFException eof ){
          }catch( Exception ef ){
             _log.warn("Ups : "+ef ) ;
          }finally{
             try{ br.close() ; }catch(Exception ce ){}
          }
          return ;
      }
      public void getInfo( PrintWriter pw ){
         pw.println( "         Version : $Id: LocationManager.java,v 1.15 2007-10-22 12:30:38 behrmann Exp $") ;
         pw.println( "      # of nodes : "+_nodeDb.size() ) ;
         pw.println( "RequestsReceived : "+_requestsReceived ) ;
         pw.println( "     RepliesSent : "+_repliesSent) ;
         pw.println( "     Exceptions  : "+_totalExceptions) ;
      }

      @Override
      public String toString(){
         return "Server:Nodes="+_nodeDb.size()+";Reqs="+_requestsReceived;
      }

      public void run(){
         DatagramPacket packet = null ;
         while (!Thread.currentThread().isInterrupted()){
            try{
                packet = new DatagramPacket(new byte[1024],1024) ;
                _socket.receive(packet);
            }catch(SocketException e) {
                if(!Thread.currentThread().isInterrupted()) {
                    _log.warn("Exception in Server receive loop (exiting)", e);
                }
                break;
            }catch(Exception ie){
                _log.warn("Exception in Server receive loop (exiting)", ie);
               break ;
            }
            try{
                process( packet ) ;
                _socket.send(packet);
            }catch(Exception se ){
                _log.warn("Exception in send ", se);
            }
         }
         _socket.close();
      }
      public void process( DatagramPacket packet )throws Exception{
          byte [] data = packet.getData() ;
          int   datalen = packet.getLength() ;
          InetAddress address = packet.getAddress() ;
          if( datalen <= 0 ){
             _log.warn( "Empty Packet arrived from "+packet.getAddress() ) ;
             return ;
          }
          String message = new String( data , 0 , datalen ) ;
          _log.info( "server query : ["+address+"] "+"("+message.length()+") "+message) ;

          message = (String)command( new Args( message ) ) ;

          _log.info( "server reply : "+message ) ;
          data = message.getBytes() ;
          packet.setData(data) ;
          packet.setLength(data.length);
          return ;
      }
      private void createSetup( PrintWriter pw ){
         pw.println( "#") ;
         pw.println( "# This setup was created by the LocationManager at "+( new Date().toString()));
         pw.println( "#" ) ;
         Iterator i = _nodeDb.values().iterator() ;
         while( i.hasNext() ){
            NodeInfo info = (NodeInfo)i.next() ;
            pw.println( "define "+info.getDomainName() ) ;
            if( info.mustListen() )pw.println( "listen "+info.getDomainName() ) ;
            String def = info.getDefault() ;
            if( def != null  )pw.println( "defaultroute "+info.getDomainName()+" "+def ) ;
            Iterator j = info.connections() ;
            while( j.hasNext() )
                pw.println( "connect "+info.getDomainName()+" "+j.next().toString() ) ;

         }
      }
      /**
        *   command interface
        */
      private final String [] __mode2string =
            { "none" , "error" , "auto" , "rw" , "rdonly" } ;
      private String setupToString( int mode ){
        if( ( mode < -2 ) || ( mode > 2 ) )return "?("+mode+")" ;
        return __mode2string[mode+2] ;
      }
      public String hh_ls_perm = " # list permanent file" ;
      public String ac_ls_perm( Args args ) throws Exception {
          if( _permFile == null )
             throw new
             IllegalArgumentException("Permamanet file not defined" ) ;

         ObjectInputStream in = new ObjectInputStream(
                                      new FileInputStream( _permFile ) ) ;
         Map<String, String> hm = null ;
         try{
             hm = (HashMap)in.readObject() ;
         }finally{
            if( in != null) try{ in.close() ; }catch(Exception ee){}
         }

         StringBuilder sb = new StringBuilder() ;
         for(Map.Entry<String, String> node_and_address: hm.entrySet()) {

            String node = node_and_address.getKey() ;
            String address = node_and_address.getValue() ;

            sb.append(node).append(" -> ").append(address).append("\n");
         }
         return sb.toString() ;

      }
      public String hh_setup_define = "<filename> [-mode=rw|rdonly|auto]" ;
      public String ac_setup_define_$_1( Args args )throws Exception {
         String filename = args.argv(0);
         prepareSetup( filename , args.getOpt( "mode" ) ) ;
         return "setupfile (mode="+setupToString(_setupMode)+") : "+filename ;
      }
      public String hh_setup_read = "" ;
      public String ac_setup_read( Args args )throws Exception {
         if( _setupFileName == null )
            throw new
            IllegalArgumentException( "Setupfile not defined" ) ;

         try{
            execSetupFile( _setupFile ) ;
         }catch(Exception ee){
            throw new
            Exception( "Problem in setupFile : "+ee.getMessage());
         }
         return "" ;

      }
      public String hh_setup_write = "" ;
      public String ac_setup_write( Args args )throws Exception {
         if( _setupMode != SETUP_WRITE )
            throw new
            IllegalArgumentException("Setupfile not in write mode" ) ;

         File tmpFile = new File( _setupFile.getParent() , "$-"+_setupFile.getName() ) ;
         PrintWriter pw = new PrintWriter( new FileWriter( tmpFile ) ) ;
         try{
            createSetup( pw ) ;
         }catch(Exception ee ){
            throw ee ;
         }finally{
            try{ pw.close() ; }catch(Exception eee){}
         }
         if( ! tmpFile.renameTo( _setupFile ) )
           throw new
           IOException("Failed to replace setupFile" ) ;

         return "" ;

      }
      private synchronized NodeInfo getInfo( String nodeName , boolean create ){
         NodeInfo info = _nodeDb.get(nodeName) ;
         if( ( info != null ) || ! create )return info ;
         _nodeDb.put( nodeName , info = new NodeInfo( nodeName ) ) ;
         return info ;
      }
      public String hh_define = "<domainName>" ;
      public String ac_define_$_1( Args args ){
         getInfo( args.argv(0) , true ) ;
         return "" ;
      }
      public String hh_undefine = "<domainName>" ;
      public String ac_undefine_$_1( Args args ){
          String nodeName = args.argv(0) ;
          _nodeDb.remove( nodeName ) ;
          Iterator i = _nodeDb.values().iterator() ;
          while( i.hasNext() )   ((NodeInfo)i.next()).remove( nodeName ) ;
          return "" ;
      }
      public String hh_nodefaultroute = "<sourceDomainName>" ;
      public String ac_nodefaultroute_$_1( Args args ){
         NodeInfo info = getInfo( args.argv(0) , false ) ;
         if( info == null )return "";
          info.setDefault( null ) ;
          return "" ;
      }
      public String hh_defaultroute = "<sourceDomainName> <destinationDomainName>" ;
      public String ac_defaultroute_$_2( Args args ){
          getInfo( args.argv(1) , true ) ;
          getInfo( args.argv(0) , true ).setDefault( args.argv(1) ) ;
          return "" ;
      }
      public String hh_connect = "<sourceDomainName> <destinationDomainName>" ;
      public String ac_connect_$_2( Args args ){
          NodeInfo dest = getInfo( args.argv(1) , true ) ;
          dest.setListen(true);
          getInfo( args.argv(0) , true ).add( args.argv(1) )  ;
          return "" ;
      }
      public String hh_disconnect = "<sourceDomainName> <destinationDomainName>" ;
      public String ac_disconnect_$_2( Args args ){
         NodeInfo info = getInfo( args.argv(0) , false ) ;
         if( info == null )return "";
         info.remove( args.argv(1) ) ;
         return "" ;

      }
      public String hh_listen = "<listenDomainName> [...] [-port=<portNumber>] [-security=<security>]" ;
      public String ac_listen_$_1_99( Args args ){
         int port = 0 ;
         String portString = args.getOpt("port") ;
         if( portString != null )port = Integer.parseInt(portString);
         String secString = args.getOpt("security");

         for( int i = 0 ; i < args.argc() ; i++ ){
             NodeInfo info = getInfo( args.argv(i) , true ) ;
             info.setListen(true) ;
             if( port > 0 )info.setListenPort( port ) ;
             if( ( secString != null      ) &&
                 ( secString.length() > 0 ) &&
                 ! secString.equalsIgnoreCase("none") )info.setSecurity( secString ) ;
         }
         return "" ;
      }
      public String hh_unlisten = "<listenDomainName> [...]" ;
      public String ac_unlisten_$_1_99( Args args ){
         for( int i = 0 ; i < args.argc() ; i++ ){
             NodeInfo info = getInfo( args.argv(i) , false ) ;
             if( info == null )continue ;
             info.setListen(false) ;
         }
         return "" ;
      }
      public String hh_ls_setup = "" ;
      public String ac_ls_setup( Args args ){
         StringWriter sw = new StringWriter() ;
         PrintWriter pw = new PrintWriter( sw ) ;
         createSetup( pw ) ;
         pw.flush() ;
         sw.flush() ;
         return sw.getBuffer().toString() ;
      }
      public String hh_ls_node = "[<domainName>]" ;
      public String ac_ls_node_$_0_1( Args args ){
         if( args.argc() == 0 ){
            Iterator i = _nodeDb.values().iterator() ;
            StringBuffer sb = new StringBuffer() ;
            while( i.hasNext() )sb.append( i.next().toString() ).append("\n") ;
            return sb.toString() ;
         }else{
             NodeInfo info = getInfo(args.argv(0),false);
             if( info == null )
                 throw new
                 IllegalArgumentException( "Node not found : "+args.argv(0));
             return info.toString() ;
         }
      }
      public String hh_set_address = "<domainname> <address>" ;
      public String ac_set_address_$_2( Args args ){
         NodeInfo info = getInfo(args.argv(0),false) ;
         if( info == null )
           throw new
           IllegalArgumentException( "Domain not defined : "+args.argv(0));

         if( ! info.mustListen() )
           throw new
           IllegalArgumentException( "Domain won't listen : "+args.argv(0));

         info.setAddress( args.argv(1) ) ;
         try { savePersistentMap() ; }catch(Exception eee){}
         return info.toString() ;
      }
      public String hh_unset_address = "<domainname>" ;
      public String ac_unset_address_$_1( Args args ){
         NodeInfo info = getInfo(args.argv(0),false) ;
         if( info == null )
           throw new
           IllegalArgumentException( "Domain not defined : "+args.argv(0));

         info.setAddress( null ) ;
         try { savePersistentMap() ; }catch(Exception eee){}
         return info.toString() ;
      }
      public String hh_clear_server = "" ;
      public String ac_clear_server( Args args ){
         _nodeDb.clear() ;
         return "" ;
      }
      public String hh_whatToDo = "<domainName>" ;
      public String ac_whatToDo_$_1( Args args ){
         NodeInfo info = getInfo( args.argv(0) , false ) ;
         if( info == null ){
             if( _strict || ( ( info = getInfo( "*" , false ) ) == null ) )
                throw new
                IllegalArgumentException( "Domain not defined : "+args.argv(0) );

         }
         String tmp = null ;
         String serial = ( tmp = args.getOpt("serial") ) != null ?
                         ( "-serial="+tmp ) : "" ;
         return "do "+serial+" "+info.toWhatToDoReply(true) ;
      }
      public String hh_whereIs = "<domainName>" ;
      public String ac_whereIs_$_1( Args args ){
         NodeInfo info = getInfo( args.argv(0) , false ) ;
         if( info == null )
             throw new
             IllegalArgumentException( "Domain not defined : "+args.argv(0) );
         String tmp = null ;
         String serial = ( tmp = args.getOpt("serial") ) != null ?
                         ( "-serial="+tmp ) : "" ;

         StringBuffer sb = new StringBuffer() ;
         sb.append("location ").append(serial).append(" ").append(info.getDomainName()) ;
         String out = info.getAddress() ;
         sb.append(" ").append( out == null ? "none" : out ) ;
         out = info.getSecurity() ;
         if( out != null )sb.append(" -security=\"").append(out).append("\""); ;

         return sb.toString() ;
      }
      public String hh_listeningOn = "<domainName> <address>" ;
      public String ac_listeningOn_$_2( Args args ){
         String nodeName = args.argv(0);
         NodeInfo info = getInfo( nodeName , false ) ;
         if(  info == null ){
            if( _strict )
              throw new
              IllegalArgumentException( "Domain not defined : "+nodeName );

            _nodeDb.put( nodeName , info = new NodeInfo( nodeName , false ) ) ;
         }
         info.setAddress( args.argv(1).equals("none") ? null : args.argv(1) ) ;
         try { savePersistentMap() ; }catch(Exception eee){}
         String tmp = null ;
         String serial = ( tmp = args.getOpt("serial") ) != null ?
                         ( "-serial="+tmp ) : "" ;
         return "listenOn "+serial+
                " "+info.getDomainName()+
                " "+( info.getAddress() == null ? "none" : info.getAddress() ) ;
      }

       /**
        * Shutdown the server. Notice that the method will not wait
        * for the worker thread to shut down.
        */
       public void shutdown()
       {
           _worker.interrupt();
           _socket.close();
       }
   }

   private class LocationManagerHandler implements Runnable {

      private DatagramSocket _socket = null ;
      private Map<Integer, StringBuffer>        _map      = new HashMap<Integer, StringBuffer>() ;
      private int            _serial   = 0 ;
      private InetAddress    _address  = null ;
      private int            _port     = 0 ;
      private Thread         _thread   = null ;

      private int _requestsSent     = 0 ;
      private int _repliesReceived  = 0 ;

      /**
       * Create a client listening on the supplied UDP port
       * @param localPort UDP port number to which the client will bind or 0
       *        for a random port.
       * @param address location of the server
       * @param port port number of the server
       * @throws SocketException if a UDP socket couldn't be created
       */
      private LocationManagerHandler(int localPort, InetAddress address,
                                     int port) throws SocketException
      {
          _port    = port ;
          _socket  = new DatagramSocket(localPort);
          _address = address ;
          _thread  = _nucleus.newThread( this , "LocationManagerHandler" ) ;
      }

      public void start(){
         _thread.start() ;
      }
      public int getRequestsSent(){ return _requestsSent ; }
      public int getRepliesReceived(){ return _repliesReceived ; }

       @Override
       public void run()
       {
         DatagramPacket packet = null;
         while (!Thread.currentThread().isInterrupted()) {
            try {
               packet = new DatagramPacket(new byte[1024], 1024);

               _socket.receive(packet);

               byte [] data = packet.getData();
               int packLen  = packet.getLength();

               if ((data == null) || (packLen == 0)) {
                   _log.warn("Zero packet received");
                   continue;
               }

               Args   a   = new Args(new String(data, 0, packLen));
               String tmp = a.getOpt("serial");
               if (tmp == null) {
                   _log.warn("Packet didn't provide a serial number");
                   continue;
               }

               Integer      s = Integer.valueOf(tmp);
               StringBuffer b = _map.get(s);
               if (b == null) {
                   _log.warn("Not waiting for " + s);
                   continue;
               }

               _log.info("Reasonable reply arrived (" + s + ") : " + b);

               synchronized (b) {
                  b.append(a.toString());
                  b.notifyAll();
               }
            } catch (InterruptedIOException e) {
                Thread.currentThread().interrupt();
            } catch (SocketException e) {
                if (!Thread.currentThread().isInterrupted()) {
                    _log.warn("Receiver socket problem : " + e.getMessage());
                }
            } catch (IOException e) {
                _log.warn("Receiver IO problem : " + e.getMessage());
            }
         }
         _log.info("Receiver thread finished");
      }

      private String askServer( String message , long waitTime )
          throws IOException, InterruptedException
       {
         _requestsSent ++ ;

         int serial ;
         synchronized( this ){ serial = (_serial++) ; }

         byte [] data = ( message+" -serial="+serial ).getBytes() ;

         StringBuffer   b      = new StringBuffer() ;
         DatagramPacket packet = null ;

         Integer s      = Integer.valueOf( serial ) ;
         long    rest   = waitTime ;
         long    start  = System.currentTimeMillis() ;
         long    now    = 0 ;


         _log.info( "Sending to "+_address+":"+_port+" : "+new String(data,0,data.length));

         synchronized(  b ){

            packet = new DatagramPacket( data , data.length , _address , _port    ) ;
            _map.put( s , b ) ;
            _socket.send( packet ) ;
            while( rest > 0 ){
               b.wait( rest ) ;
               if( b.length() > 0 ){
                  _repliesReceived++ ;
                  _map.remove( s ) ;
                  return b.toString() ;
               }
               now = System.currentTimeMillis() ;
               rest -= ( now - start ) ;
               start = now ;
            }
            _map.remove(s);
         }
         throw new IOException( "Request timed out" ) ;
      }

       /**
        * Shutdown the client. Notice that the method will not wait
        * for the worker thread to shut down.
        */
       public void shutdown()
       {
           _thread.interrupt();
           _socket.close();
       }

   }
   public class Client implements Runnable {

      private Thread         _receiver   = null;
      private Thread         _whatToDo   = null;
      private String         _toDo       = null ;
      private String         _registered = null ;
      private int            _state      = 0 ;
      private int _requestsReceived = 0 ;
      private int _repliesSent      = 0 ;
      private int _totalExceptions  = 0 ;

      private LocationManagerHandler _lmHandler = null ;

      private Client(InetAddress address, int port, Args args)
              throws SocketException
      {
         addCommandListener(this);

         String portOption = args.getOption("clientPort");
         portOption = portOption == null ? "random" : portOption;
         int clientPort = portOption.equals("random") ? 0 : Integer.parseInt(portOption);

         _lmHandler = new LocationManagerHandler(clientPort, address, port);
         _lmHandler.start() ;

         if( !args.hasOption("noboot") ){
           _whatToDo = _nucleus.newThread(this,"WhatToDo");
           _whatToDo.start() ;
         }
      }

      public void getInfo( PrintWriter pw ){
         pw.println( "            ToDo : "+(_state>-1?("Still Busy ("+_state+")"):_toDo));
         pw.println( "      Registered : "+(_registered==null?"no":_registered) ) ;
         pw.println( "RequestsReceived : "+_requestsReceived ) ;
         pw.println( "    RequestsSent : "+_lmHandler.getRequestsSent() ) ;
         pw.println( " RepliesReceived : "+_lmHandler.getRepliesReceived() ) ;
         pw.println( "     RepliesSent : "+_repliesSent) ;
         pw.println( "     Exceptions  : "+_totalExceptions) ;
      }

      @Override
      public String toString(){
         return ""+(_state>-1?("Client<init>("+_state+")"):"ClientReady") ;
      }
      private class BackgroundServerRequest implements Runnable {

         private String      _request = null ;
         private CellMessage _message = null ;

         private BackgroundServerRequest( String request , CellMessage message ){
            _request = request ;
            _message = message ;
         }
         @Override
         public void run(){
           try{

              String reply = _lmHandler.askServer( _request , 4000 ) ;

              _message.setMessageObject( reply ) ;
              _message.revertDirection() ;
              sendMessage(_message);

              _repliesSent++;

           }catch(Exception ee){
              _log.warn("Problem in 'whereIs' request : "+ee ) ;
              _totalExceptions ++ ;
           }
         }

      }
      public String ac_where_is_$_1( Args args ){

         _requestsReceived++ ;

         String domainName = args.argv(0) ;

         _nucleus.newThread(
              new BackgroundServerRequest( "whereIs "+domainName , getThisMessage() ) ,
              "where-is"   ).start()  ;

         return null ;
      }
      //
      //
      //  create dmg.cells.services.LocationManager lm "11111"
      //
      //  create dmg.cells.network.LocationMgrTunnel connect "dCache lm"
      //
      //  create dmg.cells.services.login.LoginManager listen
      //                    "0 dmg.cells.network.LocationMgrTunnel -prot=raw -lm=lm"
      //
      public String ac_listening_on_$_2( Args args ){

         CellMessage   msg = getThisMessage() ;
         String portString = args.argv(1) ;

         try{
             _registered  = InetAddress.getLocalHost().getHostName()+":"+portString ;
         }catch( java.net.UnknownHostException uhe ){
             _log.warn("Couldn't resolve hostname : "+uhe);
             return null ;
         }

         String request = "listeningOn "+getCellDomainName()+" "+_registered ;

         _requestsReceived++ ;

         _nucleus.newThread( new BackgroundServerRequest( request , msg ) ).start() ;

         return null ;
      }
      private void startListener( int port , String securityContext ) throws Exception {
         String cellName  = "l*" ;
         String inetClass = "dmg.cells.services.login.LoginManager" ;
         String cellClass = "dmg.cells.network.LocationMgrTunnel" ;
         String protocol  = null ;
         if( ( securityContext          == null ) ||
             ( securityContext.length() == 0    ) ||
             ( securityContext.equalsIgnoreCase("none") ) ){

            protocol = "-prot=raw" ;

         }else if( securityContext.equalsIgnoreCase("ssh") ||
                   securityContext.equalsIgnoreCase("ssh1")    ){

            protocol = "-prot=ssh -auth=dmg.cells.services.login.SshSAuth_A" ;

         }else{
            protocol = securityContext ;
         }
         String cellArgs  = ""+port+" "+cellClass+" "+protocol+" -lm="+getCellName();
         _log.info(" LocationManager starting acceptor with "+cellArgs ) ;
         Cell c = _nucleus.createNewCell( inetClass , cellName , cellArgs , true ) ;
         _log.info( "Created : "+c ) ;
         return ;
      }

       private void startConnector(final String remoteDomain)
           throws Exception
       {
         String cellName  = "c-"+remoteDomain+"*";
         String cellClass = "dmg.cells.network.LocationManagerConnector";

         String clientKey = _args.getOpt("clientKey") ;
                clientKey = ( clientKey != null ) && ( clientKey.length() > 0 ) ?
                            ("-clientKey="+clientKey ) : "" ;
         String clientName = _args.getOpt("clientUserName") ;
                clientName = ( clientName != null ) && ( clientName.length() > 0 ) ?
                            ("-clientUserName="+clientName ) : "" ;

         String cellArgs =
             "-domain=" + remoteDomain + " "
             + "-lm=" + getCellName() + " "
             + clientKey + " "
             + clientName;

         _log.info("LocationManager starting connector with " + cellArgs);
         Cell c = _nucleus.createNewCell(cellClass, cellName, cellArgs, true);
         _log.info("Created : " + c);
       }

      private void setDefaultRoute( String domain ) throws Exception {
          _nucleus.routeAdd( new CellRoute( null ,  "*@"+domain , CellRoute.DEFAULT ) ) ;
      }
      @Override
      public void run(){
         if( Thread.currentThread() == _whatToDo )runWhatToDo() ;
      }
      /**
        * loop until it gets a reasonable 'what to do' list.
        */
      private void runWhatToDo(){

         String request = "whatToDo "+getCellDomainName() ;

         while( true ){

            _state ++ ;

            try{

               String reply = _lmHandler.askServer( request , 5000 ) ;
               _log.info( "whatToDo got : "+reply ) ;

               Args args = new Args( reply ) ;

               if( args.argc() < 2 )
                  throw new
                  IllegalArgumentException( "No enough arg. : "+reply ) ;

               if( ( ! args.argv(0).equals("do" ) ) ||
                   ( ! ( args.argv(1).equals(getCellDomainName()) ||
                         args.argv(1).equals("*")                   ) ) )
                  throw new
                  IllegalArgumentException("Not a 'do' or not for us : "+reply ) ;

               if( args.argc() == 2 ){
                  _log.info("Nothing to do for us");
                  return ;
               }

               executeToDoList( args ) ;

               _toDo  = reply ;
               _state = -1 ;

               return ;

            }catch(InterruptedException ie ){
               _log.warn( _toDo = "whatToDo : interrupted" ) ;
               break ;
            }catch(InterruptedIOException ie ){
               _log.warn( _toDo = "whatToDo : interrupted(io)" ) ;
               break ;
            }catch(Exception ee ){
               _log.warn(_toDo = "whatToDo : exception : "+ee ) ;
            }
            try{
               Thread.sleep(10000) ;
            }catch(InterruptedException iie ){
               _log.warn(_toDo = "whatToDo : interrupted sleep") ;
               break ;
            }
         }
         _log.info( "whatToDo finished" ) ;

      }
      /**
        *  Gets the reply from the 'server' and can
        *  i) create a connector
        *  ii) listens to a given port
        * iii) sets a default route
        *
        *  or all of it.
        */
      private void executeToDoList( Args args ) throws Exception {
         for( int i = 2 ; i < args.argc() ; i++ ){

            String arg = args.argv(i) ;

            try{
               //
               // expected formats
               //   l:[<portNumber>]:[<securityContext>]
               //   c:<DomainName>
               //   d:<DomainName>
               //
               if( arg.startsWith("l") ){
                  int port = 0 ;
                  StringTokenizer st = new StringTokenizer(arg,":");
                  //
                  // get rid of the 'l'
                  //
                  st.nextToken() ;
                  //
                  // get the port if availble
                  //
                  if( st.hasMoreTokens() ){
                     String tmp = st.nextToken() ;
                     if( tmp.length() > 0 )
                     try{
                         port = Integer.parseInt(tmp);
                     }catch(Exception e ){
                         _log.warn("Got illegal port numnber <"+arg+">, using random");
                     }
                  }
                  //
                  // get the security context
                  //
                  String securityContext = null ;
                  if( st.hasMoreTokens() )securityContext = st.nextToken() ;

                  startListener( port , securityContext ) ;

               }else if( ( arg.length() > 2 ) &&
                          arg.startsWith("c:")    ){

                  startConnector(arg.substring(2)) ;

               }else if( ( arg.length() > 2 ) &&
                          arg.startsWith("d:")    ){

                  setDefaultRoute(arg.substring(2)) ;

               }
            }catch(InterruptedIOException ioee ){
               throw ioee ;
            }catch(InterruptedException iee ){
               throw iee ;
            }catch(Exception ee ){
               _log.warn("Command >"+arg+"< received : "+ee ) ;
            }
         }

      }

       /**
        * Shutdown the client. Notice that the method will not wait
        * for the worker thread to shut down.
        */
       public void shutdown()
       {
           _lmHandler.shutdown();
       }

   }
   /**
     *   Usage : ... [<host>] <port> -noclient [-clientPort=<UDP port number> | random]
     *   Server Options : -strict=[yes|no] -perm=<helpFilename> -setup=<setupFile>
     *
     */
   public LocationManager( String name , String args )throws Exception {
       super( name , args , false ) ;
       _args      = getArgs() ;
       _nucleus   = getNucleus() ;
       String tmp = null ;
       try{
           int    port = 0 ;
           InetAddress host = null ;
           if( _args.argc() < 1 )
              throw new
              IllegalArgumentException("Usage : ... [<host>] <port> [-noclient] [-clientPort=<UDP port number> | 'random']") ;

           if( _args.argc() == 1 ){
              //
              // we are a server and a client
              //
              port = Integer.parseInt( _args.argv(0) );
              host = InetAddress.getByName("localhost") ;
              _server = new Server( port , _args ) ;
              _log.info("Server Setup Done") ;
           }else{
              port = Integer.parseInt( _args.argv(1) );
              host = InetAddress.getByName( _args.argv(0) ) ;
           }
           if( !_args.hasOption("noclient") ){
              _client = new Client( host , port , _args ) ;
              _log.info("Client started");
           }
       }catch(Exception ee){
           _log.warn(ee.toString(), ee) ;
           start() ;
           kill() ;
           throw ee;
       }
       start() ;
   }

   @Override
   public void getInfo( PrintWriter pw ){
      if( _client != null ){
        pw.println( "Client\n--------") ;
        _client.getInfo( pw ) ;
      }
      if( _server != null ){
        pw.println( "Server\n--------") ;
        _server.getInfo( pw ) ;
      }
      return ;
   }

    @Override
    public void cleanUp()
    {
        if (_server != null)
            _server.shutdown();
        if (_client != null)
            _client.shutdown();
    }

   @Override
   public String toString(){
      StringBuffer sb = new StringBuffer() ;
      if( _client != null )
         sb.append(_client.toString()).
            append(_server!=null?";":"");
      if( _server != null )sb.append(_server.toString()) ;
      return sb.toString();
   }

   static class XXClient {
       XXClient( InetAddress address , int port , String message )throws Exception {
           byte [] data = message.getBytes() ;
           DatagramPacket packet =
              new DatagramPacket( data , data.length , address , port ) ;

           DatagramSocket socket = new DatagramSocket() ;

           socket.send( packet ) ;
           packet = new DatagramPacket( new byte[1024] , 1024 ) ;
           socket.receive( packet ) ;
           data = packet.getData() ;
           System.out.println(new String(data,0,data.length) );
       }
   }
   public static void main(String [] args )throws Exception {
       if( args.length < 3 )
          throw new
          IllegalArgumentException("Usage : ... <host> <port> <message>" ) ;
       InetAddress address = InetAddress.getByName( args[0] ) ;
       int         port    = Integer.parseInt( args[1] ) ;
       String      message = args[2] ;

       new XXClient( address , port , message ) ;
       System.exit(0);
   }
}
