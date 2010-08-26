//
// $Id: CodeVersioningCell.java,v 1.2 2002-03-19 08:02:52 cvs Exp $
//
package dmg.cells.services.codeVersioning ;

import   dmg.cells.nucleus.* ;
import   dmg.util.* ;
import   dmg.cells.network.* ;

import java.util.* ;
import java.io.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Mar 2002
  */
public class CodeVersioningCell extends CellAdapter  {

   private CellNucleus    _nucleus ;
   private Args           _args ;
   private HashMap        _receiverMap    = new HashMap() ;
   private HashMap        _senderHash     = new HashMap() ;
   private HashSet        _destinationSet = new HashSet() ;
   private File           _base           = null ;
   private Object         _messageLock    = new Object() ;
   private HashMap        _messages       = new HashMap() ;
   
   private static int __idCounter = 100 ;
   private static synchronized int getNextId(){ return __idCounter++ ; }
   
   public String hh_dh_add = "<dest> [<dest>[...]]" ;
   public synchronized String ac_dh_add_$_1_100(Args args ){
      for( int i = 0 ; i < args.argc() ; i++ )
         _destinationSet.add(args.argv(i));
      return "" ;
   }
   public String hh_dh_remove = "<dest> [<dest>[...]]" ;
   public synchronized String ac_dh_remove_$_1_100( Args args ){
      for( int i = 0 ; i < args.argc() ; i++ )
         _destinationSet.remove(args.argv(i));
      return "" ;
   }
   public String hh_dh_list = "" ;
   public synchronized String ac_dh_list( Args args ){
      StringBuffer sb = new StringBuffer();
      Iterator i = _destinationSet.iterator() ;
      while( i.hasNext() )sb.append(i.next().toString()).append("\n") ;
      return sb.toString() ;
   }
   public String hh_dh_clear = "" ;
   public String ac_dh_clear( Args args ){
      _destinationSet.clear() ;
      return "" ;
   }
   public String hh_dh_scan = "[<topologyCell>]" ;
   public String ac_dh_scan_$_0_1( Args args )throws Exception {
      String dest = args.argc() == 0 ? "topo" : args.argv(0) ;
      
      CellMessage msg = new CellMessage( new CellPath(dest) ,
                                        "gettopomap" ) ;
   
      msg = _nucleus.sendAndWait( msg , 20000 ) ;
      
      if( msg == null )
         throw new
         Exception("Request timed out") ;
         
      Object o = msg.getMessageObject() ;
      
      if( ! ( o instanceof CellDomainNode [] ) )
        throw new
        Exception("Illegal reply : "+o.getClass().getName() ) ;
        
      CellDomainNode [] nodes = (CellDomainNode [])o;
      for( int i = 0 ; i < nodes.length ; i++ ){
         _destinationSet.add( "cvc@"+nodes[i].getName() ) ;
      }
      return ""+nodes.length+" nodes found" ;
   }
   private String _defaultSender  = null ;
   public String hh_new = "<senderName>" ;
   public synchronized String ac_new( Args args ){
      String senderName = args.argv(0);
      _senderHash.clear() ;
      if( _senderHash.get( senderName ) != null )
         throw new
         IllegalArgumentException("Sender already exists") ;
         
      Sender s = new Sender( _destinationSet.iterator() ) ;
      _senderHash.put( _defaultSender = senderName , s ) ;
      return "" ;
   }
   public synchronized String ac_list( Args args ){
      String senderName = args.getOpt("sender") ;
      if( ( senderName == null ) && ( _defaultSender == null ) )
        throw new
        IllegalArgumentException("No sender defined" ) ;
        
      senderName = senderName == null ? _defaultSender : senderName ;
      Sender sender = (Sender)_senderHash.get(senderName);
      if( sender == null )
         throw new
         IllegalArgumentException("Sender not found : "+senderName );
         
      return sender.list() ;      
   }
   public synchronized String ac_clear( Args args ){
      String senderName = args.getOpt("sender") ;
      if( ( senderName == null ) && ( _defaultSender == null ) )
        throw new
        IllegalArgumentException("No sender defined" ) ;
        
      senderName = senderName == null ? _defaultSender : senderName ;
      Sender sender = (Sender)_senderHash.get(senderName);
      if( sender == null )
         throw new
         IllegalArgumentException("Sender not found : "+senderName );
         
      sender.clear() ; 
      return "" ;     
   }
   
   public String hh_mkdir = "<directoryName> [-timeout=t/sec]" ;
   public String ac_mkdir_$_1( Args args ){
      String senderName = args.getOpt("sender") ;
      if( ( senderName == null ) && ( _defaultSender == null ) )
        throw new
        IllegalArgumentException("No sender defined" ) ;
      senderName = senderName == null ? _defaultSender : senderName ;
      Sender sender = (Sender)_senderHash.get(senderName);
      if( sender == null )
         throw new
         IllegalArgumentException("Sender not found : "+senderName );
        
      String to   = args.getOpt("timeout") ;
      
      createObject( sender , args.argv(0) , "directory" , 
                    to == null ? 20000L : Long.parseLong(to) ) ;
                    
      return "" ;
   }
   public String hh_touch = "<filename> [-timeout=t/sec]" ;
   public String ac_touch_$_1( Args args ){
      String senderName = args.getOpt("sender") ;
      if( ( senderName == null ) && ( _defaultSender == null ) )
        throw new
        IllegalArgumentException("No sender defined" ) ;
      senderName = senderName == null ? _defaultSender : senderName ;
      Sender sender = (Sender)_senderHash.get(senderName);
      if( sender == null )
         throw new
         IllegalArgumentException("Sender not found : "+senderName );
        
      String to   = args.getOpt("timeout") ;
      
      createObject( sender , args.argv(0) , "file" , 
                    to == null ? 20000L : Long.parseLong(to) ) ;
                    
      return "" ;
   }
   public String hh_create_object = "<name> [-type=file|directory] [-timeout=t/sec]" ;  
   public String ac_create_object_$_1( final Args args )throws InterruptedException {
      String senderName = args.getOpt("sender") ;
      if( ( senderName == null ) && ( _defaultSender == null ) )
        throw new
        IllegalArgumentException("No sender defined" ) ;
        
      senderName = senderName == null ? _defaultSender : senderName ;
      Sender sender = (Sender)_senderHash.get(senderName);
      if( sender == null )
         throw new
         IllegalArgumentException("Sender not found : "+senderName );
         
      String type = args.getOpt("type") ;
      
      String to   = args.getOpt("timeout") ;
      
      createObject( sender , args.argv(0) , 
                    type == null ? "file" : type ,
                    to == null ? 20000L : Long.parseLong(to) ) ;
      
      return "" ;
   }  
   public String hh_remove = "<name> [-type=file|directory] [-timeout=t/sec]" ;  
   public String ac_remove_$_1( final Args args )throws InterruptedException {
      String senderName = args.getOpt("sender") ;
      if( ( senderName == null ) && ( _defaultSender == null ) )
        throw new
        IllegalArgumentException("No sender defined" ) ;
        
      senderName = senderName == null ? _defaultSender : senderName ;
      final Sender sender = (Sender)_senderHash.get(senderName);
      if( sender == null )
         throw new
         IllegalArgumentException("Sender not found : "+senderName );
         
      String type = args.getOpt("type") ;
      
      String to   = args.getOpt("timeout") ;
      
      removeObject( sender , args.argv(0) , 
                    type == null ? "file" : type ,
                    to == null ? 20000L : Long.parseLong(to) ) ;
      
      return "" ;
   }  
   private void createObject( final Sender sender , final String name , 
                              final String type , final long timeout     ){ 
      _nucleus.newThread(
         new Runnable(){
            public void run() {
              try{
                 sender.send( new CVCreatePacket(name,type) , timeout  ) ;
              }catch(Exception ee ){
                 esay(ee);
              }  
            }
         }
         , "command" ).start() ;
      return  ;    
   }
   private void removeObject( final Sender sender , final String name , 
                              final String type , final long timeout     ){ 
      _nucleus.newThread(
         new Runnable(){
            public void run() {
              try{
                 sender.send( new CVRemovePacket(name,type) , timeout  ) ;
              }catch(Exception ee ){
                 esay(ee);
              }  
            }
         }
         , "command" ).start() ;
      return  ;    
   }
   public String hh_copy = "<name> [-timeout=t/sec]" ;
   public String ac_copy_$_1( final Args args )throws Exception {
      String senderName = args.getOpt("sender") ;
      if( ( senderName == null ) && ( _defaultSender == null ) )
        throw new
        IllegalArgumentException("No sender defined" ) ;
        
      senderName = senderName == null ? _defaultSender : senderName ;
      final Sender sender = (Sender)_senderHash.get(senderName);
      if( sender == null )
         throw new
         IllegalArgumentException("Sender not found : "+senderName );
         
      final File localFile = new File( _base , args.argv(0) ) ;
      if( ! localFile.exists() )
         throw new
         FileNotFoundException( "File not found : "+localFile ) ;
         
      _nucleus.newThread(
         new Runnable(){
            public void run() {
              String tmp = args.getOpt("timeout") ;
              long timeout = tmp == null || tmp.equals("") ? 20000L : 1000*Long.parseLong(tmp) ;
              try{
                 sender.copyFile( localFile , args.argv(0) , timeout  ) ;
              }catch(Exception ee ){
                 esay(ee);
              }  
            }
         }
         , "command" ).start() ;
      return "" ;    
   }
   private interface Receivable {
      public void messageArrived( CellMessage message ) ;
   }
   private class Sender implements Runnable{
   
      private ArrayList _list          = new ArrayList() ;
      private Object    _listLock      = new Object() ;
      private int       _resultCounter = 0 ;
      private class Entry implements Receivable {
         private UOID   _uoid        = null ;
         private String _destination = null ;
         private String _state       = "<idle>" ;
         private int    _stateCode   = -1 ;
         private Entry( String destination ){
            _destination = destination ;
         }
         public int getResultCode(){ return _stateCode ; }
         public void messageArrived(CellMessage message ){
            Object o = message.getMessageObject() ;
            if( o instanceof CVPacket ){
               CVPacket packet = (CVPacket)o ;
               _stateCode = packet.getResultCode() ;
               _state = "Return : ["+packet.getResultCode()+"] "+
                                     packet.getResultMessage() ;
            }else{
               _state = "Unexpected reply : "+o.getClass().getName() ;
            }
//            say("messageArrived : "+o.toString() );
            synchronized( _messageLock ){
               _resultCounter -- ;
               _messageLock.notifyAll() ;
            }
         }
         public void clear(){
            _state     = "<idle>" ;
            _stateCode = -1 ;
            synchronized( _messageLock ){
               if( _uoid != null )_messages.remove(_uoid) ;
               _uoid = null ;
               
            }
         }
         public String toString(){
            return _destination+
                   "  uoid="+(_uoid==null?"<unknown>":_uoid.toString())+
                   " state="+_state ;
         }
         private void  send( CVPacket packet ){
           synchronized( _messageLock ){
              _state = "Waiting for reply of "+packet ;
              CellMessage m = new CellMessage(new CellPath(_destination) , packet ) ;
              try{
                 _nucleus.sendMessage( m ) ;
                 _uoid = m.getUOID() ;
                 _messages.put( _uoid , this ) ;
              }catch(Exception e){
                 _state = "Exception in sending : "+e.toString() ;
              }
           }
         }
      }
      
      private Sender( Iterator destinations ){
         while( destinations.hasNext() ){
            String destination = destinations.next().toString() ;
//            say("Adding "+destination ) ;
            _list.add( new Entry( destination ) ) ;
         }
      }
      private void copyFile( File name , String destinationName , long timeout ) throws Exception {
         byte [] data = new byte[4*1024] ;
         int     rc   = 0 ;
         
         BufferedInputStream in = new BufferedInputStream(new FileInputStream( name )) ;
         try{
            send( new CVCreatePacket( destinationName , "file" ) , timeout ) ;
            if( ! isOk() )
              throw new
              Exception( "Create Failed" ) ;
              
            for( int n = 0 ; ( rc = in.read( data , 0 , data.length ) ) > 0 ; n++ ){
               write( destinationName , "file" , data , rc , timeout ) ;
               if( ! isOk() )
                 throw new
                 Exception( "Write "+n+" Failed" ) ;
            }
         }finally{
            in.close() ;
         }
        
         return ;  
      }
      private void write( String name , String type , byte [] data , int size , long timeout )
              throws InterruptedException {
         send( new CVWritePacket( name , type , data , size ) , timeout ) ;            
      }
      private void send( CVPacket packet , long timeout )
         throws InterruptedException {
         
         synchronized( _messageLock ){
             _resultCounter = 0 ;
             synchronized( _listLock ){
                for( Iterator i = _list.iterator() ; i.hasNext() ; ){
                  
                   ((Entry)i.next()).send(packet) ;
                   _resultCounter ++ ;
//                   say("Sending : "+_resultCounter ) ;
                }
             }
             if( timeout <= 0  )return ;
             long finished = System.currentTimeMillis() + timeout ;
             long now = 0 ;
             while( _resultCounter > 0 ){
//                say("ResultCounter : "+_resultCounter );
                if( finished <= ( now = System.currentTimeMillis() ) )
                   throw new
                   InterruptedException("Timeout") ;
                   
                _messageLock.wait(finished-now) ;
             
             }
         }
      }
      public void run(){}
      public boolean isOk(){
          synchronized( _messageLock ){
             if( _resultCounter > 0 )return false ;
          }
          synchronized( _listLock ){
             for( Iterator i = _list.iterator() ; i.hasNext() ; ){
                Entry e = (Entry)i.next() ;
                if( e.getResultCode() != 0 )return false ;
             }
          }
          return true ;
      }
      public String list(){
          StringBuffer sb = new StringBuffer() ;
          sb.append("Listing destinations : \n");
          synchronized( _listLock ){
             for( Iterator i = _list.iterator() ; i.hasNext() ; ){
                Entry e = (Entry)i.next() ;
                sb.append(e.toString()).append("\n");
             }
          }
          return sb.toString() ;
      }
      public void clear(){
          synchronized( _listLock ){
             for( Iterator i = _list.iterator() ; i.hasNext() ; ){
                ((Entry)i.next()).clear() ;
             }
             _resultCounter = 0 ;
          }
          return ;
      }
   }
   public CodeVersioningCell( String name , String args )
          throws Exception {
      super( name , args  , false ) ;
      
      _nucleus = getNucleus() ;
      _args    = getArgs() ;
      
      try{
          if( _args.argc() < 1 )
             throw new
             IllegalArgumentException("Usage : ... <baseDirectory>") ;
             
          _base = new File( _args.argv(0) ) ;
          if( ! _base.isDirectory() )
             throw new
             IllegalArgumentException("Not a directory : "+_args.argv(0) ) ;
             
             
          start() ;
      }catch( Exception ee ){
         esay("Constructor failed : "+ee ) ;
         start() ;
         kill() ;
         throw ee ;
      }
      
   }
   public void messageArrived( CellMessage message ){
       UOID uoid = message.getLastUOID() ;
//       say("messageArrived : waiting for messagLock");
       synchronized( _messageLock ) {
         
          Receivable r = (Receivable)_messages.remove(uoid) ;
          
          if( r != null ){
//             say("messageArrived : receivable found ... "+r.toString() ) ;
             r.messageArrived( message ) ;
//             say("messageArrived : receivable delivered ... "+r.toString() ) ;
             return ;
          }
       }

       executePacket( message.getMessageObject() ) ;
       message.revertDirection() ;
       try{
          _nucleus.sendMessage( message ) ;
       }catch(Exception ee ){
          esay(ee) ;
       }
       return ;
   }
   //
   //   RECEIVER PART
   //
   private  void executePacket( Object message ){
      if( message instanceof CVPacket ){  
         CVPacket packet = (CVPacket)message ;
         try{    
            if( packet instanceof CVCreatePacket ){
                createObject( (CVCreatePacket)packet) ;
            }else if( packet instanceof CVRemovePacket ){
                removeObject( (CVRemovePacket)packet) ;
            }else if( packet instanceof CVWritePacket ){
                writeToObject( (CVWritePacket)packet) ;
            }else{
               String msg = "Unknown packet type : "+packet.getClass().getName() ;
               esay("executePacket : "+msg ) ;
            }
            return ;
         }catch(Throwable t ){
            String msg = "Unexpected event : "+t.toString() ;
            esay(msg) ;
            esay(t) ;
            packet.setResult( 666 , msg ) ;
            return ;
         }
      }else{
         String msg = "Unknown packet type : "+message.getClass().getName() ;
         esay("executePacket : "+msg ) ;
         return ;
      }
   }
   private void createObject( CVCreatePacket packet ){
      if( packet.getType().equals("file") ){
         try{
            File file = new File( _base , packet.getName() ) ;
            boolean ok = file.createNewFile() ;
            if( packet.isExclusive() && ! ok ){
               packet.setResult( 3 , "File already exists : "+packet.getName() ) ;
               return ;
            }
         }catch(IOException ee ){
            packet.setResult(4,ee.toString());
            return ;
         }
      }else if( packet.getType().equals("directory") ) {
         File file = new File( _base , packet.getName() ) ;
         boolean ok = file.mkdirs() ;
         if( ! ok ){
            packet.setResult(5,"Failed : couldn't create : "+file.toString() ) ;
            return ;
         }
      }else{
         packet.setResult(2,"Unsupported container 'type' : "+packet.getType() ) ;
         return ;
      }
      return ;
   }
   private void removeObject( CVRemovePacket packet ){
      if( packet.getType().equals("file") || packet.getType().equals("directory") ){
         try{
            File file  = new File( _base , packet.getName() ) ;
            boolean ok = file.delete() ;
            if( ! ok ){
               packet.setResult( 6 , "Object not removed : "+file.toString() ) ;
               return ;
            }
         }catch(Exception ee ){
            packet.setResult(4,ee.toString());
            return ;
         }
      }else{
         packet.setResult(2,"Unsupported container 'type' : "+packet.getType() ) ;
         return ;
      }
      return ;
   }
   private void writeToObject( CVWritePacket packet ){
      if( packet.getType().equals("file") ){
         try{
            File file  = new File( _base , packet.getName() ) ;
            boolean ok = file.exists() && file.isFile() ;
            if( ! ok ){
               packet.setResult( 6 , "Object doesn't exist or not a file : "+file.toString() ) ;
               return ;
            }
            if( packet.isAppend() ){
               FileOutputStream out = new FileOutputStream(file.getAbsolutePath(),true) ;
               try{
                  out.write( packet.getData() ) ;
               }finally{
                  out.close() ;
               }
            }else{
               RandomAccessFile out = new RandomAccessFile(file,"rw") ;
               try{
                  out.seek( packet.getOffset() ) ;
                  out.write(packet.getData() ) ;
               }finally{
                  out.close() ;
               }
            }
         }catch(IOException ee ){
            packet.setResult(4,ee.toString());
            return ;
         }
      }else{
         packet.setResult(7,"Can't write to object : "+packet.getType() ) ;
         return ;
      }
      return ;
   }
   
   
}
