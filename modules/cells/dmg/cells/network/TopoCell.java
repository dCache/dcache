package dmg.cells.network ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  java.util.* ;
import  java.io.* ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class TopoCell extends CellAdapter implements Runnable  {

   private final Thread      _worker ;
   private final Object      _infoLock     = new Object() ;
   private long        _waitTime     = 300*1000 ;
   private int         _requestCount = 0 ;
   private CellDomainNode [] _infoMap = null ;

   public TopoCell( String cellName , String cellArgs ){
      super( cellName , cellArgs , true ) ;

      String wait = getArgs().getOpt("update") ;

      try{
         _waitTime = Long.parseLong( wait ) * 1000L ;
      }catch(NumberFormatException ee){ /* bad values ignored */}
      say("Update set to "+_waitTime+" millis");
      _worker = getNucleus().newThread( this ) ;
      _worker.start() ;
   }

   public void run(){
     if( Thread.currentThread() == _worker ){
        while( true ){
           try{
               Thread.sleep(_waitTime);
               setStatus( "Starting auto topology detector : "+_requestCount ) ;
               CellDomainNode [] info = getTopologyMap() ;
               setStatus( "Auto Topology Detector Ready : "+_requestCount ) ;
               synchronized( _infoLock ){ _infoMap = info ; }
               _requestCount++;
           }catch(InterruptedException ie){
               say( "Topology Thread was interrupted" ) ;
               break ;
           }catch(Exception e){
               esay( "Exception in Loop : "+e ) ;
           }
       }
     }
   }

    CellDomainNode[] getInfoMap() {
        CellDomainNode[] info;
        synchronized (_infoLock) {
            info = _infoMap;
        }
        return info;
    }

   public String hh_ls = "ls [-l]" ;
   public String ac_ls_$_0( Args args ){
       boolean detail = args.getOpt("l") != null ;


       CellDomainNode [] info = getInfoMap() ;
       if( info == null )return "";
       StringBuffer sb = new StringBuffer() ;
           for(  int i = 0 , n = info.length ; i < n ; i++ ){
               sb.append(info[i].getName()) ;
               if( detail )sb.append(" ").append(info[i].getAddress()) ;
               sb.append("\n");
           }
       return sb.toString() ;
   }

   private CellDomainNode [] getTopologyMap() throws Exception {

       List<CellDomainNode>   vec  = new Vector<CellDomainNode>() ;
       Map<String, CellDomainNode>  hash = new HashMap<String, CellDomainNode>() ;

       CellDomainNode node = new CellDomainNode(
                 getCellDomainName() ,
                 "System@"+getCellDomainName() ) ;
       vec.add( node ) ;

       for( int i= 0 ; i < vec.size() ; i++ ){
          node           = vec.get(i) ;
          String name    = node.getName() ;
          String address = node.getAddress() ;

          if( hash.get( name ) != null )continue ;
          hash.put( name , node ) ;

          setStatus( "Request to : "+address ) ;
          CellTunnelInfo [] info = getCTI( address ) ;
          if( info == null ){
             setStatus( "No Answer from : "+address ) ;
             continue ;
          }
          setStatus( "Answer Ok : "+address ) ;
          Set<CellTunnelInfo> acceptedTunnels = new HashSet<CellTunnelInfo>();
          String domain ;
          for( int j = 0 ; j < info.length ; j++ ){
            try{
              domain = info[j].getRemoteCellDomainInfo().
                                      getCellDomainName() ;
            }catch( Exception npe ){
              esay( "Exception in domain info : "+info[j].toString() );
              continue ;
            }
            node = new CellDomainNode( domain , address+":System@"+domain) ;
            vec.add( node ) ;
            acceptedTunnels.add( info[j]);
          }

          // Make sure we only add the links that haven't caused a problem.
          node.setLinks( acceptedTunnels.toArray( new CellTunnelInfo [acceptedTunnels.size()]));
       }
       CellDomainNode [] nodes = hash.values().toArray(new CellDomainNode[hash.size()] ) ;

       return nodes ;
   }

   @Override
    public String toString() {
        return "Run Count : " + _requestCount;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println("   Topology Cell");
        pw.println(" Request Counter : " + _requestCount);
        pw.println(" Topology Information  : ");
        CellDomainNode[] info = getInfoMap();
        if (info != null) {
            for (int i = 0; i < info.length; i++)
                pw.print(info[i].toString());
        } else {
            pw.println("    No Information yet");
        }
    }

   private synchronized CellTunnelInfo [] getCTI( String cellPath )
           throws Exception {

      CellMessage msg = new CellMessage(
                                new CellPath(cellPath) ,
                                "getcelltunnelinfos"      ) ;

      msg = sendAndWait( msg , _waitTime ) ;
      if( msg == null ){
         setStatus( "Timeout from : "+cellPath ) ;
         return null ;
      }
      return (CellTunnelInfo [] )msg.getMessageObject() ;
   }

   public Object ac_gettopomap( Args args ){ return getInfoMap() ; }
   public String hh_set_updatetime = "<seconds>" ;
   public String ac_set_updatetime_$_1( Args args ){
      _waitTime = (long)Integer.parseInt( args.argv(0) ) * 1000 ;
      return "Refresh rate set to "+_waitTime+" mseconds" ;
   }
   public String ac_show_updatetime( Args args ){
      return "Refresh rate set to "+_waitTime+" mseconds" ;
   }

   @Override
    public void cleanUp() {
        _worker.interrupt();
    }

    private void setStatus(String st) {
        say(st);
    }

}
