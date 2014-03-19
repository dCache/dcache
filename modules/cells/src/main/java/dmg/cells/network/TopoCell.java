package dmg.cells.network ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellTunnelInfo;

import org.dcache.util.Args;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class TopoCell extends CellAdapter implements Runnable  {

   private final static Logger _log = LoggerFactory.getLogger(TopoCell.class);

   private final Thread      _worker ;
   private final Object      _infoLock     = new Object() ;
   private long        _waitTime     = 300*1000 ;
   private int         _requestCount;
   private CellDomainNode [] _infoMap;

   public TopoCell( String cellName , String cellArgs ){
      super( cellName , cellArgs , true ) ;

      String wait = getArgs().getOpt("update") ;

      try{
         _waitTime = Long.parseLong( wait ) * 1000L ;
      }catch(NumberFormatException ee){ /* bad values ignored */}
      _log.info("Update set to "+_waitTime+" millis");
      _worker = getNucleus().newThread( this ) ;
      _worker.start() ;
   }

   @Override
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
               _log.info( "Topology Thread was interrupted" ) ;
               break ;
           }catch(Exception e){
               _log.warn( "Exception in Loop : "+e ) ;
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

   public static final String hh_ls = "ls [-l]" ;
   public String ac_ls_$_0( Args args ){
       boolean detail = args.hasOption("l") ;


       CellDomainNode [] info = getInfoMap() ;
       if( info == null ) {
           return "";
       }
       StringBuilder sb = new StringBuilder() ;
       for (CellDomainNode node : info) {
           sb.append(node.getName());
           if (detail) {
               sb.append(" ").append(node.getAddress());
           }
           sb.append("\n");
       }
       return sb.toString() ;
   }

   private CellDomainNode [] getTopologyMap() throws Exception {

       List<CellDomainNode>   vec  = new Vector<>() ;
       Map<String, CellDomainNode>  hash = new HashMap<>() ;

       CellDomainNode node = new CellDomainNode(
                 getCellDomainName() ,
                 "System@"+getCellDomainName() ) ;
       vec.add( node ) ;

       for( int i= 0 ; i < vec.size() ; i++ ){
          node           = vec.get(i) ;
          String name    = node.getName() ;
          String address = node.getAddress() ;

          if( hash.get( name ) != null ) {
              continue;
          }
          hash.put( name , node ) ;

          setStatus( "Request to : "+address ) ;
          CellTunnelInfo [] infos = getCTI( address ) ;
          if( infos == null ){
             setStatus( "No Answer from : "+address ) ;
             continue ;
          }
          setStatus( "Answer Ok : "+address ) ;
          Set<CellTunnelInfo> acceptedTunnels = new HashSet<>();
          String domain ;
           for (CellTunnelInfo info : infos) {
               try {
                   domain = info.getRemoteCellDomainInfo().
                           getCellDomainName();
               } catch (Exception npe) {
                   _log.warn("Exception in domain info : " + info.toString());
                   continue;
               }
               node = new CellDomainNode(domain, address + ":System@" + domain);
               vec.add(node);
               acceptedTunnels.add(info);
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
        CellDomainNode[] infos = getInfoMap();
        if (infos != null) {
            for (CellDomainNode anInfo : infos) {
                pw.print(anInfo.toString());
            }
        } else {
            pw.println("    No Information yet");
        }
    }

   private synchronized CellTunnelInfo [] getCTI( String cellPath )
           throws Exception {

      CellMessage msg = new CellMessage(
                                new CellPath(cellPath) ,
                                "getcelltunnelinfos"      ) ;

       msg = getNucleus().sendAndWait(msg, _waitTime);
      if( msg == null ){
         setStatus( "Timeout from : "+cellPath ) ;
         return null ;
      }
      return (CellTunnelInfo [] )msg.getMessageObject() ;
   }

   public Object ac_gettopomap( Args args ){ return getInfoMap() ; }
   public static final String hh_set_updatetime = "<seconds>" ;
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
        _log.info(st);
    }

}
