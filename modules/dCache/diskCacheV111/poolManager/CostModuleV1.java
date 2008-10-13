// $Id: CostModuleV1.java,v 1.21 2007-08-01 20:19:23 tigran Exp $

package diskCacheV111.poolManager ;
import  java.util.* ;
import  java.util.regex.* ;
import  java.io.PrintWriter ;
import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  diskCacheV111.vehicles.* ;
import  diskCacheV111.pools.* ;
import diskCacheV111.pools.PoolCostInfo.NamedPoolQueueInfo;

public class CostModuleV1 implements CostModule {

   private final CellAdapter _cell;
   private final Map<String, Entry>     _hash     = new HashMap<String, Entry>() ;
   private boolean     _isActive = true ;
   private boolean     _update   = true ;
   private boolean     _magic    = true ;
   private boolean     _debug    = false ;

   private static class Entry {
       private long timestamp ;
       private PoolCostInfo _info ;
       private double _fakeCpu   = -1.0 ;
       private double _fakeSpace = -1.0 ;
       private Map<String, String> _tagMap = null;
       private Entry( PoolCostInfo info ){
          setPoolCostInfo(info) ;
       }
       private void setPoolCostInfo( PoolCostInfo info ){
          timestamp = System.currentTimeMillis();
          _info = info ;
       }
       private PoolCostInfo getPoolCostInfo(){
           return _info ;
        }
       private boolean isValid(){
          return ( System.currentTimeMillis() - timestamp ) < 5*60*1000L ;
       }
       private void setTagMap(Map<String, String> tagMap) {
           _tagMap = tagMap;
       }
       private Map<String, String> getTagMap() {
           return _tagMap;
       }
   }
   private CostCalculationEngine _costCalculationEngine = null ;
   private class CostCheck extends PoolCheckAdapter implements PoolCostCheckable  {

       private PoolCostInfo _info ;

       private static final long serialVersionUID = -77487683158664348L;

       private CostCheck( String poolName , Entry e , long filesize ){
          super(poolName,filesize);
          _info     = e.getPoolCostInfo() ;

          CostCalculatable  cost = _costCalculationEngine.getCostCalculatable( _info ) ;

          cost.recalculate( filesize ) ;

          setSpaceCost( e._fakeSpace > -1.0 ?
                        e._fakeSpace :
                        cost.getSpaceCost() ) ;
          setPerformanceCost( e._fakeCpu > -1.0 ?
                              e._fakeCpu :
                              cost.getPerformanceCost() ) ;
          setTagMap( e.getTagMap() );
       }
   }

   public CostModuleV1( CellAdapter cellAdapter ) throws Exception {
      _cell = cellAdapter ;

      String costName = _cell.getArgs().getOpt("costCalculator") ;
      costName = costName == null ? "diskCacheV111.pools.CostCalculationV5" : costName ;
      _costCalculationEngine =  new CostCalculationEngine(costName) ;
   }
   public synchronized void messageArrived( CellMessage cellMessage ){

      Object message = cellMessage.getMessageObject() ;

      //say( "messageArrived : class "+message.getClass().getName() ) ;
      //say( "messageArrived : objec "+message ) ;

      if( message instanceof PoolManagerPoolUpMessage ){

         if( ! _update )return ;

         PoolManagerPoolUpMessage msg = (PoolManagerPoolUpMessage)message ;
         String poolName = msg.getPoolName() ;
         PoolV2Mode poolMode = msg.getPoolMode();
         if (poolMode.getMode() != PoolV2Mode.DISABLED &&
             !poolMode.isDisabled(PoolV2Mode.DISABLED_STRICT) &&
             !poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD)) {
             if( msg.getPoolCostInfo() != null ){
                 Entry e = _hash.get(poolName);

                 if( e == null ){
                     e = new Entry( msg.getPoolCostInfo() );
                     _hash.put( poolName , e ) ;
                 }else{
                     e.setPoolCostInfo( msg.getPoolCostInfo() ) ;
                 }
                 e.setTagMap(msg.getTagMap());
             }
         } else {
             _hash.remove( poolName ) ;
         }
      }else if( message instanceof PoolIoFileMessage ){

         PoolIoFileMessage msg = (PoolIoFileMessage)message ;
         String poolName = msg.getPoolName();
         Entry e = _hash.get(poolName);
         if( e == null )return ;

         String requestedQueueName = msg.getIoQueueName() ;

         Map<String, NamedPoolQueueInfo> map = e.getPoolCostInfo().getExtendedMoverHash() ;

         PoolCostInfo.PoolQueueInfo queue = null ;

         if( map == null ){
            queue = e.getPoolCostInfo().getMoverQueue() ;
         }else{
            requestedQueueName = ( requestedQueueName == null ) ||
                                 ( map.get(requestedQueueName) == null ) ?
                                 e.getPoolCostInfo().getDefaultQueueName() :
                                 requestedQueueName ;

            queue = map.get(requestedQueueName) ;


         }

         int diff = ( msg.isReply() && ( msg.getReturnCode() != 0 ) ) ? -1 :
                    ( ( ! msg.isReply() )  && ( ! _magic ) )          ?  1 : 0 ;

         queue.modifyQueue( diff ) ;

         xsay( "Mover"+(requestedQueueName==null?"":("("+requestedQueueName+")")) , poolName, diff, message ) ;

      }else if( message instanceof DoorTransferFinishedMessage){

         DoorTransferFinishedMessage msg = (DoorTransferFinishedMessage)message ;
         String poolName = msg.getPoolName();
         Entry e = _hash.get(poolName);
         if( e == null )return ;

         String requestedQueueName = msg.getIoQueueName() ;

         Map<String, NamedPoolQueueInfo> map = e.getPoolCostInfo().getExtendedMoverHash() ;
         PoolCostInfo.PoolQueueInfo queue = null ;

         if( map == null ){
            queue = e.getPoolCostInfo().getMoverQueue() ;
         }else{
            requestedQueueName = ( requestedQueueName == null ) ||
                                 ( map.get(requestedQueueName) == null ) ?
                                 e.getPoolCostInfo().getDefaultQueueName() :
                                 requestedQueueName ;

            queue = map.get(requestedQueueName) ;

         }
         int diff = -1 ;

         queue.modifyQueue( diff ) ;

         xsay( "Mover"+(requestedQueueName==null?"":("("+requestedQueueName+")")), poolName, diff, message ) ;
      }else if( message instanceof PoolFetchFileMessage ){
         PoolFetchFileMessage msg = (PoolFetchFileMessage)message ;
         String poolName = msg.getPoolName();
         Entry e = _hash.get(poolName);
         if( e == null )return ;

         PoolCostInfo.PoolQueueInfo queue = e.getPoolCostInfo().getRestoreQueue() ;

         int diff = msg.isReply() ? -1 : 1 ;

         queue.modifyQueue( diff ) ;

         xsay( "Restore" , poolName, diff, message ) ;
      }else if( message instanceof PoolMgrSelectPoolMsg ){

         if( ! _magic )return ;

         PoolMgrSelectPoolMsg msg = (PoolMgrSelectPoolMsg)message ;
         if( ! msg.isReply() )return ;
         String poolName = msg.getPoolName();
         Entry e = _hash.get(poolName);
         if( e == null )return ;

         String requestedQueueName = msg.getIoQueueName() ;

         Map<String, NamedPoolQueueInfo> map = e.getPoolCostInfo().getExtendedMoverHash() ;
         PoolCostInfo.PoolQueueInfo queue = null ;

         if( map == null ){
            queue = e.getPoolCostInfo().getMoverQueue() ;
         }else{
            requestedQueueName = ( requestedQueueName == null ) ||
                                 ( map.get(requestedQueueName) == null ) ?
                                 e.getPoolCostInfo().getDefaultQueueName() :
                                 requestedQueueName ;

            queue = map.get(requestedQueueName) ;

         }

         int diff = 1 ;

         queue.modifyQueue( diff ) ;

         xsay( "Mover (magic)"+(requestedQueueName==null?"":("("+requestedQueueName+")")) , poolName, diff, message ) ;

      }else if( message instanceof Pool2PoolTransferMsg ){
         Pool2PoolTransferMsg msg = (Pool2PoolTransferMsg)message ;
         say( "Pool2PoolTransferMsg : reply="+msg.isReply());

         String sourceName      = msg.getSourcePoolName() ;
         Entry source = _hash.get(sourceName);
         if( source == null )return ;

         PoolCostInfo.PoolQueueInfo sourceQueue = source.getPoolCostInfo().getP2pQueue() ;

         String destinationName = msg.getDestinationPoolName() ;
         Entry destination = _hash.get(destinationName);
         if( destination == null )return ;

         PoolCostInfo.PoolQueueInfo destinationQueue = destination.getPoolCostInfo().getP2pClientQueue() ;

         int diff = msg.isReply() ? -1 : 1 ;

         sourceQueue.modifyQueue( diff ) ;
         destinationQueue.modifyQueue( diff ) ;

         xsay( "P2P client (magic)" , destinationName , diff, message ) ;
         xsay( "P2P server (magic)" , sourceName , diff, message ) ;
      }
   }
   public void getInfo( StringBuffer sb ){
      sb.append( "Submodule : CostModule (cm) : ").
         append(this.getClass().getName()).
         append("\n Version : $Revision$\n") ;
        sb.append(" Debug   : ").append(_debug?"on":"off").append("\n");
        sb.append(" Update  : ").append(_update?"on":"off").append("\n");
        sb.append(" Active  : ").append(_isActive?"yes":"no").append("\n");
        sb.append(" Magic   : ").append(_magic?"yes":"no").append("\n");
   }
   public void getInfo( PrintWriter pw ){
      StringBuffer sb = new StringBuffer() ;
      getInfo(sb) ;
      pw.print(sb.toString());
   }
   public void dumpSetup( StringBuffer sb ){
      sb.append( "#\n# Submodule CostModule (cm) : ").
         append(this.getClass().getName()).
         append("\n# $Revision$ \n#\n") ;
      sb.append("cm set debug "+(_debug?"on":"off")+"\n");
      sb.append("cm set update "+(_update?"on":"off")+"\n");
      sb.append("cm set magic "+(_magic?"on":"off")+"\n");

   }
   protected void say( String msg ){
      if(_debug)_cell.say("CostModuleV1 : "+msg ) ;
   }
   protected void esay( String msg ){
      if(_debug)_cell.esay("CostModuleV1 : "+msg ) ;
   }
   private void xsay( String queue , String pool , int diff , Object obj ){
      if(_debug)_cell.say("CostModuleV1 : "+queue+" queue of "+pool+" modified by "+diff+" due to "+obj.getClass().getName());
   }
   public  PoolCostCheckable getPoolCost( String poolName , long filesize ){

      Entry cost = _hash.get(poolName);

      if( ( cost == null ) ||( !cost.isValid() && _update  ) )
    	  return null ;

      return  new CostCheck( poolName , cost , filesize ) ;

   }
   public boolean isActive(){ return _isActive ; }
   public String hh_cm_info = "" ;
   public String ac_cm_info( Args args ){
      StringBuffer sb = new StringBuffer() ;
      getInfo(sb);
      return sb.toString();
   }
   public String hh_cm_set_debug = "on|off" ;
   public String ac_cm_set_debug_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _debug = true ; }
     else if( args.argv(0).equals("off") ){ _debug = false ; }
     else throw new IllegalArgumentException("on|off") ;
     return "";
   }
   public String hh_cm_set_active = "on|off" ;
   public String ac_cm_set_active_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _isActive = true ; }
     else if( args.argv(0).equals("off") ){ _isActive = false ; }
     else throw new IllegalArgumentException("on|off") ;
     return "";
   }
   public String hh_cm_set_update = "on|off" ;
   public String ac_cm_set_update_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _update = true ; }
     else if( args.argv(0).equals("off") ){ _update = false ; }
     else throw new IllegalArgumentException("on|off") ;
     return "";
   }
   public String hh_cm_set_magic = "on|off" ;
   public String ac_cm_set_magic_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _magic = true ; }
     else if( args.argv(0).equals("off") ){ _magic = false ; }
     else throw new IllegalArgumentException("on|off") ;
     return "";
   }
   public String hh_cm_fake = "<poolName> [off] | [-space=<spaceCost>|off] [-cpu=<cpuCost>|off]" ;
   public String ac_cm_fake_$_1_2( Args args ){
      String poolName = args.argv(0) ;
      Entry e = _hash.get(poolName);
      if( e == null )
         throw new
         IllegalArgumentException("Pool not found : "+poolName);

      if( args.argc() > 1 ){
        if( args.argv(1).equals("off") ){
           e._fakeCpu   = -1.0 ;
           e._fakeSpace = -1.0 ;
        }else{
           throw new
           IllegalArgumentException("Unknown argument : "+args.argv(1));
        }
        return "Faked Costs switched off for "+poolName ;
      }
      String val = args.getOpt("cpu") ;
      if( val != null )e._fakeCpu = Double.parseDouble(val) ;
      val = args.getOpt("space") ;
      if( val != null )e._fakeSpace = Double.parseDouble(val);

      return poolName+" -space="+e._fakeSpace+" -cpu="+e._fakeCpu ;
   }
   public String hh_xcm_ls = "<poolName> [<filesize>] [-l]" ;
   public Object ac_xcm_ls_$_0_2( Args args )throws Exception {


      if( args.argc()==0 ){   // added by nicolo : binary full cm ls list

	   CostModulePoolInfoTable reply = new CostModulePoolInfoTable();

	   /* This cycle browse an HashMap of Entry
	    * and put the PoolCostInfo object in the
	    * InfoPoolTable to return.
	    */
	   for (Entry e : _hash.values() ){
  		   reply.addPoolCostInfo(e.getPoolCostInfo().getPoolName(), e.getPoolCostInfo());
	   }
	   return reply;

      }

      String poolName = args.argv(0) ;
      long filesize   = Long.parseLong( args.argc() < 2 ? "0" : args.argv(2) ) ;
      boolean pci     = args.getOpt("l") != null ;
      Object [] reply;

      if( pci ){

         Entry e = _hash.get(poolName) ;
         reply = new Object[3] ;
         reply[0] = poolName ;
         reply[1] = e == null ? null : e.getPoolCostInfo() ;
         reply[2] = e == null ? null : Long.valueOf( System.currentTimeMillis() - e.timestamp ) ;

      }else{

         PoolCostCheckable pcc = getPoolCost( poolName , filesize ) ;

         reply = new Object[4] ;

         reply[0] = poolName ;
         reply[1] = Long.valueOf( filesize ) ;
         reply[2] = pcc == null ? null : new Double( pcc.getSpaceCost() ) ;
         reply[3] = pcc == null ? null : new Double( pcc.getPerformanceCost() ) ;

      }

      return reply ;
   }
   public String hh_cm_ls = " -d  | -t | -r [-size=<filesize>] <pattern> # list all pools" ;
   public String ac_cm_ls_$_0_1( Args args )throws Exception {
      StringBuilder   sb = new StringBuilder() ;
      boolean useTime   = args.getOpt("t") != null ;
      boolean useDetail = args.getOpt("d") != null ;
      boolean useReal   = args.getOpt("r") != null ;
      String  sizeStr   = args.getOpt("size") ;
      long    filesize  = Long.parseLong( sizeStr == null ? "0" : sizeStr ) ;
      Pattern pattern   = args.argc() == 0 ? null : Pattern.compile(args.argv(0)) ;


      for(  Entry e : _hash.values() ){

         String poolName = e.getPoolCostInfo().getPoolName() ;
         if( ( pattern != null ) && ( ! pattern.matcher(poolName).matches() ) )continue ;
         sb.append(e.getPoolCostInfo().toString()).append("\n") ;
         if( useReal ){
             PoolCostCheckable pcc = getPoolCost(poolName,filesize) ;
             if( pcc == null )
                sb.append("NONE\n") ;
             else
                sb.append( getPoolCost(poolName,filesize).toString() ).
                append("\n");
         }
         if( useDetail )
             sb.append(new CostCheck(poolName,e,filesize).toString()).
             append("\n");
         if( useTime )
             sb.append(poolName).
                append("=").
                append(System.currentTimeMillis()-e.timestamp).
                append("\n");

      }

      return sb.toString();
   }


   public PoolCostInfo getPoolCostInfo(String poolName) {

	   PoolCostInfo poolCostInfo = null;

	   Entry poolEntry = _hash.get(poolName);

	   if( poolEntry != null ) {
		   poolCostInfo = poolEntry.getPoolCostInfo();
	   }

	   return poolCostInfo;
   }

}
