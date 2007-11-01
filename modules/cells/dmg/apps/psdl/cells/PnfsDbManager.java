package dmg.apps.psdl.cells ;

import java.lang.reflect.* ;
import dmg.apps.psdl.pnfs.* ;
import dmg.apps.psdl.vehicles.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;
import java.util.* ;
import java.io.* ;

public class PnfsDbManager extends CellAdapter {
   private Args        _args ;
   private Date        _lastRequest = null , _lastAnswer = null ;
   private int         _requests = 0 , _answers = 0 ;
   private String      _mountpoint  = null ;
   private CellPath    _poolMgrPath = null ;
   private Class []    _hsmPropConstArgs = { dmg.apps.psdl.pnfs.PnfsFile.class , 
                                             dmg.apps.psdl.pnfs.PnfsFile.class } ;

   private String      _hsmPropClassName = "dmg.apps.psdl.vehicles.HsmPropertiesOSM" ;
     
   public PnfsDbManager( String name , String args ){
       super( name , args , false ) ; // don't start now
       
       _args = getArgs() ;

       if( _args.argc() > 0 ){
          _poolMgrPath = new CellPath( _args.argv(0) ) ;
          if( _args.argc() > 1 )_mountpoint  = _args.argv(1) ;
       }
       //
       //     
       start() ;  
       //
       if( ( _mountpoint == null ) || ( _poolMgrPath == null ) ){
          String problem =
               "<PnfsDbManager> : Mountpoint or poolMgrPath not specified"  ;
          esay( problem ) ;
          kill() ;
          throw new IllegalArgumentException( problem ) ;
       } 
   }
   public String toString(){
      return _args==null?"Initializing ... ":_args.toString() ;
   }
   public void messageToForward( CellMessage msg ){
      try{
         msg.nextDestination() ;
         sendMessage( msg ) ;
      }catch( Exception e ){
         esay( "Can't forward message to : "+msg.getDestinationPath() ) ;
      }
      _answers++ ;
      _lastAnswer = new Date() ;
   }
   public void messageArrived( CellMessage msg ){
   
      Object obj = msg.getMessageObject() ;
      if( obj instanceof PsdlCoreRequest ){
      
         requestArrived( (PsdlCoreRequest) obj , msg ) ;
         
      }else if( obj instanceof NoRouteToCellException ){
         exceptionArrived( (NoRouteToCellException) obj , msg ) ;
      }
   }
   private void requestArrived( PsdlCoreRequest req , CellMessage msg ){
   
      _requests ++ ;
      _lastRequest = new Date() ;
      
      PnfsFile pnfsFile = PnfsFile.getFileByPnfsId( _mountpoint ,
                                                    req.getPnfsId() ) ;
      PnfsFile pnfsDir  = PnfsFile.getFileByPnfsId( _mountpoint ,
                                                    req.getPnfsParentId() ) ;

      if( ( pnfsFile == null ) || ( pnfsDir == null ) ){
          req.setReturnValue( 37 , "PnfsFile <"+req.getPnfsId()+
                                   "> or PnfsDir <"+req.getPnfsParentId()+
                                   "> not found !" ) ;
          msg.revertDirection() ;
          try{
             sendMessage( msg ) ;          
          }catch( Exception e ){
             esay( "Can't return message to : "+msg.getDestinationPath() ) ;
          }
      }
      if( req instanceof PsdlPutRequest ){
          putRequestArrived( (PsdlPutRequest)req , msg , pnfsFile , pnfsDir ) ;
      }else{
          req.setReturnValue( 39 , "Unsupported request : "+req.getClass() ) ;
          msg.revertDirection() ;
          try{
             sendMessage( msg ) ;          
          }catch( Exception e ){
             esay( "Can't return message to : "+msg.getDestinationPath() ) ;
          }
      }
   }
   private void sendProblem( CellMessage msg ){
   
         msg.revertDirection() ;
         try{
            sendMessage( msg ) ;
         }catch( Exception eee ){
             esay( "Can't return message to : "+msg.getDestinationPath() ) ;
         }
   }
   private HsmProperties newHsmProperty( PnfsFile dir , PnfsFile file )
           throws Exception {
   
          Class hsmPropClass = loadClass( _hsmPropClassName ) ; 
          Constructor hsmPropConst = 
                      hsmPropClass.getConstructor( _hsmPropConstArgs ) ; 
                      
          Object [] arg = new Object[2] ;
          arg[0] = dir ;
          arg[1] = file ;
          
          return (HsmProperties)hsmPropConst.newInstance( arg ) ;
   }
   private void putRequestArrived( PsdlPutRequest req , 
                                   CellMessage msg ,
                                   PnfsFile    pnfsFile ,
                                   PnfsFile    pnfsDir     ){

      HsmProperties hsm = null ;
      try{ 
         hsm = newHsmProperty( pnfsDir , pnfsFile ) ;                 
      }catch( Exception ioe ){
         say( "Problem in newHsmProperty : "+ioe ) ;
         pnfsFile.delete() ;
         
         String err = "Problem in getHsmProperties of "+pnfsFile.getPnfsId()+
                      "("+pnfsDir.getPnfsId()+")"  ;
         if( ioe instanceof InvocationTargetException ){
            Throwable te = ((InvocationTargetException)ioe).getTargetException();
            err+="<"+te.getMessage()+">" ;
         }else{
            err+="<"+ioe.getMessage()+">" ;
         }
         req.setReturnValue( 38 , err ) ;
         sendProblem( msg ) ;
         return ;
      }
      if( hsm.isFile() ){
         req.setReturnValue( 39 , "File exists" ) ;
         sendProblem( msg ) ;
         return ;
      }
      say( "Hsm info : "+hsm ) ;
      req.setHsmProperties( hsm ) ;
      try{
         msg.getDestinationPath().add( _poolMgrPath ) ;
         msg.nextDestination() ;
         sendMessage( msg ) ;
      }catch( Exception ee ){
         req.setReturnValue( 40 , "PoolManager not available : "+_poolMgrPath ) ;
         sendProblem(msg ) ;
      }
   
   }
   public void getInfo( PrintWriter pw ){
       super.getInfo( pw ) ;
       pw.println( " Pnfs Mountpoint       : "+_mountpoint ) ;
       pw.println( " PoolManager           : "+_poolMgrPath ) ;
       pw.println( " Hsm Properity Class   : "+_hsmPropClassName ) ;
       pw.println( " Last Request Received : "+
                   (_lastRequest==null?"None Yet":_lastRequest.toString() ) ) ;
       pw.println( " Last Answer Received  : "+
                   (_lastAnswer==null?"None Yet":_lastAnswer.toString() ) ) ;
       pw.println( " Number of requests    : "+_requests ) ;
       pw.println( " Number of answers     : "+_answers ) ;
       pw.println( " Outstanding requests  : "+(_requests-_answers) ) ;
   }
   public String ac_set_hsmPropertyClass_$_1( Args args ){
       _hsmPropClassName = args.argv(0) ;
       return "" ;
   }
   public String ac_set_mountpoint_$_1( Args args ){
       _mountpoint = args.argv(0) ;
       return "" ;
   }
   public String ac_set_poolmgr_$_1( Args args ){
       _poolMgrPath = new CellPath( args.argv(0) ) ;
       return "" ;
   }
   private void exceptionArrived( NoRouteToCellException exc , CellMessage msg ){
      esay( "PANIC : No Route To Cell arrived from : "+
            msg.getSourcePath()+" -> "+exc ) ;
   } 

}
