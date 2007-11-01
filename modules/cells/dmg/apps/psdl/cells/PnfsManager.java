package dmg.apps.psdl.cells ;

import dmg.apps.psdl.pnfs.* ;
import dmg.apps.psdl.vehicles.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;
import java.util.* ;
import java.io.* ;

public class PnfsManager extends CellAdapter {
   private Args      _args ;
   private Date      _lastRequest = null , _lastAnswer = null ;
   private int       _requests = 0 , _answers = 0 ;
   private DbLookup  _db = new DbLookup() ;
      
   private class DbLookup {
      private String [] _dbTable    = new String[32] ;
      private String    _dbDefault  = null ;
      
      public void setDefaultDbEntry( String defName ){
         _dbDefault = defName ;
      }
      public void setDbEntry( int pos , String name ){
         if( pos >= _dbTable.length ){         
             String [] tmp = new String[ pos + 10 ] ;
             for( int i = 0 ; i < _dbTable.length ; i++ )
                 tmp[i] = _dbTable[i] ;
             _dbTable = tmp ;
         }
         _dbTable[pos] = name ;
      }
      public String  getDbEntry( int pos ){
         return  ( pos  >= _dbTable.length ) ||
                 ( _dbTable[pos] == null   )    ?
                _dbDefault : _dbTable[pos]  ;
      }
      public String toString(){
         Hashtable dbNames = new Hashtable() ;
         for( int i = 0 ; i < _dbTable.length ; i++ ){
             if( _dbTable[i] == null )continue ;
             String name  = _dbTable[i]  ;
             Vector array = (Vector)dbNames.get( name ) ;
             if( array == null ){
                array = new Vector() ;
                dbNames.put( name , array ) ;
             }
             array.addElement( new Integer(i) ) ;         
         }
         StringBuffer   sb = new StringBuffer() ;
         sb.append( "Default CellName : "+
                    ((_dbDefault==null)?"none":_dbDefault)+"\n" ) ;
         for( Enumeration e = dbNames.keys() ; e.hasMoreElements() ; ){
             String cellName = (String)e.nextElement() ;
             Vector array    = (Vector)dbNames.get( cellName ) ;
             sb.append( cellName + " :" ) ;
             for( int i = 0 ; i < array.size() ; i++ )
                sb.append( " "+array.elementAt(i).toString() ) ;
             sb.append( "\n" ) ;
         
         }
         return sb.toString() ;
      }
   
   }
   public PnfsManager( String name , String args ){
       super( name , args , false ) ; // don't start now
       
       _args = getArgs() ;

       start() ;   
   }
   public String toString(){
      return _args==null?"Initializing ... ":_args.toString() ;
   }
   public void messageToForward( CellMessage msg ){

      messageArrived( msg ) ;      
      _answers++ ;
      _lastAnswer = new Date() ;
   }
   public void messageArrived( CellMessage msg ){
   
      Object obj = msg.getMessageObject() ;
      if( obj instanceof PsdlCoreRequest ){
         boolean wayBack ;
         PsdlCoreRequest req = (PsdlCoreRequest) obj ;
         if( req.getRequestCommand().equals("answer") ){
            //
            // on the way form the PnfsDbManager back to the door
            //
            if( req.getReturnCode() == 0 ){
               //
               // now travel to our HsmManager
               //
               wayBack = answerRequestArrived( req , msg ) ;
            }else{
               //
               // failed for some reason, so skip the HsmManager
               //
               wayBack = true ;
            }
            //
         }else if( req.getRequestCommand().equals("storeFile") ){
            // 
            // after the HsmManager has processed the request
            //
            if( req.getReturnCode() != 0 ){
                //
                // create a remove request and send it to the
                // appropiate the PoolObserver
                //
            }
            msg.nextDestination() ;
            wayBack = true ;
            //
         }else{
            wayBack = initialRequestArrived( req , msg ) ;
         }
         try{
            sendMessage( msg ) ;
         }catch( Exception e ){
            if( wayBack ){
                esay( "PANIC : can't send msg to "+msg.getDestinationPath() ) ;
            }else{
                req.setReturnValue( 66 , "Problem : "+e ) ;
                msg.revertDirection() ;
                try{
                    sendMessage( msg ) ;
                }catch( Exception ee ){
                    esay( "PANIC : can't send msg to "+msg.getDestinationPath() ) ;
                }
            }
         }
         
         
      }else if( obj instanceof NoRouteToCellException ){
         exceptionArrived( (NoRouteToCellException) obj , msg ) ;
      }
   }
   private boolean answerRequestArrived( PsdlCoreRequest coreReq , CellMessage msg ){
       if( coreReq instanceof PsdlPutRequest ){
          PsdlPutRequest req = (PsdlPutRequest)coreReq ;
          req.setRequestCommand( "storeFile" ) ;
          msg.getDestinationPath().insert( req.getHsmManager() ) ;
          msg.nextDestination() ;
          return true ;
       }
       return true ;
   }
   private boolean initialRequestArrived( PsdlCoreRequest req , CellMessage msg ){
   
//      String   cellName = req.getPnfsId().toShortString() ;

//      PnfsFile pnfsFile = PnfsFile.getFileByPnfsId( _mountpoint ,
//                                                    req.getPnfsId() ) ;
      _requests ++ ;
      _lastRequest = new Date() ;
      int dbId = req.getPnfsId().getDatabaseId() ;
      String cellName = null ;
      if( ( cellName = _db.getDbEntry( dbId ) ) == null  ){
          req.setReturnValue( 33 , "No setup found for dbId = "+dbId ) ;
          msg.revertDirection() ;
          return true ;
      }

      msg.getDestinationPath().add( new CellPath( cellName ) ) ;
      msg.nextDestination() ;
      return false ;
   
   }
   public void getInfo( PrintWriter pw ){
       super.getInfo( pw ) ;
       pw.println( " Last Request Received : "+
                   (_lastRequest==null?"None Yet":_lastRequest.toString() ) ) ;
       pw.println( " Last Answer Received  : "+
                   (_lastAnswer==null?"None Yet":_lastAnswer.toString() ) ) ;
       pw.println( " Number of requests    : "+_requests ) ;
       pw.println( " Number of answers     : "+_answers ) ;
       pw.println( " Outstanding requests  : "+(_requests-_answers) ) ;
       pw.println( " Database to Cell map  : \n"+_db.toString()  ) ;
   }
   private void exceptionArrived( NoRouteToCellException exc , CellMessage msg ){
      //
      // this part should never happen, because we send the message only
      // locally.
      //
      esay( "PANIC : No Route To Cell arrived from : "+
            msg.getSourcePath()+" -> "+exc ) ;
   } 
   public String hh_set_db  = "<pnfsDbCellName> <dbId0> [<dbId1> [...]]" ;
   public String ac_set_db_$_2_99( Args args ) throws CommandException {
       String cellName = args.argv(0) ;
       for( int i = 1 ; i < args.argc() ; i++ ){
           if( args.argv(i).equals( "..." ) ){
              _db.setDefaultDbEntry( cellName ) ;
           }else{
              try{
                  int id = Integer.parseInt( args.argv(i) ) ;
                  _db.setDbEntry( id , cellName ) ;
              }catch(IllegalArgumentException e ){
                  throw new CommandException( "Not a valid dbId : "+args.argv(i) ) ;
              }
           }
       
       }
       return "" ;
         
   }
   public String ac_show_db(Args args ){
      return _db.toString() ;
   }

}
