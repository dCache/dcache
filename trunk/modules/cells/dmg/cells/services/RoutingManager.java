package dmg.cells.services ;
import  java.util.* ;
import  java.io.* ;
import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

/**
  *
  * The dmg.cells.services.RoutingManager is a ready to use
  * service Cell, performing the following services :
  * <ul>
  * <li>Watching a specified tunnel cell and setting the
  *     default route to this cell as soon as this tunnel
  *     cell establishes its domain route.
  * <li>Assembling downstream routing informations and
  *     the exportCell EventListener Event and maitaining
  *     a wellknown Cell list.
  * <li>Sending its wellknown cell list upstream as soon
  *     as a default route is available and whenever
  *     the wellknown cell list changes.
  * </ul>
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class RoutingManager 
       extends CellAdapter 
       implements CellEventListener {
       
   private CellNucleus _nucleus ;
   private Args        _args ;
   private Hashtable   _localExports = new Hashtable() ;
   private Hashtable   _domainHash   = new Hashtable() ;
   private String      _watchCell    = null ;
   private boolean     _defaultInstalled = false ;
   public RoutingManager( String name , String args ){
       super( name ,"System",  args , false ) ;
       _nucleus = getNucleus() ;
       _args    = getArgs() ;
       
       _nucleus.addCellEventListener( this ) ;
       _watchCell = _args.argc() == 0 ? null : _args.argv(0) ;
       
       start() ;
   }
   public void getInfo( PrintWriter pw ){
      pw.println( " Our routing knowledge : " ) ;
      pw.print( " Local : ") ;
      Enumeration e = _localExports.elements() ;
      for( ; e.hasMoreElements() ; )
         pw.print( e.nextElement().toString()+"," )  ;
      pw.println("");
      
      e = _domainHash.keys() ;
      for( ; e.hasMoreElements() ; ){
         String  domain  = (String)e.nextElement() ;
         pw.print( " "+domain+" : " ) ;
         Hashtable h =  (Hashtable)_domainHash.get( domain ) ;
         Enumeration ee = h.elements() ;
         for( ; ee.hasMoreElements() ; )
            pw.print( ee.nextElement().toString()+"," ) ;
         pw.println("");
      }
      
   }
   private void updateUpstream(){

      Vector all = new Vector() ;
      say( "update requested to upstream Domains" ) ;
      //
      // the protocol requires the local DomainName
      // first
      //
      all.addElement( _nucleus.getCellDomainName() ) ;
      //
      // here we add our own exportables
      //
      Enumeration e = _localExports.elements() ;
      for( ; e.hasMoreElements() ; )
         all.addElement( e.nextElement() ) ;
      //
      // and now all the others
      //
      e = _domainHash.elements() ;
      for( ; e.hasMoreElements() ; ){
         Hashtable h = (Hashtable)e.nextElement() ;
         Enumeration ee = h.elements() ;
         for( ; ee.hasMoreElements() ; )
            all.addElement( ee.nextElement() ) ;
      }

      String [] arr = new String[all.size() ] ;
      all.copyInto( arr ) ;
      
      StringBuffer sb = new StringBuffer() ;
      for( int i = 0 ; i < arr.length ; i++ )sb.append(arr[i]).append(",");
      String destinationManager = _nucleus.getCellName()  ;
      say( "Resending to "+destinationManager+" : "+sb.toString() ) ;
      try{
          _nucleus.resendMessage(
                        new CellMessage( 
                               new CellPath( destinationManager ) ,
                                arr )
                              ) ;
                              
      }catch( Exception eeee ){
         esay( "update can't send update  to RoutingMgr" + eeee.getMessage() ) ;
      }
   }
   private synchronized void addRoutingInfo( String [] info ) {
      String domain = info[0] ;
      Hashtable hash = null ;
      if( ( hash = (Hashtable)_domainHash.get( domain ) ) == null ){
         say( "Adding new domain : "+domain ) ;
         hash = new Hashtable() ;
         for( int i = 1 ; i < info.length ; i++ ){
            hash.put( info[i] , info[i] ) ;
            addWellknown( info[i] , domain ) ;
         }  
         _domainHash.put( domain , hash ) ;
      
      }else{
         say( "Updating domain : "+domain ) ;
         Hashtable h = (Hashtable)hash.clone() ;
         Hashtable newHash = new Hashtable() ;
         for( int i = 1 ; i < info.length ; i++ ){
            say( "Adding : "+info[i] ) ;
            newHash.put( info[i] , info[i] ) ;
            if( h.remove( info[i] ) == null ){
               // entry not found, so make it
               addWellknown( info[i] , domain ) ;
            }
         }
         // all additional route added now, need to remove the rest
         Enumeration e = h.keys() ;
         for( ; e.hasMoreElements() ; ){
             String cell = (String)e.nextElement() ;
             say( "Removing : "+cell ) ;
             removeWellknown( cell , domain ) ;
         }
         _domainHash.put( domain , newHash ) ;
      }
      if( _defaultInstalled )updateUpstream() ;   
   }
   public void messageArrived( CellMessage msg ){
      Object obj = msg.getMessageObject() ;
      if( obj instanceof String [] ){
         String [] info = (String [] ) obj ;
         if( info.length < 1 ){
            esay( "Protocol error 1 in routing info" ) ;
            return ;
         }
         say( "Routing info arrived for Domain : "+info[0] ) ;
         addRoutingInfo( info ) ;
      }else{
         esay( "Unidentified message ignored : "+obj ) ;
      }
   }
   public void cellCreated( CellEvent ce ){
      String name = (String) ce.getSource() ;
      say( "cellCreated : "+name ) ;
   }
   public void cellDied( CellEvent ce ){
      String name = (String) ce.getSource() ;
      say("cellDied : "+name ) ;
      _localExports.remove( name ) ;
      synchronized( this ){ updateUpstream() ; }
   }
   public void cellExported( CellEvent ce ){
      String name = (String) ce.getSource() ;
      say( "cellExported : "+name);
      _localExports.put( name , name ) ;
      synchronized( this ){ updateUpstream() ; }
   }
   public void routeAdded( CellEvent ce ){
      CellRoute       cr   = (CellRoute)ce.getSource() ;
      CellAddressCore gate = new CellAddressCore( cr.getTargetName() ) ;
      say("Got 'route added' event : "+cr ) ;
      if( cr.getRouteType() == CellRoute.DOMAIN ){
          if( ( _watchCell != null ) && gate.getCellName().equals(_watchCell)    ){
             //
             // the upstream route ( we only support one )
             //
             try{
                CellRoute defRoute = 
                   new CellRoute( "" , 
                                  "*@"+cr.getDomainName() , 
                                  CellRoute.DEFAULT         ) ;
                _nucleus.routeAdd( defRoute ) ;
             }catch( IllegalArgumentException e ){
                esay( "Couldn't add default route : "+e ) ;
             }
          }else{
             //
             // possible downstream routes
             //
             // say( "Downstream route added : "+ cr ) ;
             say( "Downstream route added to Domain : "+cr.getDomainName() ) ;
             //
             // If the locationManager takes over control
             // the default route may be installed before
             // the actual domainRouted is added. Therefore
             // we have to 'updateUpstream' for each route.
             updateUpstream() ;
          }
      }else if( cr.getRouteType() == CellRoute.DEFAULT ){
          say( "Default route was added" ) ;
          _defaultInstalled = true ;
          updateUpstream() ;
      }
   }
   private void addWellknown( String cell , String domain ){
      if( cell.startsWith("@") )return ;
      try{
         _nucleus.routeAdd(
            new CellRoute( cell ,
                           "*@"+domain ,
                           CellRoute.WELLKNOWN ) 
                          ) ;
      }catch( IllegalArgumentException e ){
         esay( "Couldn't add wellknown route : "+e ) ;
      }
   }
   private void removeWellknown( String cell , String domain ){
      if( cell.startsWith("@") )return ;
      try{
         _nucleus.routeDelete(
            new CellRoute( cell ,
                           "*@"+domain ,
                           CellRoute.WELLKNOWN ) 
                          ) ;
      }catch( IllegalArgumentException e ){
         esay( "Couldn't delete wellknown route : "+e ) ;
      }
   }
   public void routeDeleted( CellEvent ce ){
      CellRoute cr = (CellRoute)ce.getSource() ;
      CellAddressCore gate = new CellAddressCore( cr.getTargetName() ) ;
      if( cr.getRouteType() == CellRoute.DOMAIN ){
          if( ( _watchCell != null ) && gate.getCellName().equals(_watchCell)  ){
             CellRoute defRoute = 
                new CellRoute( "" , 
                               "*@"+cr.getDomainName() , 
                               CellRoute.DEFAULT         ) ;
             _nucleus.routeDelete( defRoute ) ;
          }else{
            synchronized( this ){
               String domain = cr.getDomainName() ;
               say( "Removing all routes to domain : "+domain ) ;
               Hashtable hash = 
                  (Hashtable)_domainHash.remove( domain ) ;
               if( hash == null ){
                  say( "No entry found for domain : "+domain ) ;
                  return ;
               }
               Enumeration e = hash.keys() ;
               for( ; e.hasMoreElements() ; )
                  removeWellknown( (String)e.nextElement() , domain ) ;
               
            }
          }
      }else if( cr.getRouteType() == CellRoute.DEFAULT ){
          _defaultInstalled = false ;
      }
   }
   public String ac_update( Args args )throws Exception{
      updateUpstream() ;
      return "Done" ;      
   }

}
