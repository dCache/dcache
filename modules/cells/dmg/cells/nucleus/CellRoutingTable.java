package dmg.cells.nucleus ;
import  dmg.util.Formats;
import  dmg.util.Args;
import  java.io.* ;
import  java.util.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellRoutingTable implements Serializable {
   static final long serialVersionUID = -1456280129622980563L;
   private Hashtable  _wellknown  = new Hashtable() ;
   private Hashtable  _domain     = new Hashtable() ;
   private Hashtable  _exact      = new Hashtable() ;
   private CellRoute  _dumpster   = null ;
   private CellRoute  _default    = null ;
   
   public CellRoutingTable(){
   
   } 
   public synchronized void add( CellRoute route ) 
          throws IllegalArgumentException {
       
      int type = route.getRouteType() ;
      String dest ;
      switch( type ){
        case CellRoute.EXACT :
        case CellRoute.ALIAS :
           dest = route.getCellName()+"@"+route.getDomainName() ;
           if( _exact.get( dest ) != null )
             throw new IllegalArgumentException( "Duplicated Entry" ) ;
           _exact.put( dest , route ) ;
        break ;
        case CellRoute.WELLKNOWN :
           dest = route.getCellName() ;
           if( _wellknown.get( dest ) != null )
             throw new IllegalArgumentException( "Duplicated Entry" ) ;
           _wellknown.put( dest , route ) ;
        break ;
        case CellRoute.DOMAIN :
           dest = route.getDomainName() ;
           if( _domain.get( dest ) != null )
             throw new IllegalArgumentException( "Duplicated Entry" ) ;
           _domain.put( dest , route ) ;
        break ;
        case CellRoute.DEFAULT :
           if( _default != null )
             throw new IllegalArgumentException( "Duplicated Entry" ) ;
           _default = route ;
        break ;
        case CellRoute.DUMPSTER :
           if( _dumpster != null )
             throw new IllegalArgumentException( "Duplicated Entry" ) ;
           _dumpster = route ;
        break ;
      
      }   
          
   }
   public synchronized void delete( CellRoute route ) 
          throws IllegalArgumentException {
          
      int type = route.getRouteType() ;
      String dest ;
      switch( type ){
        case CellRoute.EXACT :
        case CellRoute.ALIAS :
           dest = route.getCellName()+"@"+route.getDomainName() ;
           if( _exact.remove( dest ) == null )
             throw new IllegalArgumentException( "Route Entry Not Found" ) ;
        break ;
        case CellRoute.WELLKNOWN :
           dest = route.getCellName() ;
           if( _wellknown.remove( dest ) == null )
             throw new IllegalArgumentException( "Route Entry Not Found" ) ;
        break ;
        case CellRoute.DOMAIN :
           dest = route.getDomainName() ;
           if( _domain.remove( dest ) == null )
             throw new IllegalArgumentException( "Route Entry Not Found" ) ;
        break ;
        case CellRoute.DEFAULT :
           if( _default == null )
             throw new IllegalArgumentException( "Route Entry Not Found" ) ;
           _default = null ;
        break ;
        case CellRoute.DUMPSTER :
           if( _dumpster == null )
             throw new IllegalArgumentException( "Route Entry Not Found" ) ;
           _dumpster = null ;
        break ;
      
      }   
          
   }
   public synchronized CellRoute find( CellAddressCore addr ){
      String cellName   = addr.getCellName() ;
      String domainName = addr.getCellDomainName(); 
      CellRoute route   = null   ;
      if( domainName.equals("local") ){
        //
        // this is not really local but wellknown
        // we checked for local before we called this.
        //
        route = (CellRoute)_wellknown.get( cellName ) ;
        if( route != null )return route ;
      }else{
        route = (CellRoute)_exact.get(cellName+"@"+domainName) ;
        if( route != null )return route ;
        route = (CellRoute)_domain.get( domainName ) ;        
        if( route != null )return route ;
      }
      route = (CellRoute)_exact.get( cellName+"@"+domainName ) ;
      return route == null ? _default : route ;
        
   }
   public String toString(){
      Enumeration e ;
      StringBuffer sb = new StringBuffer() ;
      sb.append( CellRoute.headerToString()+"\n" ) ;
      for( e = _exact.elements() ; e.hasMoreElements() ; )
         sb.append( ((CellRoute)e.nextElement()).toString()+"\n" ) ;
      for( e = _wellknown.elements() ; e.hasMoreElements() ; )
         sb.append( ((CellRoute)e.nextElement()).toString()+"\n" ) ;
      for( e = _domain.elements() ; e.hasMoreElements() ; )
         sb.append( ((CellRoute)e.nextElement()).toString()+"\n" ) ;
      if( _default != null )
         sb.append( _default.toString()+"\n" ) ;
      if( _dumpster != null )
         sb.append( _dumpster.toString()+"\n" ) ;
      
      return sb.toString();
   }
   public synchronized CellRoute [] getRoutingList(){
      int total = _exact.size() + 
                  _wellknown.size() +
                  _domain.size() +
                  ( _default != null  ? 1 : 0 ) +
                  ( _dumpster != null ? 1 : 0 )  ;
                   
      CellRoute [] routes = new CellRoute[total] ;
      int i = 0 ;   
              
      Enumeration e ;
      for( e = _exact.elements() ; e.hasMoreElements() ; )
         routes[i++] = ((CellRoute)e.nextElement()) ;
      for( e = _wellknown.elements() ; e.hasMoreElements() ; )
         routes[i++] = ((CellRoute)e.nextElement()) ;
      for( e = _domain.elements() ; e.hasMoreElements() ; )
         routes[i++] = ((CellRoute)e.nextElement())  ;
      if( _default != null )
         routes[i++] =  _default ;
      if( _dumpster != null )
         routes[i++] =  _dumpster ;
      
      return routes ;
   }
   public static void main( String [] argsxx ){
      CellRoutingTable table = new CellRoutingTable() ;
      String line ;
      BufferedReader reader = 
         new BufferedReader( new InputStreamReader( System.in ) ) ;
      try{
         while( ( line = reader.readLine() ) != null ){
           Args args = new Args( line ) ;
           if( args.argc() < 1 )continue ;
           String com = args.argv(0) ;
           args.shift() ;
           if( com.equals( "add" ) ){
              c_add( table , args ) ;
           }else if( com.equals( "show" ) ){
              System.out.println( table.toString() ) ;
           }else if( com.equals( "delete" ) ){
              c_delete( table , args ) ;
           }else if( com.equals( "find" ) ){
              c_find( table , args ) ;
           }
         }
      }catch( IOException ioe ){
        System.err.println( " Exception "+ioe ) ;
      }
   }
   public static void c_delete( CellRoutingTable table , Args args ){
      try{
         table.delete( new CellRoute( args ) ); 
      }catch( Exception e ){
        System.err.println( " Exception "+e ) ;
      }
   }
   public static void c_add( CellRoutingTable table , Args args ){
      // route add type destination target
      try{
         table.add( new CellRoute( args ) ); 
      }catch( Exception e ){
        System.err.println( " Exception "+e ) ;
      }
   }
   public static void c_find( CellRoutingTable table , Args args ){
      // route add type destination target
      if( args.argc() == 0 ){
        System.out.println( "Cell Argument missing" ) ;
        return ;
      }
      try{
         CellAddressCore addr = new CellAddressCore( args.argv(0) ) ;
         System.out.println( " Searching : "+addr.toString() ); 
         CellRoute route = table.find( addr );
         if( route != null )System.out.println( " Found route : "+route.toString() ); 
         else System.out.println( "Not found" ) ;
      }catch( Exception e ){
        System.err.println( " Exception "+e ) ;
      }
   }

}
