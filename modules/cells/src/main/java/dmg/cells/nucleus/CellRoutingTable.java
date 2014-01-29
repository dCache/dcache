package dmg.cells.nucleus ;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.dcache.util.Args;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellRoutingTable implements Serializable {

   private static final long serialVersionUID = -1456280129622980563L;

   private final Map<String, CellRoute>  _wellknown  = new HashMap<>() ;
   private final Map<String, CellRoute>  _domain     = new HashMap<>() ;
   private final Map<String, CellRoute>  _exact      = new HashMap<>() ;
   private CellRoute  _dumpster   = null ;
   private CellRoute  _default    = null ;

   public synchronized void add( CellRoute route )
          throws IllegalArgumentException {

      int type = route.getRouteType() ;
      String dest ;
      switch( type ){
        case CellRoute.EXACT :
        case CellRoute.ALIAS :
           dest = route.getCellName()+"@"+route.getDomainName() ;
           if( _exact.get( dest ) != null ) {
               throw new IllegalArgumentException("Duplicated route Entry for : " + dest);
           }
           _exact.put( dest , route ) ;
        break ;
        case CellRoute.WELLKNOWN :
           dest = route.getCellName() ;
           if( _wellknown.get( dest ) != null ) {
               throw new IllegalArgumentException("Duplicated route Entry for : " + dest);
           }
           _wellknown.put( dest , route ) ;
        break ;
        case CellRoute.DOMAIN :
           dest = route.getDomainName() ;
           if( _domain.get( dest ) != null ) {
               throw new IllegalArgumentException("Duplicated route Entry for : " + dest);
           }
           _domain.put( dest , route ) ;
        break ;
        case CellRoute.DEFAULT :
           if( _default != null ) {
               throw new IllegalArgumentException("Duplicated route Entry for default.");
           }
           _default = route ;
        break ;
        case CellRoute.DUMPSTER :
           if( _dumpster != null ) {
               throw new IllegalArgumentException("Duplicated route Entry for dumpster");
           }
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
           if( _exact.remove( dest ) == null ) {
               throw new IllegalArgumentException("Route Entry Not Found for : " + dest);
           }
        break ;
        case CellRoute.WELLKNOWN :
           dest = route.getCellName() ;
           if( _wellknown.remove( dest ) == null ) {
               throw new IllegalArgumentException("Route Entry Not Found for : " + dest);
           }
        break ;
        case CellRoute.DOMAIN :
           dest = route.getDomainName() ;
           if( _domain.remove( dest ) == null ) {
               throw new IllegalArgumentException("Route Entry Not Found for : " + dest);
           }
        break ;
        case CellRoute.DEFAULT :
           if( _default == null ) {
               throw new IllegalArgumentException("Route Entry Not Found for default");
           }
           _default = null ;
        break ;
        case CellRoute.DUMPSTER :
           if( _dumpster == null ) {
               throw new IllegalArgumentException("Route Entry Not Found dumpster");
           }
           _dumpster = null ;
        break ;

      }

   }
   public synchronized CellRoute find( CellAddressCore addr ){
      String cellName   = addr.getCellName() ;
      String domainName = addr.getCellDomainName();
      CellRoute route;
      if( domainName.equals("local") ){
        //
        // this is not really local but wellknown
        // we checked for local before we called this.
        //
        route = _wellknown.get( cellName ) ;
        if( route != null ) {
            return route;
        }
      }else{
        route = _exact.get(cellName+"@"+domainName) ;
        if( route != null ) {
            return route;
        }
        route = _domain.get( domainName ) ;
        if( route != null ) {
            return route;
        }
      }
      route = _exact.get( cellName+"@"+domainName ) ;
      return route == null ? _default : route ;

   }
   public synchronized String toString(){

      StringBuilder sb = new StringBuilder() ;
      sb.append(CellRoute.headerToString()).append("\n");
      for( CellRoute route:  _exact.values()  ) {
          sb.append(route.toString()).append("\n");
      }
      for( CellRoute route: _wellknown.values() ) {
          sb.append(route.toString()).append("\n");
      }
      for( CellRoute route: _domain.values() ) {
          sb.append(route.toString()).append("\n");
      }
      if( _default != null ) {
          sb.append(_default.toString()).append("\n");
      }
      if( _dumpster != null ) {
          sb.append(_dumpster.toString()).append("\n");
      }

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

      for( CellRoute route:  _exact.values() ) {
          routes[i++] = route;
      }
      for( CellRoute route:  _wellknown.values() ) {
          routes[i++] = route;
      }
      for( CellRoute route:  _domain.values() ) {
          routes[i++] = route;
      }
      if( _default != null ) {
          routes[i++] = _default;
      }
      if( _dumpster != null ) {
          routes[i++] = _dumpster;
      }

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
           if( args.argc() < 1 ) {
               continue;
           }
           String com = args.argv(0) ;
           args.shift() ;
             switch (com) {
             case "add":
                 c_add(table, args);
                 break;
             case "show":
                 System.out.println(table.toString());
                 break;
             case "delete":
                 c_delete(table, args);
                 break;
             case "find":
                 c_find(table, args);
                 break;
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
         if( route != null ) {
             System.out.println(" Found route : " + route.toString());
         } else {
             System.out.println("Not found");
         }
      }catch( Exception e ){
        System.err.println( " Exception "+e ) ;
      }
   }

}
