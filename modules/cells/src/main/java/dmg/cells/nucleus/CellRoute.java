package dmg.cells.nucleus ;

import  dmg.util.*;
import  java.io.Serializable ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
  /*
   * route add -default             <cell>[@<domain>]
   * route add -domain  <domain>    <cell>[@<domain>]
  *   WARNING : This Class is designed to be imutual.
  *             All other class rely on that fact and
  *             a lot of things may fail at runtime
  *             if this design item is changed.
   */
public class CellRoute implements Cloneable , Serializable {
   static final long serialVersionUID = 4566260400288960984L;
   private String  _destCell ;
   private String  _destDomain ;
   private String  _gateway ;
   private int     _type  ;
   
   public static final int AUTO      = 0 ;
   public static final int EXACT     = 1 ;
   public static final int WELLKNOWN = 2 ;
   public static final int DOMAIN    = 3 ;
   public static final int DEFAULT   = 4 ;
   public static final int DUMPSTER  = 5 ;
   public static final int ALIAS     = 6 ;
   
   private final static String [] __typeNames =
     { "Auto"    , "Exact"    , "Wellknown" , "Domain" , 
       "Default" , "Dumpster" , "Alias"                  } ;
   
   public CellRoute(){}
   public CellRoute( Args args )
          throws IllegalArgumentException {
          
      String  opt = args.optc() == 0 ? "-auto" : args.optv(0) ;
      int type ;
      
      if( args.argc() == 0 ) {
          throw new IllegalArgumentException("Not enough arguments");
      }
      
      type = AUTO ;
           if( opt.equals( "-auto")     ) {
               type = AUTO;
           } else if( opt.equals( "-domain"    )) {
               type = DOMAIN;
           } else if( opt.equals( "-wellknown" )) {
               type = WELLKNOWN;
           } else if( opt.equals( "-exact"     )) {
               type = EXACT;
           } else if( opt.equals( "-default"   )) {
               type = DEFAULT;
           } else if( opt.equals( "-dumpster"  )) {
               type = DUMPSTER;
           } else if( opt.equals( "-alias"     )) {
               type = ALIAS;
           }
        
      if( args.argc() == 1 ){
        if( ( type == DEFAULT ) || ( type == DUMPSTER ) ){
           _CellRoute( null , args.argv(0) , type ) ;
        }else{
           throw new IllegalArgumentException("Not enough arguments" ) ;
        }
      }else if( args.argc() == 2 ){
        if( ( type == DEFAULT ) || ( type == DUMPSTER ) ){
           throw new IllegalArgumentException("Too many arguments" ) ;
        }else{
           _CellRoute( args.argv(0) , args.argv(1) , type ) ;
        }
      }else {
          throw new IllegalArgumentException("Too many arguments");
      }
          
   } 
   public CellRoute( String dest , String gateway , int type )
          throws IllegalArgumentException {
          
          _CellRoute( dest , gateway , type ) ;
   }
   public CellRoute( String dest , String gateway , String type )
          throws IllegalArgumentException {
          
        int i ;
        for( i = 0 ; 
             ( i < __typeNames.length ) &&
             ( ! __typeNames[i].equals( type )  )  ; i++ ) {
            ;
        }
        if( ( i == 0 ) || ( i == __typeNames.length ) ) {
            throw new IllegalArgumentException("Illegal Route Type " + type);
        }
        _CellRoute( dest , gateway , i ) ;
   }
   public void _CellRoute( String dest , String gateway , int type )
          throws IllegalArgumentException {
      splitDestination( dest ) ;
      _gateway    = gateway ;
      _type       = type ;
      switch( _type ){
        case EXACT :
        case ALIAS :
           if( _destCell == null ) {
               throw new IllegalArgumentException("No destination cell spec.");
           }
           if( _destDomain == null ) {
               _destDomain = "local";
           }
        break ;
        case WELLKNOWN :
           if( _destCell == null ) {
               throw new IllegalArgumentException("No destination cell spec.");
           }
           if( _destDomain != null ) {
               throw new IllegalArgumentException("WELLKNOWN doesn't accept domain");
           }
           _destDomain = "*" ;
        break ;
        case DOMAIN :
           if( _destDomain != null ) {
               throw new IllegalArgumentException("DOMAIN doesn't accept cell");
           }
           if( _destCell == null ) {
               throw new IllegalArgumentException("No destination domain spec.");
           }
           _destDomain = _destCell ;
           _destCell   = "*" ;
        break ;
        case DUMPSTER :
           if( _destCell != null ) {
               throw new IllegalArgumentException("DUMPSTER doesn't accept cell");
           }
           if( _destDomain != null ) {
               throw new IllegalArgumentException("DUMPSTER doesn't accept domain");
           }
           _destDomain = "*" ;
           _destCell   = "*" ;
        break ;
        case DEFAULT :
           if( _destCell != null ) {
               throw new IllegalArgumentException("DEFAULT doesn't accept cell");
           }
           if( _destDomain != null ) {
               throw new IllegalArgumentException("DEFAULT doesn't accept domain");
           }
           _destDomain = "*" ;
           _destCell   = "*" ;
        break ;
        case AUTO :
           if( ( _destCell != null ) && ( _destDomain != null ) ){
             if( _destCell.equals("*") && _destDomain.equals("*") ){
                _type = DEFAULT ;
             }else if( _destCell.equals("*") ){
                _type = DOMAIN ;
             }else if( _destDomain.equals("*") ){
                _type = WELLKNOWN ;
             }else{
                _type = EXACT ;
             }
           }else if( _destCell == null ){
             _destCell = "*" ;
             _type     = DOMAIN ;
           }else if( _destDomain == null ){
             _destDomain = "*" ;
             _type       = WELLKNOWN ;
           }else{
             _destCell   = "*" ;
             _destDomain = "*" ;
             _type       = DEFAULT ;
           }
        break ;
        default :
           throw new IllegalArgumentException( "Unknown Route type" ) ;
      
      }
   }
   public String getCellName(){   return _destCell ; }
   public String getDomainName(){ return _destDomain ; }
   public String getTargetName(){ return _gateway ; }
   
   public int    getRouteType(){  return _type ; }
   public CellAddressCore getTarget(){
   
      return new CellAddressCore( _gateway  ) ;
   }
   public String getRouteTypeName(){
      return __typeNames[_type] ;
   }
   private void splitDestination( String dest ){
       if( ( dest == null ) || ( dest.equals("") ) ){
           _destCell   = null ;
           _destDomain = null ;
           return ;
       }
       int ind = dest.indexOf( '@' ) ;
       if( ind < 0 ){
           _destCell   = dest ;
           _destDomain = null ;
       }else{
           _destCell = dest.substring( 0 , ind ) ;
           if( ind == ( dest.length() -1 ) ) {
               _destDomain = null;
           } else {
               _destDomain = dest.substring(ind + 1);
           }
       }
   
   }
   public int hashCode(){  
      return  (_destCell+_destDomain+_gateway).hashCode() ;
   } 
   public boolean equals( Object x ){
	   
	   if( !(x instanceof CellRoute) ) {
               return false;
           }
	   
      CellRoute route = (CellRoute)x ;
      return ( route._destCell.equals( _destCell ) ) &&
             ( route._destDomain.equals( _destDomain ) ) ;
   }
   private static final int _destLength   = 15 ;
   private static final int _domainLength = 15 ;
   private static final int _gateLength   = 25 ;
   public static String headerToString(){
      return Formats.field( "Dest Cell"    , _destLength   , Formats.CENTER ) +
             Formats.field( "Dest Domain"  , _domainLength , Formats.CENTER ) +
             Formats.field( "Gateway"      , _gateLength   , Formats.CENTER ) +
             Formats.field( "Type"        , 10 , Formats.CENTER );
   }
   public String toString(){
      return Formats.field( _destCell    , _destLength   , Formats.CENTER ) +
             Formats.field( _destDomain  , _domainLength , Formats.CENTER ) +
             Formats.field( _gateway     , _gateLength   , Formats.CENTER ) +
             Formats.field( __typeNames[_type]  , 10 , Formats.LEFT );
   }
   /*
   CellRoute getClone(){ 
      try { 
         return (CellRoute)this.clone() ; 
      }catch( CloneNotSupportedException cnse ){
         return null ;
      }
   }
   */
   CellRoute getClone(){ 
      CellRoute cr   = new CellRoute() ;
      cr._destCell   = _destCell ;
      cr._destDomain = _destDomain ;
      cr._gateway    = _gateway ;
      cr._type       = _type ;
      return cr ;
   }
   protected Object clone(){ 
      CellRoute cr   = new CellRoute() ;
      cr._destCell   = _destCell ;
      cr._destDomain = _destDomain ;
      cr._gateway    = _gateway ;
      cr._type       = _type ;
      return cr ;
   }
   public static void main( String [] args ){
     if( args.length < 3 ) {
         System.exit(4);
     }
     
     try{
        CellRoute route ;
        route = new CellRoute( args[0] , args[1] , args[2] ) ;
        System.out.println( CellRoute.headerToString() ) ;
        System.out.println( route.toString() ) ;
     }catch( IllegalArgumentException iae ){
        System.out.println( "exception : "+iae ) ;
     }
   }


}
 
