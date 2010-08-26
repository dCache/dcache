package  dmg.cells.examples ;

import   dmg.cells.nucleus.* ;
import   dmg.util.* ;
import   dmg.cells.network.* ;

import java.util.* ;
import java.io.* ;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class AnyServer {

  public static void main( String [] args ){
     if( args.length < 2 ){
        System.out.println( "USAGE : <domainName> <tunnelPort> [<telnetPort>]" ) ;
        System.exit(43);
      }
      new  SystemCell( args[0] ) ; 
      new  GNLCell( "tunnel" ,  "dmg.cells.network.SimpleTunnel "+args[1] ) ;
      if( args.length > 2 )
         new  GNLCell( "telnet" ,  "dmg.cells.services.TelnetShell "+args[2] ) ;
     
  
  
  }

}
