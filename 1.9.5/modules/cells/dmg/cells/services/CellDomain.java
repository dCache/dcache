package  dmg.cells.services ;

import dmg.cells.nucleus.SystemCell;


/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellDomain {

  public static void main( String [] args ){
  
     if( args.length < 1 ){
        System.out.println( "USAGE : <domainName> [<configBase>]" ) ;
        System.exit(1);
      }
      new  SystemCell( args[0] ) ;
      
      String base = args.length > 1 ? args[1] : "/tmp/domains" ; 

      new  ConfigCell( "Config" ,  base ) ;
       
  }

}
