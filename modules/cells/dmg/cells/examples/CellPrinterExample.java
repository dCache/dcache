 package dmg.cells.examples ;
 import java.util.* ;
 import dmg.util.* ;
 import java.text.*;
 
//
//
/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellPrinterExample  implements dmg.cells.nucleus.CellPrinter {

   private long       _sequence = 0 ;
   private Args       _args = null ;
   private Dictionary _dict = null ;
   private DateFormat _df   = new SimpleDateFormat("MM/dd HH:mm:ss" ) ;
   
   public CellPrinterExample( Args args , Dictionary dict ){
       _args = args ;
       _dict = dict ;
       String tmp = _args.getOpt("count") ;
       if( tmp != null )_sequence = Long.parseLong( tmp ) ;
   }
   public void say( String cellName ,
                    String domainName ,
                    String cellType ,
                    int level , 
                    String msg ){
                    
                    
      System.out.println(_df.format(new Date()) +" "+(_sequence++)+" "+cellType+"("+cellName+"@"+domainName+") ["+level+"] "+msg );
   }

}

