 package dmg.protocols.telnet ;

 import java.io.FilterOutputStream;
 import java.io.IOException;
 import java.io.OutputStream;

 /**
   *   TelnetOutputStream add additional control functionality
   *   to the output stream.
  *
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
   */
 public class TelnetOutputStream extends FilterOutputStream {
     //
     //   the telnet constants
     //
     private static final byte telnetSE   =   (-16);
     private static final byte telnetNOP  =   (-15);
     private static final byte telnetDM   =   (-14);
     private static final byte telnetBRK  =   (-13);
     private static final byte telnetIP   =   (-12);
     private static final byte telnetAO   =   (-11);
     private static final byte telnetAYT  =   (-10);
     private static final byte telnetEC   =   (-9);
     private static final byte telnetEL   =   (-8);
     private static final byte telnetGA   =   (-7);
     private static final byte telnetSB   =   (-6);
     private static final byte telnetWILL =   (-5);
     private static final byte telnetWONT =   (-4);
     private static final byte telnetDO   =   (-3);
     private static final byte telnetDONT =   (-2);
     private static final byte telnetIAC  =   (-1);
     private static final byte telnetCR   =   (0xd);
     private static final byte telnetLF   =   (0xa);
     private static final byte telnetNUL  =   (0);

     private static final byte telnetOptionEcho  = (1);

     private static final byte [] telnetBN = { telnetCR , telnetLF } ;

     private static final int cctData = 1 ;
     private static final int cctCR   = 2 ;
     private static final int cctCR2  = 3 ;
     private static final int cctCT1  = 4 ;
     private static final int cctCT2  = 5 ;
     private static final int cctSUB  = 6 ;
     private static final int cctESC  = 7 ;

     private final byte[] willEcho = {telnetIAC, telnetWILL, 3,
             telnetIAC, telnetWILL, telnetOptionEcho};

     private final byte[] wontEcho = {telnetIAC, telnetWONT, 3,
             telnetIAC, telnetWONT, telnetOptionEcho};
    //
    // class variables
    //

   public TelnetOutputStream( OutputStream out ){
      super( out ) ;
   }
   public void write( String string ) throws IOException {
      for( int i = 0 ; i < string.length() ; i++ ){
         char c = string.charAt(i) ;
         if( c == '\n' ) {
             super.write(telnetBN);
         } else {
             super.write(c);
         }
      }
   }
   public void setEcho( boolean echo ){
      try{ super.write( echo ? wontEcho : willEcho ) ;
      }catch( Exception e ){}
   }


}
