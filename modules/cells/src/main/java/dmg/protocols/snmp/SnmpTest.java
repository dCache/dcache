package dmg.protocols.snmp ;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;


public class SnmpTest {
 public static final int listenPort = 22112 ;
 public static void main( String [] args ){
   if( args.length < 2 ){
     try{
       DatagramSocket socket = new DatagramSocket(listenPort) ;
       DatagramPacket recPacket = new DatagramPacket(
                                        new byte[1024] , 1024 ) ;
       System.out.println( "Waiting for answer" ) ;
       socket.receive( recPacket ) ;
       System.out.println( "Tranmission finished" ) ;
       byte [] b = recPacket.getData() ;
       int len = recPacket.getLength() ;
       SnmpObjectHeader._print( b , 0 , len ) ;
     }catch( Exception eee){
       System.err.println( " Exception : "+eee ) ;
     }
   }else{
     String hostName = args[0] ;
     int port = Integer.parseInt(args[1]);
     String oid ;
     if( args.length < 3 ) {
         oid = "1.3.6.1.2.1.1.3";
     } else {
         oid = args[2];
     }

     SnmpSequence   list = new SnmpSequence() ;
     list.addObject( new SnmpVarBind(
                           new SnmpOID( oid ) ,
                           new SnmpNull() ) ) ;
     SnmpGetNextRequest getRequest =
        new SnmpGetNextRequest(  new SnmpInteger( 100 ) ,
                             new SnmpInteger( 0 ) ,
                             new SnmpInteger( 0 ) ,
                             list  ) ;

     SnmpSequence request = new SnmpSequence() ;
     request.addObject( new SnmpInteger(0) ) ;
     request.addObject( new SnmpOctetString( "public" ) ) ;
     request.addObject( getRequest ) ;

     byte [] b = request.getSnmpBytes() ;
     String output = SnmpObjectHeader._print( b , 0 , b.length ) ;
     System.out.println( "Sending to host "+hostName+" port "+port ) ;
     System.out.println( "Data : "+output ) ;
     try{
       DatagramSocket socket = new DatagramSocket() ;
       InetAddress addr = InetAddress.getByName( hostName ) ;
       DatagramPacket recPacket = new DatagramPacket(
                                        new byte[1024] , 1024 ) ;
       DatagramPacket packet = new DatagramPacket(
                                    b , b.length ,
                                    addr , port ) ;
       socket.send( packet ) ;
       System.out.println( "Waiting for answer" ) ;
       socket.receive( recPacket ) ;
       System.out.println( "Tranmission finished" ) ;
       b = recPacket.getData() ;
       int len = recPacket.getLength() ;
       System.out.println( " Result ("+len+") : "+
               SnmpObjectHeader._print( b , 0 , len ) ) ;

       SnmpObject res = SnmpObject.generate( b , 0 , len ) ;
       System.out.println( "Data : "+res ) ;

     }catch( Exception eee){
       System.err.println( " Exception : "+eee ) ;
     }
   }
 }

}
