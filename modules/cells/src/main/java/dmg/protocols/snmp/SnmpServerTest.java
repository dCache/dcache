package dmg.protocols.snmp ;

import java.net.SocketException;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpServerTest implements SnmpEventListener {

    SnmpServerTest( int port ) throws SocketException {
         SnmpServer server = new SnmpServer( port ) ;
         server.addSnmpEventListener( this ) ;
    }
    @Override
    public SnmpRequest snmpEventArrived( SnmpEvent event ){
       SnmpRequest request = event.getSnmpRequest() ;
       String communityString = request.getCommunity().toString() ;
       int count = request.varBindListSize() ;
       int type  = request.getRequestType() ;
       say( "Request from "+event.getSourceAddress() ) ;
       say( "Community :  "+communityString ) ;
       say( "Count     :  "+count ) ;
       say( "Type      :  "+type ) ;
       if( ! communityString.equals("public") ) {
           return null;
       }
       SnmpRequest response = new SnmpRequest(
                                 new SnmpOctetString( "public" ) ,
                                 new SnmpInteger( request.getRequestID().longValue() ) ,
                                 new SnmpInteger( 0 ) ,
                                 new SnmpInteger( 0 )    ) ;
       say( "response\n"+response);
       for( int i = 0 ; i < count ; i++ ){
          SnmpOID     oid      = request.varBindOIDAt(i) ;

          if( type == SnmpObjectHeader.GetNextRequest ){
             int [] vector = oid.getVector() ;
             vector[vector.length-1] ++ ;
             oid = new SnmpOID( vector ) ;
          }

          response.addVarBind( oid ,
                               new SnmpOctetString( "Cell OS 4.3 "+oid ) ) ;

       }
       return response ;


    }
    private void say( String str ){
      System.out.println( str ) ;
    }
    public static void main( String [] args ){
      if( args.length < 1 ){
        System.err.println( " USAGE : SnmpServerTest <port>" ) ;
        System.exit(4);
      }
      try{
         new SnmpServerTest(Integer.parseInt(args[0])) ;
      }catch( Exception e ){
         System.err.println( "Exception ; "+e ) ;
         System.exit(4);
      }

    }

}
