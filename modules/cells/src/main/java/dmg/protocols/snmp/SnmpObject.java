package dmg.protocols.snmp ;

 /**
  *  The SnmpObject is the basice snmp datatype which offers
  *  a static method to create all Snmp Version 1 data types
  *  out of the byte stream represeting an snmp request or
  *  response. In addition it enforces all extending SnmpXXX
  *  classes to implement 'getSnmpBytes' which decodes the 
  *  internal representation into the network representation.
  *  See <a href=guide/Guide-dmg.snmp.html>Guide to dmg.snmp</a>
  *  for more informations.
  *
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  * 
  */
abstract class SnmpObject {

  private int _codedLength;
  
  abstract byte [] getSnmpBytes() ;
  
  public static SnmpObject generate( byte [] b , int offIn , int len )
         throws NumberFormatException {
     SnmpObjectHeader header = new SnmpObjectHeader( b , offIn , len ) ;
     SnmpObject snmp;
     switch( header.getType() ){
        case SnmpObjectHeader.OBJECT_IDENTIFIER :
          snmp = new SnmpOID( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.NULL :
          snmp = new SnmpNull( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.IpAddress :
          snmp = new SnmpIpNumber( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.TimeTicks :
          snmp = new SnmpTimeTicks( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.INTEGER :
        case SnmpObjectHeader.Counter :
        case SnmpObjectHeader.Gauge :
        case SnmpObjectHeader.Opaque :
          snmp = new SnmpInteger( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.OCTET_STRING :
          snmp = new SnmpOctetString( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.SEQUENCE :
          snmp = new SnmpSequence( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.GetRequest :
          snmp = new SnmpGetRequest( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.GetNextRequest :
          snmp = new SnmpGetNextRequest( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.SetRequest :
          snmp = new SnmpSetRequest( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.GetResponse :
          snmp = new SnmpGetResponse( header , b , offIn , len ) ;
        break ;
        case SnmpObjectHeader.Trap :
          snmp = new SnmpTrap( header , b , offIn , len ) ;
        break ;
        default : throw new NumberFormatException()  ;
     }
     return snmp ;
  }
  public int getCodedLength(){ return _codedLength ; }
  public void setCodedLength( int len ){ _codedLength = len ; } 
  public static void main( String [] args ){
      if( args.length == 0 ) {
          System.exit(4);
      }
      byte [] b = new byte[args.length] ;
      int x ;
      for( int i = 0 ; i < args.length ; i++ ){
          x = Integer.valueOf(args[i], 16);
          x &= 0xff ;
          x = x > 127 ? ( x - 256 ) : x ;
          b[i] =  (byte)(x > 127 ? ( x - 256 ) : x ) ;
      }
      String output  =  SnmpObjectHeader._print( b , 0 , b.length ) ;
      System.out.println( output ) ;
      SnmpObject snmp = SnmpObject.generate( b , 0 , b.length ) ;
      System.out.println( " Got class  : "+snmp.getClass().getName() ) ;
      System.out.println( " CodeLength : "+snmp.getCodedLength() ) ;
      System.out.println(" Value      : \n" + snmp) ;
      b = snmp.getSnmpBytes() ;
      output  =  SnmpObjectHeader._print( b , 0 , b.length ) ;
      System.out.println( output ) ;
//      snmp = new SnmpOID( snmp.toString() ) ;
//      b = snmp.getSnmpBytes() ;
//      output  =  SnmpObjectHeader._print( b , 0 , b.length ) ;
//      System.out.println( output ) ;
 
  }
}
