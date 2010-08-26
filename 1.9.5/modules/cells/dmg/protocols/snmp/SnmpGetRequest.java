package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpGetRequest extends SnmpPDU {
  
  SnmpGetRequest( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
      super( head , b , offIn , maxLen ) ;
  }
  public SnmpGetRequest( SnmpInteger id , SnmpInteger status ,
                  SnmpInteger index , SnmpSequence list   ){
      super( id , status , index ,list ) ;              
                  
  }
  public SnmpGetRequest( SnmpSequence snmp ){ super( snmp ) ; }
  public String toString(){ return toString("GetRequest") ;}
  
  public static void main( String [] args ){
     SnmpSequence   list = new SnmpSequence() ;
     list.addObject( new SnmpVarBind( 
                           new SnmpOID( "1.3.6.1.2.1.1.3" ) ,
                           new SnmpNull() ) ) ;
     SnmpGetRequest snmp = 
        new SnmpGetRequest(  new SnmpInteger( 100 ) ,
                             new SnmpInteger( 0 ) ,
                             new SnmpInteger( 0 ) ,
                             list  ) ;
                             
     byte [] b = snmp.getSnmpBytes() ;
     String output = SnmpObjectHeader._print( b , 0 , b.length ) ;
     System.out.println( output ) ;
  }
  
} 
