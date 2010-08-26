package dmg.protocols.snmp ;

public class SnmpSetRequest extends SnmpPDU {
  
  SnmpSetRequest( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
      super( head , b , offIn , maxLen ) ;
  }
  public String toString(){ return toString("SetRequest") ;}
  public SnmpSetRequest( SnmpInteger id , SnmpInteger status ,
                  SnmpInteger index , SnmpSequence list   ){
      super( id , status , index ,list ) ;              
                  
  }
  public SnmpSetRequest( SnmpSequence snmp ){ super( snmp ) ; }
  
  
} 
