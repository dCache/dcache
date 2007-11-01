package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpTrap extends SnmpPDU {
  
  SnmpTrap( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
      super( head , b , offIn , maxLen ) ;
  }
  public String toString(){ return toString("Trap") ;}
  public SnmpTrap( SnmpInteger id , SnmpInteger status ,
                  SnmpInteger index , SnmpSequence list   ){
      super( id , status , index ,list ) ;              
                  
  }
  public SnmpTrap( SnmpSequence snmp ){ super( snmp ) ; }
  
  
} 
