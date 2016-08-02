package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpIpNumber extends SnmpInteger {

  SnmpIpNumber( SnmpObjectHeader head , byte [] b , int offIn , int maxLen )
    throws NumberFormatException {
       super( head , b , offIn , maxLen ) ;
  }
  public String toString(){
    long v = longValue() ;
    return ((v >> 24) & 0xff) + "." + ((v >> 16) & 0xff) + '.' + ((v >> 8) & 0xff) + '.' + ((v) & 0xff);
  }
} 
