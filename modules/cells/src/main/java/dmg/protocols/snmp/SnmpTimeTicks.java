package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpTimeTicks extends SnmpInteger {

  SnmpTimeTicks( SnmpObjectHeader head , byte [] b , int offIn , int maxLen )
    throws NumberFormatException {
       super( head , b , offIn , maxLen ) ;
  }
  public String toString(){
    long v = longValue() ;
    int msec = (int) ( v % 100 ) ; v /= 100 ;
    int sec  = (int) ( v % 60  ) ; v /= 60 ;
    int min  = (int) ( v % 60  ) ; v /= 60 ;
    int hour = (int) ( v % 24  ) ; v /= 24 ;
    int days = (int) v ;
    return days + " Days " + hour + " Hours " + min + " Minutes " + sec + ":" + msec + " Seconds";
  }
}  
