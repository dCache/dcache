package dmg.protocols.snmp ;

import java.util.Vector;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpSequence extends SnmpObject {
  private Vector<SnmpObject> _vector = new Vector<>() ;

  SnmpSequence( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
    int off  = offIn + head.getCodedLength()  ;
    int len  = head.getLength() ;
    int rest = len ;
    int cl ;
    SnmpObject snmp ;
    while( rest > 0 ){
        snmp = SnmpObject.generate( b , off , rest ) ;
        _vector.addElement( snmp ) ;
        cl    = snmp.getCodedLength() ;
        rest -= cl ;
        off  += cl ;
    }
    setCodedLength( head.getCodedLength() + len ) ;
  }
  public SnmpSequence(){}
  public SnmpSequence( SnmpSequence snmp ){
     _vector = snmp._vector ;
  }
  public void removeAllObjects(){ _vector.removeAllElements() ; }
  public void addObject( SnmpObject snmp ){ _vector.addElement(snmp) ; }
  public SnmpObject objectAt(int i ){
      return (_vector.elementAt(i));
  }
  public int size(){ return _vector.size() ; }
  public String toString(){ return toString("Sequence") ;}
  public String toString( String type ){
     SnmpObject snmp;
     StringBuilder sb = new StringBuilder();
     sb.append(type).append('\n');
     for( int i = 0 ; i < _vector.size() ; i++ ){
        snmp = _vector.elementAt(i);
        sb.append("Class : ").append(snmp.getClass().getName());
        if( snmp instanceof SnmpSequence ){
           sb.append("Value : \n").append(snmp).append('\n');
        }else{
           sb.append("Value : ").append(snmp).append('\n');
        }

     }
     return sb.toString();
  }
  @Override
  public byte [] getSnmpBytes(){
      return getSnmpBytes(SnmpObjectHeader.SEQUENCE);
  }
  protected byte [] getSnmpBytes( int type ){
     int       s   = _vector.size();
     byte [][] v   = new byte[s][] ;
     int  total    = 0 ;

     for( int i = 0 ; i < s ; i++ ){
        v[i]   = (_vector.elementAt(i)).getSnmpBytes() ;
        total += v[i].length ;
     }
     SnmpObjectHeader head = new SnmpObjectHeader(
                               type ,
                               total ) ;
     int headLen = head.getCodedLength() ;
     byte [] out = new byte[headLen+total] ;

     System.arraycopy( head.getSnmpBytes() , 0 , out , 0 , headLen ) ;
     int pos = headLen ;
     for( int i = 0 ; i < s ; i++){
         System.arraycopy( v[i] , 0 , out , pos , v[i].length ) ;
         pos += v[i].length ;
     }
     return out ;
  }

}
