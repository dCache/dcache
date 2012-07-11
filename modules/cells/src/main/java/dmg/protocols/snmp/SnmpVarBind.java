package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpVarBind extends SnmpSequence {
  SnmpOID     _id ;
  SnmpObject  _value ;
  
  SnmpVarBind( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
      super( head , b , offIn , maxLen ) ;
      if( size() < 4 ) {
          throw new NumberFormatException("Not a VarBind");
      }
      try{
         _id    = (SnmpOID)objectAt(0) ;
         _value = (SnmpObject)objectAt(1) ;
         
      }catch(Exception e ){
          throw new NumberFormatException("Not a VarBind structure") ; 
      }
  }
  public SnmpVarBind( SnmpOID id , SnmpObject value ){
     _id = id ;
     _value = value ;
  }
  public byte [] getSnmpBytes(){ 
      removeAllObjects() ;
      addObject( _id ) ;
      addObject( _value ) ;
      return super.getSnmpBytes() ;
  }
  public SnmpOID     getObjectID(){ return _id ; }
  public SnmpObject  getValue(){ return _value ; }
  public static void main( String [] args ){
     SnmpVarBind varBind = new SnmpVarBind( new SnmpOID( "1.3.6.1.2" ) ,
                                            new SnmpNull()  ) ;
     byte [] b = varBind.getSnmpBytes() ;
     String  x = SnmpObjectHeader._print( b , 0 , b.length ) ;
     System.out.println( x ) ;
  
  }
  
} 
