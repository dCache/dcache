package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpRequest extends SnmpSequence {
  private SnmpPDU      _pdu ;
  private SnmpSequence _varBindList ;
  private SnmpOctetString _community ;
  private int _type;
  public SnmpRequest( SnmpObject snmp ){
      super( (SnmpSequence)snmp ) ;
      try{
        _community   = (SnmpOctetString)objectAt(1) ;
        _pdu         = (SnmpPDU)objectAt(2) ;
        _varBindList = _pdu.getVarBindList() ;
      }catch( Exception e ){
        throw new NumberFormatException("Not a SnmpRequest" ) ;
      }
      if( _pdu instanceof SnmpGetRequest ){
        _type = SnmpObjectHeader.GetRequest ;
      }else if(  _pdu instanceof SnmpGetNextRequest ){
        _type = SnmpObjectHeader.GetNextRequest ;
      }
  }
  public SnmpRequest( SnmpOctetString community ,
                      SnmpInteger     requestID ,
                      int type                    ){
      _type = type ;            
      addObject( new SnmpInteger(0) ) ;
      addObject( _community  =  community ) ;
      _varBindList = new SnmpSequence() ;
      if( type == SnmpObjectHeader.GetRequest ){
         _pdu = new SnmpGetRequest ( requestID ,
                                     new SnmpInteger(0) ,
                                     new SnmpInteger(0) ,
                                     _varBindList         ) ;           
      }else if( type == SnmpObjectHeader.GetNextRequest ){
         _pdu = new SnmpGetNextRequest ( requestID ,
                                         new SnmpInteger(0) ,
                                         new SnmpInteger(0) ,
                                         _varBindList         ) ; 
      }else if( type == SnmpObjectHeader.GetResponse ){
         _pdu = new SnmpGetResponse ( requestID ,
                                         new SnmpInteger(0) ,
                                         new SnmpInteger(0) ,
                                         _varBindList         ) ; 
      }
      addObject( _pdu ) ;
//      System.out.println( " SnmpRequest end :\n"+_pdu ) ;
              
  }
  public SnmpRequest( SnmpOctetString community ,
                      SnmpInteger     requestID ,
                      SnmpInteger     errorStatus ,
                      SnmpInteger     errorIndex     ){
                      
      _type = SnmpObjectHeader.GetResponse ;            
      addObject( new SnmpInteger(0) ) ;
      addObject( _community  =  community ) ;
      _varBindList = new SnmpSequence() ;
      _pdu = new SnmpGetResponse ( requestID ,
                                   errorStatus ,
                                   errorIndex ,
                                   _varBindList         ) ; 
     
      addObject( _pdu ) ;
//      System.out.println( " SnmpRequest end :\n"+_pdu ) ;
              
  }
  public void addVarBind( SnmpOID oid , SnmpObject value ){
     SnmpSequence seq = new SnmpSequence() ;
     seq.addObject( oid ) ;
     seq.addObject( value ) ;
     _varBindList.addObject( seq ) ;
//      System.out.println( " SnmpRequest end :\n"+_pdu ) ;
  }                    
  public SnmpSequence getVarBindList(){ return _varBindList ; }
  public SnmpInteger  getRequestID(){   return _pdu.getRequestID() ; }
  public int varBindListSize(){      return _varBindList.size() ; }
  public SnmpOID varBindOIDAt(int i ){
     SnmpSequence varBind = (SnmpSequence)_varBindList.objectAt(i) ;
     return (SnmpOID)varBind.objectAt(0) ;
  }
  public SnmpInteger getErrorStatus(){ return _pdu.getErrorStatus() ; }
  public SnmpInteger getErrorIndex(){ return _pdu.getErrorIndex() ; }
  public int getRequestType(){ return _type ; } 
 
  public SnmpObject varBindValueAt(int i ){
     SnmpSequence varBind = (SnmpSequence)_varBindList.objectAt(i) ;
     return varBind.objectAt(1) ;
  }
  public SnmpOctetString getCommunity(){ return _community ; }
  public String toString(){
      return "SnmpRequest , Version " + objectAt(0) +
             " , Community " + objectAt(1) +
             '\n' +
             _pdu.toString();
                
  }

} 
