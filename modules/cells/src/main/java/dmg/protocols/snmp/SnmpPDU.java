package dmg.protocols.snmp ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpPDU extends SnmpSequence {
  SnmpInteger  _requestID ;
  SnmpInteger  _errorStatus ;
  SnmpInteger  _errorIndex ;
  SnmpSequence _varBindList ;
  int _type = 0 ;
  SnmpPDU( SnmpObjectHeader head , byte [] b , int offIn , int maxLen ){
      super( head , b , offIn , maxLen ) ;
      if( size() < 4 ) {
          throw new NumberFormatException("Not a PDU");
      }
      try{
         _requestID   = (SnmpInteger)objectAt(0) ;
         _errorStatus = (SnmpInteger)objectAt(1) ;
         _errorIndex  = (SnmpInteger)objectAt(2) ;
         _varBindList = (SnmpSequence)objectAt(3);
         
      }catch(Exception e ){
          throw new NumberFormatException("Not a PDU structure") ; 
      }
      _determineType() ;
  }
  public SnmpPDU( SnmpInteger id , SnmpInteger status ,
                  SnmpInteger index , SnmpSequence list   ){
      _requestID   = id ;
      _errorStatus = status ;
      _errorIndex  = index ;
      _varBindList = list ;              
      _determineType() ;
      addObject( _requestID ) ;
      addObject( _errorStatus ) ;
      addObject( _errorIndex ) ;
      addObject( _varBindList ) ;
  }
  public SnmpPDU( SnmpSequence snmp ){ 
      super( snmp ) ; 
      if( size() < 4 ) {
          throw new NumberFormatException("Not a PDU");
      }
      try{
         _requestID   = (SnmpInteger)objectAt(0) ;
         _errorStatus = (SnmpInteger)objectAt(1) ;
         _errorIndex  = (SnmpInteger)objectAt(2) ;
         _varBindList = (SnmpSequence)objectAt(3);
         
      }catch(Exception e ){
          throw new NumberFormatException("Not a PDU structure") ; 
      }
      _determineType() ;
  }
  @Override
  public byte [] getSnmpBytes(){
      removeAllObjects() ;
      addObject( _requestID ) ;
      addObject( _errorStatus ) ;
      addObject( _errorIndex ) ;
      addObject( _varBindList ) ;
      return super.getSnmpBytes(_type) ;
  }
  private void _determineType(){
      if( this instanceof SnmpGetRequest ){
        _type = SnmpObjectHeader.GetRequest ;
      }else if( this instanceof SnmpGetNextRequest ){
        _type = SnmpObjectHeader.GetNextRequest ;
      }else if( this instanceof SnmpGetResponse ){
        _type = SnmpObjectHeader.GetResponse ;
      }else if( this instanceof SnmpSetRequest ){
        _type = SnmpObjectHeader.SetRequest ;
      }else if( this instanceof SnmpTrap ){
        _type = SnmpObjectHeader.Trap ;
      }
//      System.out.println( " My type : "+_type ) ;
  }
  public SnmpInteger  getRequestID(){ return _requestID ; }
  public SnmpInteger  getErrorStatus(){ return _errorStatus ; }
  public SnmpInteger  getErrorIndex(){ return _errorIndex ; }
  public SnmpSequence getVarBindList(){ return _varBindList ; }
  
  
} 
