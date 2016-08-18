package dmg.cells.nucleus ;

import java.io.Serializable;
import java.util.Date;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.2, 14 Mar 2001
  */
public class CellInfo implements Serializable {
  private static final long serialVersionUID = 8837418209418282912L;

  private String _cellName     = "Unknown" ;
  private String _cellType     = "Unknown" ;
  private String _cellClass    = "Unknown" ;
  private String _domainName   = "Unknown" ;
  private Date   _creationTime;
  private String _shortInfo    = "NA" ;
  private String _privateInfo  = "NA" ;
  private int    _state;
  private int    _eventQueueSize;
  private int    _threadCount;
  private long   _expectedQueueTime;
  private CellVersion _version = new CellVersion() ;

  static final  int    INITIAL  =  0;
  static final  int    ACTIVE   =  1;
  static final  int    REMOVING =  2;
  static final  int    DEAD     =  3;

  private static final String [] _stateNames =
          { "Initial" , "Active" , "Removing" , "Dead" , "Unknown" } ;

  public CellInfo(){}

  public CellInfo( CellInfo info ){
     _cellName       = info._cellName ;
     _cellType       = info._cellType ;
     _cellClass      = info._cellClass ;
     _domainName     = info._domainName ;
     _creationTime   = info._creationTime ;
     _shortInfo      = info._shortInfo ;
     _privateInfo    = info._privateInfo ;
     _state          = info._state ;
     _eventQueueSize = info._eventQueueSize ;
     _expectedQueueTime = info._expectedQueueTime;
     _threadCount    = info._threadCount ;
     _version        = info._version ;
  }

  public void setCellName(String name ){     _cellName     = name ; }
  public void setCellType( String type ){    _cellType     = type ; }
  public void setCellClass( String info ){   _cellClass    = info ; }
  public void setCellVersion( CellVersion version ){ _version = version ; }
  public void setDomainName( String name ){  _domainName   = name ; }
  public void setCreationTime( Date date ){  _creationTime = date ; }
  public void setPrivateInfo( String info ){ _privateInfo  = info ; }
  public void setShortInfo( String info ){   _shortInfo    = info ; }
  public void setEventQueueSize( int size ){ _eventQueueSize = size ; }
  public void setExpectedQueueTime(long millis) { _expectedQueueTime = millis; }
  public void setThreadCount( int threadCount ){ _threadCount = threadCount ; }
  public void setState( int state ){
     _state = ( state < 0 ) || ( _state >= _stateNames.length )  ?
              _stateNames.length : state  ;
  }

  public int getState() {
      return _state;
  }

  public String getStateName() {
      return _stateNames[_state];
  }
  //
  // and now the public getter's
  //
  public String toString(){
     return f( _cellName              , 20 ) +
            f( _stateNames[_state].substring(0,1) , 2 ) +
            f(String.valueOf(_eventQueueSize), 3 ) +
            f(String.valueOf(_threadCount), 3 ) +
            f( cutClass( _cellClass ) , 20 ) +
            _shortInfo ;
  }
  public CellVersion getCellVersion(){ return _version ; }
  public String getPrivatInfo(){ return _privateInfo ; }
  public int    getEventQueueSize(){ return _eventQueueSize ; }
  public long   getExpectedQueueTime(){ return _expectedQueueTime; }
  public String getCellName(){ return _cellName ; }
  public String getCellType(){ return _cellType ; }
  public String getCellClass(){ return _cellClass ; }
  public String getShortInfo(){ return _shortInfo ; }
  public Date   getCreationTime(){ return _creationTime ; }
  public String getDomainName(){ return _domainName ; }
  public int    getThreadCount(){ return _threadCount ; }
  //
  // and some needfull things
  //
  public static String f( String in , int field ){ return f(in,field,0) ;}
  public static String f( String in , int field , int flags ){
    if( in.length() >= field ) {
        return in;
    }
    StringBuilder sb = new StringBuilder() ;
    sb.append( in ) ;
    int diff = field - in.length() ;
    for( int i = 0 ; i < diff ; i++ ) {
        sb.append(' ');
    }
    return sb.toString() ;
  }
  public static String cutClass( String c ){
     int lastDot = c.lastIndexOf( '.' ) ;
     if( ( lastDot < 0 ) || ( lastDot >= ( c.length() - 1 ) ) ) {
         return c;
     }
     return c.substring( lastDot+1 ) ;

  }
}
