// $Id: LoginBrokerInfo.java,v 1.4 2006-12-15 09:56:43 tigran Exp $

package dmg.cells.services.login ;

import java.io.Serializable;

public class LoginBrokerInfo implements Serializable {

   private static final long serialVersionUID = 4077557054990432737L;

   private String _cellName ;
   private String _domainName ;
   private String _protocolFamily ;
   private String _protocolVersion ;
   private String _protocolEngine ;
   private String _root;

   private String [] _hosts = new String[0] ;
   private int       _port;

   private double _load;

   private long   _update = -1 ;

   public LoginBrokerInfo( String cellName ,
                           String domainName ,
                           String protocolFamily ,
                           String protocolVersion ,
                           String protocolEngine,
                           String root){
      _cellName        = cellName ;
      _domainName      = domainName ;
      _protocolFamily  = protocolFamily ;
      _protocolVersion = protocolVersion ;
      _protocolEngine  = protocolEngine ;
      _root            = root;
   }

   public String getHost(){ return _hosts.length == 0 ? "" : _hosts[0] ; }
   public String [] getHosts(){ return _hosts ; }
   public int getPort(){ return _port ; }
   public String getCellName(){ return _cellName ; }
   public String getDomainName(){ return _domainName ; }
   public String getProtocolFamily(){ return _protocolFamily ; }
   public String getProtocolVersion(){ return _protocolVersion  ; }
   public String getProtocolEngine(){ return _protocolEngine ; }
   public String getRoot() { return _root; }
   public double getLoad(){ return _load ; }
   public long   getUpdateTime(){ return _update ; }

   public void setUpdateTime( long update ){ _update = update ; }
   public void setLoad( double load ){ _load = load ; }
   public void setHosts( String [] hosts ){ _hosts = hosts ; }
   public void setPort( int port ){ _port = port ; }
   public String getIdentifier(){
     return _cellName + "@" + _domainName ;
   }
   public boolean equals( Object obj ){

	  if( !(obj instanceof LoginBrokerInfo) ) {
              return false;
          }
      LoginBrokerInfo info = (LoginBrokerInfo)obj ;
      return _cellName.equals(info._cellName) &&  _domainName.equals(info._domainName ) ;
   }
   public int hashCode(){
     return ( _cellName + "@" + _domainName ).hashCode() ;
   }
   public String toString(){
     int          pos    = _protocolEngine.lastIndexOf('.') ;
     String       engine =
        ( pos < 0 ) || ( pos == ( _protocolEngine.length() - 1 ) ) ?
        _protocolEngine : _protocolEngine.substring(pos+1) ;

     StringBuilder sb     = new StringBuilder() ;

     sb.append(_cellName).append("@").append(_domainName).append(";") ;
     sb.append(engine).append(";") ;
     sb.append("{").append(_protocolFamily).append(",").
        append(_protocolVersion).append("};");

     sb.append("[");
     for( int i = 0 ; i < (_hosts.length-1) ; i++ ) {
         sb.append(_hosts[i]).append(",");
     }
     if( _hosts.length > 0 ) {
         sb.append(_hosts[_hosts.length - 1]);
     }
     sb.append(":").append(_port).append("]").append(";");
     sb.append("<");
     sb.append((int)(_load*100.)).append(",") ;
     sb.append(_update).append(">;") ;

     return sb.toString();
   }
}
