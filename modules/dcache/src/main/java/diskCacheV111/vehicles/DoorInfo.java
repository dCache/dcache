// $Id: DoorInfo.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;

import java.io.Serializable;

public class DoorInfo implements Serializable {

   private String _cellName ;
   private String _cellDomainName ;
   private String _protocolFamily  = "<unkown>" ;
   private String _protocolVersion = "<unknown>" ;
   private String _owner           = "<unknown>" ;
   private String _process         = "<unknown>" ;
   private Object _detail;

   private static final long serialVersionUID = 8147992359534291288L;

   public DoorInfo( String cellName , String cellDomainName ){
      _cellName       = cellName ;
      _cellDomainName = cellDomainName ;
   }
   public void setProtocol( String protocolFamily ,
                            String protocolVersion  ){

      _protocolFamily = protocolFamily ;
      _protocolVersion = protocolVersion ;
   }

   public String getCellName(){ return _cellName ; }
   public String getDomainName(){ return _cellDomainName ; }
   public void setOwner( String owner ){ _owner = owner ; }
   public void setProcess( String process ){ _process = process ; }

   public String getProtocolFamily(){ return _protocolFamily ; }
   public String getProtocolVersion(){ return _protocolVersion ; }
   public String getOwner(){ return _owner ; }
   public String getProcess(){ return _process ; }

   public void setDetail( Serializable detail ){ _detail = detail ; }
   public Serializable getDetail(){ return (Serializable) _detail ; }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      sb.append(_cellName).append("@").append(_cellDomainName).
         append(";p=").append(_protocolFamily).
         append("-").append(_protocolVersion).
         append(";o=").append(_owner).append("/").append(_process).
         append(";");
      return sb.toString();
   }
}
