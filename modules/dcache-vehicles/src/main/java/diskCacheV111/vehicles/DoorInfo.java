// $Id: DoorInfo.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;

import java.io.IOException;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class DoorInfo implements Serializable {

   private String _cellName ;
   private String _cellDomainName ;
   private String _protocolFamily  = "<unknown>" ;
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

      _protocolFamily = checkNotNull(protocolFamily);
      _protocolVersion = checkNotNull(protocolVersion);
   }

   public String getCellName(){ return _cellName ; }
   public String getDomainName(){ return _cellDomainName ; }
   public void setOwner( String owner ){ _owner = checkNotNull(owner); }
   public void setProcess( String process ){ _process = checkNotNull(process); }

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

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        _cellDomainName = _cellDomainName.intern();
        _protocolFamily = _protocolFamily.intern();
        _protocolVersion = _protocolVersion.intern();
        _owner = _owner.intern();
        _process = _process.intern();
    }
}
