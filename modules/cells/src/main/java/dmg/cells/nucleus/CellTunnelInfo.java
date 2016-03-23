package dmg.cells.nucleus ;

import java.io.Serializable;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
/*
 * @Immutable
 */
public class CellTunnelInfo implements Serializable {

   private static final long serialVersionUID = 6337314695599159656L;

   private final CellDomainInfo _remote ;
   private final CellDomainInfo _local ;
   private final String _tunnelName ;

   public CellTunnelInfo( String tunnelName ,
                          CellDomainInfo local ,
                          CellDomainInfo remote ){
      _remote     = remote ;
      _local      = local ;
      _tunnelName = tunnelName ;

   }
   public CellDomainInfo getRemoteCellDomainInfo(){ return _remote ; }
   public CellDomainInfo getLocalCellDomainInfo(){  return _local ; }
   public String getTunnelName() { return _tunnelName; }
   public String toString(){
      return _tunnelName+" L["+(_local!=null?_local.toString():"Unknown")+
             "];R["+(_remote!=null?_remote.toString():"Unknown")+"]" ;
   }
}
