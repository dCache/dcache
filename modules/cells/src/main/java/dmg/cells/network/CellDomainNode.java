package dmg.cells.network ;
import  dmg.cells.nucleus.* ;
import  java.io.Serializable ;
/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */

public class CellDomainNode implements Serializable {
   static final long serialVersionUID = 1165416552852548445L;
   private String _name ;
   private String _address ;
   private CellTunnelInfo [] _infos = null ;
   
   public CellDomainNode( String name , String address ){
       _name = name ; 
       _address = address ;
   }
   public String getName(){ return _name ; }
   public String getAddress(){ return _address ; }
   public void setLinks( CellTunnelInfo [] infos ){
       _infos = infos ;
   }
   public CellTunnelInfo [] getLinks(){ return _infos ; }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      sb.append(_name).append("   address : ").append(_address).append("\n");
      if( _infos != null ){
         for( int i = 0 ; i < _infos.length ; i++ ){
             String domain = _infos[i].getRemoteCellDomainInfo().
                                     getCellDomainName() ;
             sb.append("        -> ").append(domain).append("\n");
         }
      
      }
      return sb.toString() ;
   
   }
}
