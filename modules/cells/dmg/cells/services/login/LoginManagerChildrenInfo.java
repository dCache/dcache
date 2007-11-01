// $Id: LoginManagerChildrenInfo.java,v 1.2 2005-03-08 15:37:16 patrick Exp $

package dmg.cells.services.login ;
import java.util.List ;
import java.util.ArrayList ;
import java.util.Arrays;

public class LoginManagerChildrenInfo implements java.io.Serializable {
   static final long serialVersionUID = -8759763067828034558L;
   private String _cellName       = null ;
   private String _cellDomainName = null ;
   private String [] _children    = null ;
   public LoginManagerChildrenInfo( String cellName ,
                                    String cellDomainName ,
                                    String [] childrenNames ){
      _cellName = cellName ;
      _cellDomainName = cellDomainName ;
      _children = new String[childrenNames.length] ;
      System.arraycopy( childrenNames , 0 , 
                        _children , 0 , 
                        childrenNames.length ) ;
   }
   public String getCellName(){ return _cellName ; }
   public String getCellDomainName(){ return _cellDomainName ; }
   public List getChildren(){ return Arrays.asList(_children) ;}
   public int getChildrenCount(){ return _children.length ; }
   public String toString(){
      StringBuffer sb = new StringBuffer() ;
      sb.append("Cell=").append(_cellName).
         append(";Domain=").append(_cellDomainName).append(";") ;
      if( _children == null )return sb.toString() ;
      for( int i = 0 ; i < _children.length ; i++ )
         sb.append(_children[i]).append(";");
      return sb.toString() ;
   }
}
