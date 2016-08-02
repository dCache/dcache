// $Id: LoginManagerChildrenInfo.java,v 1.4 2007-07-23 09:13:34 tigran Exp $

package dmg.cells.services.login ;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


/*
 * Immutable
 */
public class LoginManagerChildrenInfo implements Serializable {

	private static final long serialVersionUID = -8759763067828034558L;

   private final String _cellName ;
   private final String _cellDomainName ;
   private final String [] _children    ;

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
   public List<String> getChildren(){ return Arrays.asList(_children) ;}
   public int getChildrenCount(){ return _children.length ; }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      sb.append("Cell=").append(_cellName).
         append(";Domain=").append(_cellDomainName).append(';') ;
      if( _children == null ) {
          return sb.toString();
      }
       for (String child : _children) {
           sb.append(child).append(';');
       }
      return sb.toString() ;
   }
}
