// $Id: PoolCellInfo.java,v 1.9 2006-09-25 15:37:25 tigran Exp $

package diskCacheV111.pools ;

import java.util.* ;
import dmg.cells.nucleus.CellInfo ;

public class PoolCellInfo
       extends CellInfo
       implements java.io.Serializable {

   static final long serialVersionUID = -6302825387581672484L;

   private int          _errorCode;
   private String       _errorMessage = "" ;
   private PoolCostInfo _costInfo;
   private Map          _tagMap;

   public PoolCellInfo( CellInfo info ){
      super(info) ;
   }
   public void setPoolCostInfo( PoolCostInfo costInfo ){
      _costInfo = costInfo ;
   }
   public PoolCostInfo getPoolCostInfo(){
      return _costInfo ;
   }
   public void setTagMap( Map tagMap ){ _tagMap = new HashMap( tagMap ) ; }
   public Map getTagMap(){ return _tagMap == null ? new HashMap() : _tagMap ; }
   public void setErrorStatus( int errorCode , String errorMessage ){
      _errorCode    = errorCode ;
      _errorMessage = errorMessage ;
   }
   public int  getErrorCode(){ return _errorCode ; }
   public String getErrorMessage(){ return _errorMessage ; }

   public String toString(){
     StringBuilder sb = new StringBuilder() ;
     sb.append(super.toString()) ;
     if( _costInfo != null ) {
         sb.append(";Cost={").append(_costInfo.toString()).append("}");
     }
     if( _tagMap != null ) {
         sb.append("TagMap={").append(_tagMap.toString()).append("}");
     }

     return sb.toString() ;
   }
}
