// $Id: RestoreHandlerInfo.java,v 1.4 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

import java.io.Serializable;

public class RestoreHandlerInfo implements Serializable {
   private final String _name ;
   private final int    _clientCount;
   private final int    _retryCount;
   private final long   _started ;
   private final String _pool;
   private final String _status ;
   private final int    _errorCode ;
   private final String _errorMessage ;

   private static final long serialVersionUID = 5001829329121454794L;

   public RestoreHandlerInfo(
               String name ,
               int clientCount ,
               int retryCount ,
               long started ,
               String pool ,
               String status ,
               int errorCode ,
               String errorMessage ){
      _name         = name ;
      _clientCount  = clientCount ;
      _retryCount   = retryCount ;
      _started      = started ;
      _pool         = pool ;
      _status       = status ;
      _errorCode    = errorCode ;
      _errorMessage = errorMessage ;
   }
   public String getName(){ return _name ; }
   public int getClientCount(){ return _clientCount ; }
   public int getRetryCount(){ return _retryCount ; }
   public long getStartTime(){return _started ; }
   public String getPool(){ return _pool ; }
   public String getStatus(){ return _status ; }
   public int getErrorCode(){ return _errorCode ; }
   public String getErrorMessage(){ return _errorMessage ; }
   public String toString(){
     return _name +
            " m=" + _clientCount +
            " [" + _retryCount + "] " +
            " [" + _pool + "] " +
            _status +
            " {" + _errorCode + ',' + _errorMessage + '}';
   }
}
