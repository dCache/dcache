package dmg.apps.psdl.vehicles ; 

import dmg.apps.psdl.pnfs.* ;
import java.io.* ;

public class PsdlCoreRequest implements Serializable {

    private static long   __requestIdCounter = 0 ;
    private static Object __staticLock = new Object() ;
    private static Object __getNextId(){
        synchronized( __staticLock ){
           __requestIdCounter ++ ;
           return new Long( __requestIdCounter ) ;
        }
    }
    private Object    _id             = __getNextId() ;
    private String    _type           = null ;
    private PnfsId    _pnfsId         = null  ;
    private PnfsId    _pnfsParentId   = null  ;
    private String    _requestCommand = "" ;
    
    private long      _timeout       = 10 * 60 * 1000 ;
    
    private int       _returnCode    = 0 ;
    private String    _returnMessage = "" ;

    public PsdlCoreRequest( String type , PnfsId id  ){
       _pnfsId = id ;
       _type   = type ;
    }
    public PsdlCoreRequest( String type , PnfsId id , PnfsId parentId ){
       _pnfsId       = id ;
       _pnfsParentId = parentId ;
       _type         = type ;
    }
    public Object getId(){ return _id ; }
    public String getRequestType(){ return _type ; }
    public PnfsId getPnfsId(){ return _pnfsId ; }
    public PnfsId getPnfsParentId(){ return _pnfsParentId ; }
    public int    getReturnCode(){ return _returnCode ; }
    public String getReturnMessage(){ return _returnMessage ; }
    public String getRequestCommand(){ return _requestCommand ; }
    public long   getTimeout(){ return _timeout ; }
    public void   setRequestType( String type ){ _type = type ; }
    public void   setTimeout( long timeout ){ _timeout = timeout ; }
    public void   setReturnValue( int code , String message ){
       _returnCode    = code ;
       _returnMessage = message ;
    }
    public void   setRequestCommand( String command ){ 
        _requestCommand = command ;
    }
    public String toBaseString(){
       return "Req-"+_id+
              ";T="+_type+
              ";ID="+_pnfsId.toShortString() ;
    }
    public String toString(){
       return "Req-"+_id+
              ";T="+_type+
              ";ID="+_pnfsId.toShortString()+
              ";R=("+_returnCode+")"+_returnMessage ;
    }
    public String toReturnString(){
       return "R=("+_returnCode+")"+_returnMessage ;
    }
}
