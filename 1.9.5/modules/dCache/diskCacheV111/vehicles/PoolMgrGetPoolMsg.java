// $Id: PoolMgrGetPoolMsg.java,v 1.3 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;

public class PoolMgrGetPoolMsg extends PoolManagerMessage {

    private StorageInfo _storageInfo  = null;
    private PnfsId      _pnfsId       = null;
    private String      _poolName;

    private static final long serialVersionUID = 8907604668091102254L;
    
    public PoolMgrGetPoolMsg( String       pnfsId ,
                              StorageInfo  storageInfo ){
	_storageInfo  = storageInfo;
	_pnfsId       = new PnfsId(pnfsId);
	setReplyRequired(true);
    }
    public PoolMgrGetPoolMsg( PnfsId       pnfsId ,
                              StorageInfo  storageInfo ){
	_storageInfo  = storageInfo;
	_pnfsId       = pnfsId;
	setReplyRequired(true);
    }

    public StorageInfo getStorageInfo(){
	return _storageInfo ;
    }
    public PnfsId getPnfsId(){
	return _pnfsId;
    }
    public String getPoolName(){
	return _poolName;
    }
    public void setPoolName(String poolName){
	_poolName = poolName;
    }
    public String toString(){
       if( getReturnCode() == 0 )
         return "PnfsId="+
                (_pnfsId==null?"<unknown>":_pnfsId.toString())+
                ";StorageInfo="+
                (_storageInfo==null?"<unknown>":
                                    _storageInfo.toString())+
                ";PoolName="+
                (_poolName==null?"<unknown>":_poolName) ;
       else
          return super.toString() ;
    }


}
