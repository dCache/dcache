// $Id: PoolManagerGetWritePoolMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

public class PoolManagerGetWritePoolMessage extends PoolManagerMessage {

    private String _storageClass;
    private String _pnfsId;
    private String _poolName;

    private static final long serialVersionUID = 1261369926481827276L;

    public PoolManagerGetWritePoolMessage(String storageClass,
					  String pnfsId){
	_storageClass = storageClass;
	_pnfsId       = pnfsId;
	setReplyRequired(true);
    }

    public String getStorageClass(){
	return _storageClass;
    }
    public String getPnfsId(){
	return _pnfsId;
    }
    public String getPoolName(){
	return _poolName;
    }
    public void setPoolName(String poolName){
	_poolName = poolName;
    }
    public String toString(){
       if( getReturnCode() == 0 ) {
           return "StorageClass=" + (_storageClass == null ? "<unknown>" : _storageClass) +
                   ";PnfsId=" + (_pnfsId == null ? "<unknown>" : _pnfsId) +
                   ";PoolName=" + (_poolName == null ? "<unknown>" : _poolName);
       } else {
           return super.toString();
       }
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + " " + getPnfsId();
    }

}
