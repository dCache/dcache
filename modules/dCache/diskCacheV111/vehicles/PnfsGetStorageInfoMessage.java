// $Id: PnfsGetStorageInfoMessage.java,v 1.7 2006-04-11 09:47:53 tigran Exp $
package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;

public class PnfsGetStorageInfoMessage extends PnfsGetFileMetaDataMessage {

    private StorageInfo  _storageInfo = null ;
    private boolean      _followLinks = true;

    private static final long serialVersionUID = -2574949600859502380L;

    public PnfsGetStorageInfoMessage(){
       super() ;
       setReplyRequired(true);
    }
    public PnfsGetStorageInfoMessage( String pnfsId ){
        super( pnfsId ) ;
	setReplyRequired(true);
    }
    public PnfsGetStorageInfoMessage( PnfsId pnfsId ){
        super( pnfsId ) ;
	setReplyRequired(true);
    }
    public void setStorageInfo( StorageInfo info ){
       _storageInfo = info ;
    }
    public StorageInfo getStorageInfo(){ return _storageInfo ;}
    public String toString(){
       return super.toString()+";"+
             (_storageInfo==null?"{NoSINFO}":("{"+_storageInfo+"}")) ;
    }

    public boolean resolve() { return this._followLinks; }
    public void setResolve(boolean followLinks) { this._followLinks = followLinks; }

}
