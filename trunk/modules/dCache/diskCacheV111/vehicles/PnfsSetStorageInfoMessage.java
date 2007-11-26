// $Id: PnfsSetStorageInfoMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $
package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;

import diskCacheV111.util.* ;
public class PnfsSetStorageInfoMessage extends PnfsMessage {

    private StorageInfo  _storageInfo = null ;
    private int          _accessMode  = 0 ;
    
    private static final long serialVersionUID = -5030106015250844867L;
    
    public PnfsSetStorageInfoMessage(){ 
       super() ;
       setReplyRequired(true);
    }
    public PnfsSetStorageInfoMessage( PnfsId pnfsId ){
        super( pnfsId ) ;
	setReplyRequired(true);
    }
    public PnfsSetStorageInfoMessage( 
                 PnfsId pnfsId , 
                 StorageInfo storageInfo , 
                 int accessMode               ){
        this( pnfsId ) ;
	_accessMode  = accessMode ;
        _storageInfo = storageInfo ;
    }
    public int getAccessMode(){ return _accessMode ; }
    public void setAccessMode( int accessMode ){ _accessMode = accessMode ; }
    public void setStorageInfo( StorageInfo info ){
       _storageInfo = info ;
    }
    public StorageInfo getStorageInfo(){ return _storageInfo ;}
    public String toString(){
       return super.toString()+";mode="+_accessMode+";"+
             (_storageInfo==null?"{NoSINFO}":("{"+_storageInfo+"}")) ;
    }
}
