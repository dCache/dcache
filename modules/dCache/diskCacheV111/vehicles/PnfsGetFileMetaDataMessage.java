// $Id: PnfsGetFileMetaDataMessage.java,v 1.4 2004-11-05 12:07:19 tigran Exp $
package diskCacheV111.vehicles ;
import  diskCacheV111.util.* ;

import diskCacheV111.util.* ;
public class PnfsGetFileMetaDataMessage extends PnfsMessage {

    private FileMetaData _metaData    = null ; 
    private boolean      _resolve     = true ;
    
    private static final long serialVersionUID = 1591894346369251468L;
    
    public PnfsGetFileMetaDataMessage(){ 
       super() ;
       setReplyRequired(true);
    }
    public PnfsGetFileMetaDataMessage( String pnfsId ){
        super( pnfsId ) ;
	setReplyRequired(true);
    }
    public PnfsGetFileMetaDataMessage( PnfsId pnfsId ){
        super( pnfsId ) ;
	setReplyRequired(true);
    }
    public FileMetaData getMetaData(){ return _metaData ; }
    public void setMetaData( FileMetaData metaData ){ _metaData = metaData ; }
    public String toString(){
       return super.toString()+";"+
             (_metaData==null?"[noMetaData]":_metaData.toString()) ;
    }
    public void setResolve( boolean resolve ){ _resolve = resolve ; }
    public boolean resolve(){ return _resolve ; }
}
