// $Id

package diskCacheV111.vehicles ;

public class PoolMgrGetReadPoolMsg extends PoolMgrGetPoolMsg {

    private static final long serialVersionUID = 8077455453139521700L;
    
    public PoolMgrGetReadPoolMsg( String       pnfsId ,
                                  StorageInfo  storageInfo ){
	super( pnfsId , storageInfo ) ;
    }

}
