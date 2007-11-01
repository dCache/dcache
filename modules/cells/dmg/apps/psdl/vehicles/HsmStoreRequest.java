package dmg.apps.psdl.vehicles ;

import dmg.apps.psdl.pnfs.* ;
import java.io.* ;

public class HsmStoreRequest extends PsdlHsmRequest {

    
    public HsmStoreRequest( PnfsId id , HsmProperties hsm ){
       super( "hsmStore" , id , hsm ) ;
    }
    public String toString(){
       return super.toBaseString()+super.toReturnString() ;
    }
}
