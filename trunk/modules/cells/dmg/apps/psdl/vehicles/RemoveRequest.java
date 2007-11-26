package dmg.apps.psdl.vehicles ;

import dmg.apps.psdl.pnfs.* ;
import java.io.* ;

public class RemoveRequest extends PsdlHsmRequest {

    
    public RemoveRequest( PnfsId id , HsmProperties hsm ){
       super( "remove" , id , hsm ) ;
    }
    public String toString(){
       return super.toBaseString()+super.toReturnString() ;
    }
}
