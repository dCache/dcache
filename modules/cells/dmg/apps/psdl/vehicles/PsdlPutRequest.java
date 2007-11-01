package dmg.apps.psdl.vehicles ;

import dmg.apps.psdl.pnfs.* ;
import java.io.* ;

public class PsdlPutRequest extends PsdlIoRequest {
    
    public PsdlPutRequest(  String clientHost ,
                            int    clientPort ,
                            PnfsId id ,
                            PnfsId parentId ,
                            long   size            ){
                        
       super( "put" , clientHost , clientPort , id , parentId , size ) ;
   }
   
} 
