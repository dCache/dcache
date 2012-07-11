package diskCacheV111.clients.vsp ;

import java.io.* ;

public interface VspDataTransferrable {

    public void dataArrived( VspConnection vsp ,
                             byte [] buffer , int offset , int size )
            ;

    public void dataRequested( VspConnection vsp ,
                          byte [] buffer , int offset , int size )
            ;
}
