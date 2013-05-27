package diskCacheV111.clients.vsp ;

public interface VspDataTransferrable {

    public void dataArrived( VspConnection vsp ,
                             byte [] buffer , int offset , int size )
            ;

}
