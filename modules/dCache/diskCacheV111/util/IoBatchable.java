// $Id: IoBatchable.java,v 1.2 2003-03-08 21:47:46 cvs Exp $
package diskCacheV111.util ;

public interface IoBatchable extends Batchable {
    public long   getTransferTime() ;
    public long   getBytesTransferred() ;
    public double getTransferRate() ;
    public long   getLastTransferred() ;
    public PnfsId getPnfsId() ;
}
