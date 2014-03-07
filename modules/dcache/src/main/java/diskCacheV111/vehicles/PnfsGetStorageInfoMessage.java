package diskCacheV111.vehicles;

import java.util.EnumSet;

import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.PnfsGetFileAttributes;

@Deprecated // Kept for compatibility with 2.6 pools
public class PnfsGetStorageInfoMessage extends PnfsGetFileAttributes {

    private static final long serialVersionUID = -2574949600859502380L;

    private PnfsGetStorageInfoMessage()
    {
        super((PnfsId) null, EnumSet.noneOf(FileAttribute.class));
    }
}
