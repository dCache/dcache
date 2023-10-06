package org.dcache.chimera.nfsv41.common;

import org.dcache.nfs.v4.xdr.stateid4;

/**
 * Utility class to support removal of legacy stateid class.
 */
@Deprecated(forRemoval = true)
public class LegacyUtils {

    private LegacyUtils() {
        // no instance allowed
    }

    public static stateid4 toStateid(Object stateObject) {
        stateid4 stateid;
        if (stateObject instanceof stateid4) {
            stateid = (stateid4) stateObject;
        } else {
            org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateid
                  = (org.dcache.chimera.nfs.v4.xdr.stateid4) stateObject;
            stateid = new stateid4(legacyStateid.other, legacyStateid.seqid.value);
        }
        return stateid;
    }
}
