package org.dcache.chimera.migration;

import diskCacheV111.util.PnfsId;

/**
 * A class that implements PnfsIdValidator allows a PnfsId object to be
 * checked in some fashion. The nature of the comparison is
 * implementation-specific.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public interface PnfsIdValidator {

    /**
     * Validate a PnfsId value.
     *
     * @param pnfsId the PNFS-ID to validate
     * @return true if the pnfsID is OK, false otherwise
     */
    boolean isOK( PnfsId pnfsId);
}
