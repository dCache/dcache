package org.dcache.chimera.migration;

import diskCacheV111.util.PnfsId;

/**
 * Combine two or more PnfsIdValidator objects to form an aggregated
 * PnfsIdValidator. The isOK method is the logical AND of the component
 * PnfsIdValidator: it returns true iff all component isOK return true.
 */
public class CombinedComparator implements PnfsIdValidator {

    final private PnfsIdValidator _validators[];

    public CombinedComparator( PnfsIdValidator... validators) {
        _validators = validators;
    }

    @Override
    public boolean isOK( PnfsId pnfsId) {
        boolean isOK = true;

        for( int i = 0; i < _validators.length; i++)
            if( !_validators[i].isOK( pnfsId))
                isOK = false;

        return isOK;
    }
}
