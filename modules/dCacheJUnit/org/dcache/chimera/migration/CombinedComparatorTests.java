package org.dcache.chimera.migration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import diskCacheV111.util.PnfsId;

public class CombinedComparatorTests {

    public static final PnfsId PNFS_ID =
            new PnfsId( "000200000000000000001060");

    static class AlwaysOKValidator implements PnfsIdValidator {
        @Override
        public boolean isOK( PnfsId pnfsId) {
            return true;
        }
    }

    static class AlwaysNotOKValidator implements PnfsIdValidator {
        @Override
        public boolean isOK( PnfsId pnfsId) {
            return false;
        }
    }

    @Test
    public void testOKOK() {
        PnfsIdValidator ok1 = new AlwaysOKValidator();
        PnfsIdValidator ok2 = new AlwaysOKValidator();

        PnfsIdValidator combined = new CombinedComparator( ok1, ok2);

        assertTrue( "Checking OK + OK = OK", combined.isOK( PNFS_ID));
    }

    @Test
    public void testOKFAIL() {
        PnfsIdValidator ok = new AlwaysOKValidator();
        PnfsIdValidator fail = new AlwaysNotOKValidator();

        PnfsIdValidator combined = new CombinedComparator( ok, fail);

        assertFalse( "Checking OK + FAIL = FAIL", combined.isOK( PNFS_ID));
    }

    @Test
    public void testFAILOK() {
        PnfsIdValidator ok = new AlwaysOKValidator();
        PnfsIdValidator fail = new AlwaysNotOKValidator();

        PnfsIdValidator combined = new CombinedComparator( fail, ok);

        assertFalse( "Checking FAIL + OK = FAIL", combined.isOK( PNFS_ID));
    }

    @Test
    public void testFAILFAIL() {
        PnfsIdValidator fail1 = new AlwaysNotOKValidator();
        PnfsIdValidator fail2 = new AlwaysNotOKValidator();

        PnfsIdValidator combined = new CombinedComparator( fail1, fail2);

        assertFalse( "Checking FAIL + FAIL = FAIL", combined.isOK( PNFS_ID));
    }

}
