package org.dcache.tests.cells;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.SystemCell;

/**
 * Dummy cells
 * Create's <i> JUnitTestDomain </i> and starts SystemCell
 *
 */
public class CellAdapterHelper extends CellAdapter {

    private final static SystemCell _systemCell = new SystemCell("JUnitTestDomain");

    public CellAdapterHelper(String name, String args) {

        super(name, args, true);

    }


    public static CellAdapter getSystem() {
        return _systemCell;
    }

    public void die() throws InterruptedException
    {
        kill();
        if (!getNucleus().join(getCellName(), 1000)) {
            throw new RuntimeException("Failed to kill cell within 1 second");
        }
    }
}
