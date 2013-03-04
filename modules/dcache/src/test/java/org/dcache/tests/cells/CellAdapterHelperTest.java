package org.dcache.tests.cells;

import org.junit.Test;

import dmg.util.Args;

import static org.junit.Assert.*;


public class CellAdapterHelperTest {

    private CellAdapterHelper _sutCell;

    @Test
    public void testArgs() {

        String args = "arg0" +
        		" -opt1=opt1-value" +
        		" -opt2=opt2-value" +
        		" -flag1" +
        		" arg1";

        _sutCell = new CellAdapterHelper("testArgsCell", args );

        Args cellArgs = _sutCell.getArgs();
        assertNotNull("Args is NULL", cellArgs);


        String opt1= cellArgs.getOpt("opt1");

        assertEquals("Invalid option received", "opt1-value",opt1 );
        assertTrue("Flag is not set", cellArgs.hasOption("flag1"));

        String arg0 = cellArgs.argv(0);
        assertEquals("Invalid arg[0] received", "arg0", arg0 );

        String arg1 = cellArgs.argv(1);
        assertEquals("Invalid arg[1] received", "arg1", arg1 );


    }

}
