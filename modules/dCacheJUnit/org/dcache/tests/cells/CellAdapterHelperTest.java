package org.dcache.tests.cells;

import static org.junit.Assert.*;

import org.junit.Test;

import dmg.util.Args;


public class CellAdapterHelperTest {

    private CellAdapterHelper _sutCell;

    @Test
    public void testArgs() {

        String args = "arg1" +
        		" -opt1=opt1-value" +
        		" -opt2=opt2-value" +
        		" -flag1";

        _sutCell = new CellAdapterHelper("testArgsCell", args );

        Args cellArgs = _sutCell.getArgs();
        assertNotNull("Args is NULL", cellArgs);


        String opt1= cellArgs.getOpt("opt1");

        assertEquals("Invalid option recieved", "opt1-value",opt1 );

    }

}
