/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.xdr;

import org.junit.Test;
import static org.junit.Assert.*;

public class OncRpcProgramTest {


    @Test
    public void testGetNumber() {
        int progNum = 15;
        int progVers = 17;

        OncRpcProgram program = new OncRpcProgram(progNum, progVers);
        assertEquals("Program number miss match", progNum, program.getNumber());
    }

    @Test
    public void testGetVersion() {
        int progNum = 15;
        int progVers = 17;

        OncRpcProgram program = new OncRpcProgram(progNum, progVers);
        assertEquals("Program version miss match", progVers, program.getVersion());
    }

    @Test
    public void testEqualsTrue() {
        int progNum = 15;
        int progVers = 17;

        OncRpcProgram program1 = new OncRpcProgram(progNum, progVers);
        OncRpcProgram program2 = new OncRpcProgram(progNum, progVers);
        assertTrue("Equals programs not detected", program1.equals(program2));
    }

    @Test
    public void testNotEqualByProgNumber() {
        int progNum = 15;
        int progVers = 17;

        OncRpcProgram program1 = new OncRpcProgram(progNum, progVers);
        OncRpcProgram program2 = new OncRpcProgram(progNum, progVers + 10);
        assertFalse("Different programs not detected", program1.equals(program2));
    }

}
