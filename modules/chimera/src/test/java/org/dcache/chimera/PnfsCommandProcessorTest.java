package org.dcache.chimera;

import static org.dcache.chimera.PnfsCommandProcessor.process;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PnfsCommandProcessorTest {

    @Test
    public void shouldReturnParcsedElements() {

        String[] cmd = process(".(arg1)(arg2)");
        assertEquals("Invalid number of arguments", 2, cmd.length);
        assertArrayEquals(new String[]{"arg1", "arg2"}, cmd);
    }

    @Test
    public void shouldReturnSameElementOnMissingBrackets() {
        String[] cmd = process("arg1");
        assertEquals("Invalid number of arguments", 1, cmd.length);
        assertArrayEquals(new String[]{"arg1"}, cmd);
    }

    @Test
    public void shouldIgnoreNastedBrackets() {
        String[] cmd = process(".(get)(parameters_dict_gen'+str(i).zfill(4)+'.csv)(locality)");
        assertEquals("Invalid number of arguments", 3, cmd.length);
        assertArrayEquals(new String[]{"get",
              "parameters_dict_gen'+str(i).zfill(4)+'.csv", "locality"}, cmd);
    }

    @Test
    public void shouldIgnoreOpenBrackets() {
        String[] cmd = process(".(arg1)(parameters");
        assertEquals("Invalid number of arguments", 1, cmd.length);
        assertArrayEquals(new String[]{"arg1"}, cmd);
    }

}
