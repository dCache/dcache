package org.dcache.tests.util;

import org.junit.Test;
import static org.junit.Assert.*;

import dmg.util.Args;


public class ArgsTest {


    @Test
    public void testNoArgs() {

        Args args =  new Args("");

        assertEquals("Invalid number of arguments", 0, args.argc() );

    }


    @Test
    public void testWithArgs() {

        Args args =  new Args("arg1 aarg2 arg3 arg4 arg5");

        assertEquals("Invalid number of arguments", 5, args.argc() );

    }


    @Test
    public void testWithOps() {

        Args args =  new Args("-opt1 -opt2 -opt3");

        assertEquals("Invalid number of arguments", 0, args.argc() );
        assertEquals("Invalid number of options", 3, args.options().size() );

    }


    @Test
    public void testWithOpsArgs() {

        Args args =  new Args("-opt1 -opt2 -opt3 arg1 aarg2 arg3 arg4 arg5");

        assertEquals("Invalid number of arguments", 5, args.argc() );
        assertEquals("Invalid number of options", 3, args.options().size() );

    }


    @Test
    public void testDoubleOpts() {

        Args args =  new Args("-opt1 -opt2 -opt3 -opt4=opt4-1 -opt4=opt4-2");

        assertEquals("Invalid number of arguments", 0, args.argc() );
        assertEquals("Invalid number of options", 4, args.options().size() );

    }


}
