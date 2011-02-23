package org.dcache.gplazma.plugins;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

public class ArgumentMapTest {

    private static final String ARGKEY1 = "key 1";
    private static final String ARGV1 = "value 1";
    private static final String ARGKEY2 = "key 2";
    private static final String ARGV2 = "value 2";
    private static final String ARGKEY3 = "key 3";
    private static final String ARGV3 = "value 3";
    private static final String INVALID_ENTRY = "invalidentry";

    private static final Set<String> ARGNAME_SET = new HashSet<String>( Arrays.asList( new String[] { ARGKEY1, ARGKEY2, ARGKEY3 } ));
    private static final String[] VALID_ARGS = new String[] { ARGKEY1, ARGV1, ARGKEY2, ARGV2, ARGKEY3, ARGV3 };
    private static final String[] VALID_KV_ARGS = new String[] { ARGKEY1+"="+ARGV1, ARGKEY2+"="+ARGV2, ARGKEY3+"="+ARGV3 };
    private static final String[] MISSING_ARGKEY3 = new String[] { ARGKEY1, ARGV1, ARGKEY2, ARGV2, ARGKEY1, ARGV3 };
    private static final String[] MISSING_KV_ARGKEY3 = new String[] { ARGKEY1+"="+ARGV1, ARGKEY2+"="+ARGV2, ARGKEY1+"="+ARGV3 };
    private static final String[] INVALID_ARGS = new String[] { ARGKEY1, ARGV1, ARGKEY2, ARGV2, INVALID_ENTRY, INVALID_ENTRY };
    private static final String[] INVALID_KV_ARGS = new String[] { ARGKEY1+"="+ARGV1, ARGKEY2+"="+ARGV2, INVALID_ENTRY+"="+INVALID_ENTRY };
    private static final String[] ODD_KEY_POS = new String[] { ARGV1, ARGKEY1, ARGV2, ARGKEY2, ARGV3, ARGKEY3 };
    private static final Map<String, String> VALIDMAP = ArgumentMapFactory.create( ARGNAME_SET,  VALID_ARGS);
    private static final Map<String, String> VALIDKVMAP = ArgumentMapFactory.createFromAllKeyValuePairs(ARGNAME_SET, VALID_KV_ARGS);

    @Test(expected=IllegalArgumentException.class)
    public void createFailsForTooFewArgs() {
        ArgumentMapFactory.create(ARGNAME_SET, new String[] { ARGKEY1, ARGV1, ARGKEY2, ARGV2 } );
    }

    @Test(expected=IllegalArgumentException.class)
    public void createFromAllFailsForTooFewArgs() {
        ArgumentMapFactory.createFromAllKeyValuePairs(ARGNAME_SET, new String[] { ARGKEY1+"="+ARGV1, ARGKEY2+"="+ARGV2 } );
    }

    @Test(expected=IllegalArgumentException.class)
    public void createFailsForInvalidArgSet() {
        ArgumentMapFactory.create(ARGNAME_SET, INVALID_ARGS);
    }

    @Test(expected=IllegalArgumentException.class)
    public void createFromAllFailsForInvalidArg() {
        ArgumentMapFactory.create(ARGNAME_SET, INVALID_KV_ARGS);
    }

    @Test(expected=IllegalArgumentException.class)
    public void createFailsForMissingKey() {
        ArgumentMapFactory.create(ARGNAME_SET, MISSING_ARGKEY3);
    }

    @Test(expected=IllegalArgumentException.class)
    public void createFromAllFailsForMissingKey() {
        ArgumentMapFactory.createFromAllKeyValuePairs(ARGNAME_SET, MISSING_KV_ARGKEY3);
    }

    @Test(expected=IllegalArgumentException.class)
    public void mapInitFailsForOddKeyIndex() {
        ArgumentMapFactory.create(ARGNAME_SET, ODD_KEY_POS);
    }

    @Test
    public void testGetValue() {
        Assert.assertEquals(ARGV1, VALIDMAP.get(ARGKEY1));
        Assert.assertEquals(ARGV2, VALIDMAP.get(ARGKEY2));
        Assert.assertEquals(ARGV3, VALIDMAP.get(ARGKEY3));

        Assert.assertEquals(ARGV1, VALIDKVMAP.get(ARGKEY1));
        Assert.assertEquals(ARGV2, VALIDKVMAP.get(ARGKEY2));
        Assert.assertEquals(ARGV3, VALIDKVMAP.get(ARGKEY3));
        Assert.assertEquals(3, ARGNAME_SET.size());
    }

    @Test
    public void testNoValueReturnsNull() {
        Assert.assertNull(VALIDMAP.get(INVALID_ENTRY));
        Assert.assertNull(VALIDKVMAP.get(INVALID_ENTRY));
    }

}
