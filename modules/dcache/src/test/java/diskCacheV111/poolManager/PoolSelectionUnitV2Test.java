package diskCacheV111.poolManager;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import diskCacheV111.pools.PoolV2Mode;
import dmg.cells.nucleus.DelayedReply;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandPanicException;
import dmg.util.CommandThrowableException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import org.dcache.util.Args;
import org.junit.Before;
import org.junit.Test;

public class PoolSelectionUnitV2Test {

    static final String POOLMANAGER_CONF = "org/dcache/tests/psu/poolmanager.conf";
    static final Set<String> TAPE_POOLS = Set.of("testpool03-5", "testpool08-5", "testpool09-5",
          "testpool04-7", "testpool04-5");
    static final Set<String> STAGE_POOLS = Set.of("testpool08-7", "testpool09-6", "testpool09-7",
          "testpool08-6", "testpool03-6", "testpool04-6");

    PoolSelectionUnitV2 psu;
    CommandInterpreter commandInterpreter = new CommandInterpreter();
    File config;
    PoolPreferenceLevel[] levels;

    @Before
    public void setUp() throws Exception {
        psu = new PoolSelectionUnitV2();
        commandInterpreter.addCommandListener(psu);
        URL url = getClass().getClassLoader().getResource(POOLMANAGER_CONF);
        config = new File(url.toURI());

        psu.beforeSetup();
        byte[] data = readAllBytes(config.toPath());
        try {
            executeSetup(commandInterpreter, config.getAbsolutePath(), data);
        } finally {
            psu.afterSetup();
        }
        PoolV2Mode mode = new PoolV2Mode(PoolV2Mode.ENABLED);

        psu.getAllDefinedPools(false).forEach(p -> {
            p.setPoolMode(mode);
            p.setActive(true);
        });

        Set<String> hsmInstances = Set.of("enstore");

        TAPE_POOLS.forEach(p-> {
            psu.getPool(p).setHsmInstances(hsmInstances);
        });

        STAGE_POOLS.forEach(p-> {
            psu.getPool(p).setHsmInstances(hsmInstances);
        });
    }

    @Test
    public void testAddGroup() {
        psu.createPoolGroup("group", false);
        psu.getPoolGroups().containsKey("group");
    }

    @Test
    public void testAddPoolToGroup() {
        psu.createPoolGroup("group", false);
        psu.createPool("poolA", true, false, false);
        psu.addToPoolGroup("group", "poolA");

        assertTrue(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("poolA")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNestedCyclicGroup() {
        psu.createPoolGroup("group", false);
        psu.addToPoolGroup("group", "@group");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNestedCyclicGroupChain() {
        psu.createPoolGroup("groupA", false);
        psu.createPoolGroup("groupB", false);
        psu.createPoolGroup("groupC", false);

        psu.addToPoolGroup("groupA", "@groupB");
        psu.addToPoolGroup("groupB", "@groupC");
        psu.addToPoolGroup("groupC", "@groupA");
    }

    @Test
    public void testNestedGroup() {
        psu.createPoolGroup("group", false);

        psu.createPoolGroup("foo", false);
        psu.createPool("pool-foo", true, false, false);
        psu.addToPoolGroup("foo", "pool-foo");

        psu.createPoolGroup("bar", false);
        psu.createPool("pool-bar", true, false, false);
        psu.addToPoolGroup("bar", "pool-bar");

        psu.addToPoolGroup("group", "@foo");
        psu.addToPoolGroup("group", "@bar");

        assertTrue(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("pool-foo")));
        assertTrue(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("pool-bar")));
    }

    @Test
    public void testRemoveNestedGroup() {
        psu.createPoolGroup("group", false);
        psu.createPoolGroup("foo", false);
        psu.createPoolGroup("bar", false);


        psu.createPool("pool-foo", true, false, false);
        psu.addToPoolGroup("foo", "pool-foo");

        psu.createPool("pool-bar", true, false, false);
        psu.addToPoolGroup("bar", "pool-bar");

        psu.addToPoolGroup("group", "@foo");
        psu.addToPoolGroup("group", "@bar");

        psu.removeFromPoolGroup("group", "@foo");

        assertFalse(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("pool-foo")));
        assertTrue(psu.getPoolGroups().get("group").getPools().contains(psu.getPool("pool-bar")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveMissingNestedGroup() {
        psu.createPoolGroup("group", false);
        psu.createPoolGroup("foo", false);
        psu.createPoolGroup("bar", false);

        psu.addToPoolGroup("group", "@foo");

        psu.removeFromPoolGroup("group", "@bar");
    }

    @Test
    public void testThatReadWithSpecificValuesMatchesTapePools() {
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatReadWithProtocolDefaultMatchesTapePools() {
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 */*");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatReadWithProtocolGlobMatchesTapePools() {
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 *");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatReadWithNetDefaultIPv4MatchesTapePools() {
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore 0.0.0.0/0.0.0.0 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatReadWithNetDefaultIPv6MatchesTapePools() {
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore ::/0 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatReadWithNetGlobMatchesTapePools() {
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore * Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatReadWithAllGlobsMatchesTapePools() {
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore * *");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatWriteWithSpecificValuesMatchesTapePools() {
        whenMatchIsCalledWith("write -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatWriteWithProtocolDefaultMatchesTapePools() {
        whenMatchIsCalledWith("write -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 */*");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatWriteWithProtocolGlobMatchesTapePools() {
        whenMatchIsCalledWith("write -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 *");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatWriteWithNetDefaultIPv4MatchesTapePools() {
        whenMatchIsCalledWith("write -storageClass=tape.dcache-devel-test -hsm=enstore 0.0.0.0/0.0.0.0 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatWriteWithNetDefaultIPv6MatchesTapePools() {
        whenMatchIsCalledWith("write -storageClass=tape.dcache-devel-test -hsm=enstore ::/0 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatWriteWithNetGlobMatchesTapePools() {
        whenMatchIsCalledWith("write -storageClass=tape.dcache-devel-test -hsm=enstore * Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatWriteWithAllGlobsMatchesTapePools() {
        whenMatchIsCalledWith("write -storageClass=tape.dcache-devel-test -hsm=enstore * *");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatP2PWithSpecificValuesMatchesTapePools() {
        whenMatchIsCalledWith("p2p -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatP2PWithProtocolDefaultMatchesTapePools() {
        whenMatchIsCalledWith("p2p -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 */*");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatP2PWithProtocolGlobMatchesTapePools() {
        whenMatchIsCalledWith("p2p -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 *");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatP2PWithNetDefaultIPv4MatchesTapePools() {
        whenMatchIsCalledWith("p2p -storageClass=tape.dcache-devel-test -hsm=enstore 0.0.0.0/0.0.0.0 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatP2PWithNetDefaultIPv6MatchesTapePools() {
        whenMatchIsCalledWith("p2p -storageClass=tape.dcache-devel-test -hsm=enstore ::/0 Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatP2PWithNetGlobMatchesTapePools() {
        whenMatchIsCalledWith("p2p -storageClass=tape.dcache-devel-test -hsm=enstore * Http/1");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatP2PWithAllGlobsMatchesTapePools() {
        whenMatchIsCalledWith("p2p -storageClass=tape.dcache-devel-test -hsm=enstore * *");
        assertThatPoolsAre(TAPE_POOLS);
    }

    @Test
    public void testThatCacheWithSpecificValuesMatchesStagePools() {
        whenMatchIsCalledWith("cache -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 Http/1");
        assertThatPoolsAre(STAGE_POOLS);
    }

    @Test
    public void testThatCacheWithProtocolDefaultMatchesStagePools() {
        whenMatchIsCalledWith("cache -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 */*");
        assertThatPoolsAre(STAGE_POOLS);
    }

    @Test
    public void testThatCacheWithProtocolGlobMatchesStagePools() {
        whenMatchIsCalledWith("cache -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 *");
        assertThatPoolsAre(STAGE_POOLS);
    }

    @Test
    public void testThatCacheWithNetDefaultIPv4MatchesStagePools() {
        whenMatchIsCalledWith("cache -storageClass=tape.dcache-devel-test -hsm=enstore 0.0.0.0/0.0.0.0 Http/1");
        assertThatPoolsAre(STAGE_POOLS);
    }

    @Test
    public void testThatCacheWithNetDefaultIPv6MatchesStagePools() {
        whenMatchIsCalledWith("cache -storageClass=tape.dcache-devel-test -hsm=enstore ::/0 Http/1");
        assertThatPoolsAre(STAGE_POOLS);
    }

    @Test
    public void testThatCacheWithNetGlobMatchesTapeStage() {
        whenMatchIsCalledWith("cache -storageClass=tape.dcache-devel-test -hsm=enstore * Http/1");
        assertThatPoolsAre(STAGE_POOLS);
    }

    @Test
    public void testThatCacheWithAllGlobsMatchesTapePools() {
        whenMatchIsCalledWith("cache -storageClass=tape.dcache-devel-test -hsm=enstore * *");
        assertThatPoolsAre(STAGE_POOLS);
    }

    @Test
    public void testThatReadWithUnmappedNetIPv5DefaultFails() {
        givenIPv4DefaultIsMissingFromConfiguration();
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore 0.0.0.0/0.0.0.0 *");
        /*
         *  It would be preferable here to throw an exception, but this would
         *  require the defaults to be mapped, which would not be backward compatible.
         */
        assertNoPoolsReturned();
    }

    @Test
    public void testThatReadWithUnmappedNetIPv6DefaultFails() {
        givenIPv6DefaultIsMissingFromConfiguration();
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore ::/0 *");
        /*
         *  It would be preferable here to throw an exception, but this would
         *  require the defaults to be mapped, which would not be backward compatible.
         */
        assertNoPoolsReturned();
    }

    @Test
    public void testThatReadWithGlobValueFailsWhenIPv6DefaultMissing() {
        givenIPv6DefaultIsMissingFromConfiguration();
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore * *");
        /*
         *  It would be preferable here to throw an exception, but this would
         *  require the defaults to be mapped, which would not be backward compatible.
         */
        assertNoPoolsReturned();
    }

    @Test
    public void testThatReadWithUnmappedProtocolDefaultFails() {
        givenProtocolDefaultIsMissingFromConfiguration();
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 */*");
        /*
         *  It would be preferable here to throw an exception, but this would
         *  require the defaults to be mapped, which would not be backward compatible.
         */
        assertNoPoolsReturned();
    }

    @Test
    public void testThatReadWithGlobValueFailsWhenProtocolDefaultMissing() {
        givenProtocolDefaultIsMissingFromConfiguration();
        whenMatchIsCalledWith("read -storageClass=tape.dcache-devel-test -hsm=enstore 127.0.0.1 */*");
        /*
         *  It would be preferable here to throw an exception, but this would
         *  require the defaults to be mapped, which would not be backward compatible.
         */
        assertNoPoolsReturned();
    }

    private void assertNoPoolsReturned() {
        assertNotNull(levels);
        assertEquals("wrong number of preference levels", 0, levels.length);
    }

    private void assertThatPoolsAre(Set<String> oracle) {
        assertNotNull(levels);
        assertEquals("wrong number of preference levels", 1, levels.length);
        Set<String> pools = new HashSet();
        pools.addAll(levels[0].getPoolList());
        assertEquals("preference pools are incorrect.", oracle, pools);
    }

    /*
     *  Borrowed from UniversalSpringCell
     */
    private void executeSetup(CommandInterpreter interpreter, String source, byte[] data)
          throws Exception {
        BufferedReader in = new BufferedReader(
              new InputStreamReader(new ByteArrayInputStream(data), UTF_8));
        int lineCount = 1;
        for (String line = in.readLine(); line != null; line = in.readLine(), lineCount++) {
            line = line.trim();
            if (line.isEmpty() || line.charAt(0) == '#') {
                continue;
            }
            try {
                Serializable result = interpreter.command(new Args(line));
                if (result instanceof DelayedReply) {
                    ((DelayedReply) result).take();
                }
            } catch (InterruptedException e) {
                throw new CommandExitException(
                      "Error at " + source + ":" + lineCount + ": command interrupted");
            } catch (CommandPanicException e) {
                throw new CommandPanicException(
                      "Error at " + source + ":" + lineCount + ": " + e.getMessage(), e);
            } catch (CommandException e) {
                throw new CommandThrowableException(
                      "Error at " + source + ":" + lineCount + ": " + e.getMessage(), e);
            }
        }
    }

    private void givenIPv4DefaultIsMissingFromConfiguration() {
        psu.removeUnit("0.0.0.0/0.0.0.0", true);
    }

    private void givenIPv6DefaultIsMissingFromConfiguration() {
        psu.removeUnit("::/0", true);
    }

    private void givenProtocolDefaultIsMissingFromConfiguration() {
        psu.removeUnit("*/*", false);
    }

    private void whenMatchIsCalledWith(String params) {
        Args args = new Args(params);
        try {
            levels = (PoolPreferenceLevel[]) psu.ac_psux_match_$_3(args);
        } catch (Exception e) {
            assertNull("Unexpected exception", e);
        }
    }
}