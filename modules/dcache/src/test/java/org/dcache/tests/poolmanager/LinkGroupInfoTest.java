package org.dcache.tests.poolmanager;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.vehicles.PoolLinkGroupInfo;

import dmg.util.CommandException;
import dmg.util.CommandInterpreter;

import org.dcache.poolmanager.Utils;
import org.dcache.util.Args;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LinkGroupInfoTest {


    private PoolSelectionUnit _selectionUnit;
    private CostModuleV1 _costModule ;
    private CommandInterpreter _ci;

    @Before
    public void setUp() throws Exception {


        _selectionUnit = new PoolSelectionUnitV2();
        _costModule = new CostModuleV1();
        _ci = new CommandInterpreter(_selectionUnit);

        _ci.command( new Args("psu create pool p0" )  );
        _ci.command( new Args("psu create pool p1" )  );
        _ci.command( new Args("psu create pool p2" )  );
        _ci.command( new Args("psu create pool p3" )  );
        _ci.command( new Args("psu create pool p4" )  );
        _ci.command( new Args("psu create pool p5" )  );
        _ci.command( new Args("psu create pool p6" )  );
        _ci.command( new Args("psu create pool p7" )  );

        _ci.command( new Args("psu create pgroup pg-a" )  );
        _ci.command( new Args("psu addto pgroup pg-a p0" )  );
        _ci.command( new Args("psu addto pgroup pg-a p1" )  );
        _ci.command( new Args("psu addto pgroup pg-a p2" )  );

        _ci.command( new Args("psu create pgroup pg-a-copy" )  );
        _ci.command( new Args("psu addto pgroup pg-a p0" )  );
        _ci.command( new Args("psu addto pgroup pg-a p1" )  );
        _ci.command( new Args("psu addto pgroup pg-a p2" )  );

        _ci.command( new Args("psu create pgroup pg-b" )  );
        _ci.command( new Args("psu addto pgroup pg-b p3" )  );
        _ci.command( new Args("psu addto pgroup pg-b p4" )  );
        _ci.command( new Args("psu addto pgroup pg-b p5" )  );

        _ci.command( new Args("psu create pgroup pg-c" )  );
        _ci.command( new Args("psu addto pgroup pg-c p6" )  );
        _ci.command( new Args("psu addto pgroup pg-c p7" )  );


        _ci.command( new Args("psu create unit -net    0.0.0.0/0.0.0.0" )  );
        _ci.command( new Args("psu create ugroup world-net" )  );
        _ci.command( new Args("psu addto ugroup world-net 0.0.0.0/0.0.0.0" )  );


        _ci.command( new Args("psu create unit -store  *@*" )  );
        _ci.command( new Args("psu create ugroup any-store" )  );
        _ci.command( new Args("psu addto ugroup any-store  *@*" )  );


        _ci.command( new Args("psu create link link-a any-store world-net" )  );
        _ci.command( new Args("psu set link link-a -readpref=20 -writepref=20 -cachepref=20" )  );
        _ci.command( new Args("psu add link link-a pg-a" )  );
        _ci.command( new Args("psu add link link-a pg-a-copy" )  );

        _ci.command( new Args("psu create link link-b any-store world-net" )  );
        _ci.command( new Args("psu set link link-b -readpref=20 -writepref=20 -cachepref=20" )  );
        _ci.command( new Args("psu add link link-b pg-b" )  );

        _ci.command("psu set allpoolsactive on");
        _ci.command(new Args("psu create linkGroup link-ga"));
        _ci.command(new Args("psu addto linkGroup link-ga link-a" ) );
        _ci.command(new Args("psu addto linkGroup link-ga link-b" ) );

    }



    @Test
    public void testLingGroupSizePool_p0() {

        PoolCostInfoHelper.setCost(_costModule, "p0", 100, 20, 60, 20);

        Map<String, PoolLinkGroupInfo> linkGroupSize = Utils.linkGroupInfos(_selectionUnit, _costModule);


        assertNotNull("Link group missing", linkGroupSize.containsKey("link-ga"));
        assertEquals("Link group size misscalculated (single pool)", 40, linkGroupSize.get("link-ga").getAvailableSpaceInBytes());

    }


    @Test
    public void testLingGroupSizePool_p0p1() {

        // free size is free + removable
        PoolCostInfoHelper.setCost(_costModule, "p0", 100, 20, 60, 20);
        PoolCostInfoHelper.setCost(_costModule, "p1", 100, 80, 0, 20);

        Map<String, PoolLinkGroupInfo> linkGroupSize = Utils.linkGroupInfos(_selectionUnit, _costModule);

        assertNotNull("Link group missing", linkGroupSize.containsKey("link-ga"));
        assertEquals("Link group size misscalculated (two pool in the same pgoup)", 140, linkGroupSize.get("link-ga").getAvailableSpaceInBytes());

    }

    @Test
    public void testLingGroupSizeLink_bc() {

        // free size is free + removable
        PoolCostInfoHelper.setCost(_costModule, "p0", 100, 20, 60, 20);
        PoolCostInfoHelper.setCost(_costModule, "p3", 100, 20, 60, 20);

        Map<String, PoolLinkGroupInfo> linkGroupSize = Utils.linkGroupInfos(_selectionUnit, _costModule);

        assertNotNull("Link group missing", linkGroupSize.containsKey("link-ga"));
        assertEquals("Link group size misscalculated (two links)", 80, linkGroupSize.get("link-ga").getAvailableSpaceInBytes());

    }

    @Test
    public void testLingGroupSizePool_aa() throws CommandException {

        // free size is free + removable
        PoolCostInfoHelper.setCost(_costModule, "p0", 100, 20, 60, 20);
        PoolCostInfoHelper.setCost(_costModule, "p3", 100, 20, 60, 20);

        _ci.command( new Args("psu addto pgroup pg-a p3" )  );


        Map<String, PoolLinkGroupInfo> linkGroupSize = Utils.linkGroupInfos(_selectionUnit, _costModule);

        assertNotNull("Link group missing", linkGroupSize.containsKey("link-ga"));
        assertEquals("Link group size misscalculated (same pool in multiple links)", 80, linkGroupSize.get("link-ga").getAvailableSpaceInBytes());

    }


}
