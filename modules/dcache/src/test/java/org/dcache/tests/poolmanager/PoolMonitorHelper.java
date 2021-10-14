package org.dcache.tests.poolmanager;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitAccess;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import dmg.util.CommandException;
import dmg.util.CommandInterpreter;
import java.util.List;
import org.dcache.util.Args;

public class PoolMonitorHelper {


    public static PnfsGetCacheLocationsMessage prepareGetCacheLocation(PnfsId pnfsId,
          List<String> locations) {

        PnfsGetCacheLocationsMessage message = new PnfsGetCacheLocationsMessage(pnfsId);
        message.setCacheLocations(locations);

        return message;

    }

    /**
     * Populate Selection unit with pools
     *
     * @param unit  selection unit to populate
     * @param pools list of pools
     */
    public static void prepareSelectionUnit(PoolSelectionUnit unit,
          PoolSelectionUnitAccess access,
          List<String> pools) throws CommandException {
        CommandInterpreter ci = new CommandInterpreter(access);

        ci.command(new Args("psu create unit -store  *@*"));
        ci.command(new Args("psu create unit -protocol */*"));
        ci.command(new Args("psu create unit -net    0.0.0.0/0.0.0.0"));

        ci.command(new Args("psu create ugroup any-store"));
        ci.command(new Args("psu addto ugroup any-store *@*"));

        ci.command(new Args("psu create ugroup world-net"));
        ci.command(new Args("psu addto ugroup world-net 0.0.0.0/0.0.0.0"));

        ci.command(new Args("psu create ugroup all-protocols"));
        ci.command(new Args("psu addto ugroup  all-protocols */*"));

        ci.command(new Args("psu create pgroup all-pools"));

        for (String pool : pools) {
            ci.command(new Args("psu create pool " + pool));
            unit.getPool(pool).setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
            ci.command(new Args("psu addto pgroup all-pools " + pool));
            ci.command(new Args("psu set active " + pool));
            ci.command(new Args("psu set pool " + pool + " ping"));
        }

        ci.command("psu set allpoolsactive on");

        ci.command(new Args("psu create link default-link any-store world-net all-protocols"));
        ci.command(new Args("psu set link default-link -writepref=1  -readpref=1 -cachepref=1"));
        ci.command(new Args("psu addto link default-link all-pools"));

    }

    /**
     * Populate Selection unit with pools
     *
     * @param unit  selection unit to populate
     * @param pools list of pools
     */
    public static void prepareLinkPerPool(PoolSelectionUnit unit,
          PoolSelectionUnitAccess access,
          List<String> pools) throws CommandException {
        CommandInterpreter ci = new CommandInterpreter(access);

        ci.command(new Args("psu create unit -store  *@*"));
        ci.command(new Args("psu create unit -protocol */*"));
        ci.command(new Args("psu create unit -net    0.0.0.0/0.0.0.0"));

        ci.command(new Args("psu create ugroup any-store"));
        ci.command(new Args("psu addto ugroup any-store *@*"));

        ci.command(new Args("psu create ugroup world-net"));
        ci.command(new Args("psu addto ugroup world-net 0.0.0.0/0.0.0.0"));

        ci.command(new Args("psu create ugroup all-protocols"));
        ci.command(new Args("psu addto ugroup  all-protocols */*"));

        ci.command(new Args("psu create pgroup all-pools"));

        int pref = 0;
        for (String pool : pools) {
            pref++;
            ci.command(new Args("psu create pool " + pool));
            unit.getPool(pool).setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
            ci.command(new Args("psu set active " + pool));
            ci.command(new Args("psu set pool " + pool + " ping"));

            ci.command(
                  new Args("psu create link " + pool + "-link any-store world-net all-protocols"));
            ci.command(new Args(
                  "psu set link " + pool + "-link -writepref=" + pref + "  -readpref=" + pref
                        + " -cachepref=" + pref));
            ci.command(new Args("psu add link " + pool + "-link " + pool));

        }

        ci.command("psu set allpoolsactive on");
    }

}
