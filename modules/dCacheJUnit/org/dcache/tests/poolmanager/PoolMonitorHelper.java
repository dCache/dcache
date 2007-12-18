package org.dcache.tests.poolmanager;

import java.util.List;

import dmg.util.Args;
import dmg.util.CommandException;
import dmg.util.CommandInterpreter;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;

public class PoolMonitorHelper {


    public static PnfsGetCacheLocationsMessage prepareGetCacheLocation( PnfsId pnfsId, List<String> locations ) {

        PnfsGetCacheLocationsMessage message = new PnfsGetCacheLocationsMessage(pnfsId);
        message.setCacheLocations(locations);

        return message;

    }



    public static void prepareSelectionUnit(PoolSelectionUnit unit, List<String> pools) throws CommandException {


        CommandInterpreter ci = new CommandInterpreter(unit);

        ci.command( new Args("psu create unit -store  *@*" )  );

        ci.command( new Args("psu create unit -protocol */*" )  );

        ci.command( new Args("psu create ugroup all" )  );

        ci.command( new Args("psu addto ugroup all *@*" )  );

        ci.command( new Args("psu create unit -net    0.0.0.0/0.0.0.0" )  );

        ci.command( new Args("psu create ugroup extern" )  );

        ci.command( new Args("psu addto ugroup extern 0.0.0.0/0.0.0.0" )  );

        ci.command( new Args("psu create pgroup default" )  );
        for(String pool : pools ) {
            ci.command( new Args("psu create pool " + pool  )  );
            ci.command( new Args("psu addto pgroup default " + pool  )  );
            ci.command( new Args("psu set active " + pool + " -on"  )  );
            ci.command( new Args("psu set pool " + pool + " ping"  )  );
        }

        ci.command("psu set allpoolsactive on");

        ci.command( new Args("psu create link default all extern" )  );
        ci.command( new Args("psu set link default -writepref=1  -readpref=1 -cachepref=1" )  );

        ci.command( new Args("psu add link default default" )  );

    }

}
