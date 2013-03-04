package org.dcache.tests.cells;


import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import static org.junit.Assert.*;


public class GenericMockCellHelperTest {


    private static GenericMockCellHelper _cell = new GenericMockCellHelper("PnfsHandlerTestCell", "");
    private final static String PNFS_MANAGER = "PnfsManager";


    @Test
    public void testGetCacheLocations() throws Exception {

        // prepare
        PnfsGetCacheLocationsMessage message = new PnfsGetCacheLocationsMessage(new PnfsId("000000000000000000000000000000000001"));
        CellPath pnfsManagerPath = new CellPath(PNFS_MANAGER);
        List<String> locations = new ArrayList<>();

        locations.add("pool1");
        message.setCacheLocations(locations);

        GenericMockCellHelper.prepareMessage(pnfsManagerPath, message);


        // exercise
        PnfsGetCacheLocationsMessage messageToAsk = new PnfsGetCacheLocationsMessage(new PnfsId("000000000000000000000000000000000001"));

        CellMessage msg = new CellMessage( pnfsManagerPath , messageToAsk);

        CellMessage reply = _cell.sendAndWait(msg, 0);

        assertNotNull("no message received", reply);

        Object replyMessage = reply.getMessageObject();
        assertTrue("wrong object received", replyMessage instanceof  PnfsGetCacheLocationsMessage );

        PnfsGetCacheLocationsMessage locationReply = (PnfsGetCacheLocationsMessage)replyMessage;

        List<String> locationsRecieved = locationReply.getCacheLocations();

        assertFalse("get empty list", locationsRecieved.isEmpty());

        assertEquals("wrong pool order", "pool1", locationsRecieved.get(0));

    }

}
