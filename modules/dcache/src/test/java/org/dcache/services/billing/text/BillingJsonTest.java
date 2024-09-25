package org.dcache.services.billing.text;

import static org.junit.Assert.assertTrue;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.RemoveFileInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;
import dmg.cells.nucleus.CellAddressCore;
import java.nio.charset.StandardCharsets;
import javax.security.auth.Subject;
import org.dcache.mock.ProtocolInfoBuilder;
import org.dcache.notification.BillingMessageSerializerVisitor;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;


public class BillingJsonTest {

    private static final CellAddressCore cellAdressCore = new CellAddressCore(
          "bccs_uib_no_023@nas023_bccs_uib_no_1Domain");
    private static final PnfsId pnfsid = new PnfsId("0000B706DD4045F346F2B90F882B706DA807");
    private static final BillingMessageSerializerVisitor visitor = new BillingMessageSerializerVisitor();
    GenericStorageInfo si = new GenericStorageInfo();

    @Before
    public void setup() {
        si.setHsm("osm");
        si.setStorageClass("atlas:default");
    }

    @Test
    public void testDoorRequestInfoMessageValidJson() {
        DoorRequestInfoMessage msg = new DoorRequestInfoMessage(cellAdressCore);
        msg.setSubject(new Subject());

        msg.accept(visitor);

        assertTrue(isValidJson(new String(visitor.getData(), StandardCharsets.UTF_8)));
    }

    @Test
    public void testMoverInfoMessageValidJson() {
        MoverInfoMessage msg = new MoverInfoMessage(cellAdressCore, pnfsid);
        msg.setFileCreated(true);
        msg.setFileSize(687926);
        msg.setTransaction("remove");
        msg.setSubject(new Subject());
        msg.setStorageInfo(si);
        msg.setTransferAttributes(256437, 2784,
              ProtocolInfoBuilder.aProtocolInfo().withProtocol("GFtp")
                    .withIPAddress("109.105.124.147").build());

        msg.accept(visitor);

        assertTrue(isValidJson(new String(visitor.getData(), StandardCharsets.UTF_8)));
    }

    @Test
    public void testPoolHitInfoMessageValidJson() {
        PoolHitInfoMessage msg = new PoolHitInfoMessage(cellAdressCore, pnfsid);

        msg.accept(visitor);

        assertTrue(isValidJson(new String(visitor.getData(), StandardCharsets.UTF_8)));
    }

    @Test
    public void testRemoveFileInfoMessageValidJson() {
        RemoveFileInfoMessage msg = new RemoveFileInfoMessage(cellAdressCore, pnfsid);
        msg.setSubject(new Subject());

        msg.setStorageInfo(si);

        msg.accept(visitor);

        assertTrue(isValidJson(new String(visitor.getData(), StandardCharsets.UTF_8)));
    }

    @Test
    public void testStorageInfoMessageValidJson() {
        StorageInfoMessage msg = new StorageInfoMessage(cellAdressCore, pnfsid, true);
        msg.setStorageInfo(si);

        msg.accept(visitor);

        assertTrue(isValidJson(new String(visitor.getData(), StandardCharsets.UTF_8)));
    }

    @Test
    public void testWarningPnfsFileInfoMessageValidJson() {
        WarningPnfsFileInfoMessage msg = new WarningPnfsFileInfoMessage("TODO", cellAdressCore,
              pnfsid, 0, "TODO");

        msg.accept(visitor);

        assertTrue(isValidJson(new String(visitor.getData(), StandardCharsets.UTF_8)));
    }

    private boolean isValidJson(String string) {
        try {
            new JSONObject(string);
            return true;
        } catch (JSONException e) {
            return false;
        }
    }

}
