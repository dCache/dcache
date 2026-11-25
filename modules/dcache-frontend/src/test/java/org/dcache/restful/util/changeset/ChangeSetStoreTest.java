package org.dcache.restful.util.changeset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import javax.ws.rs.ClientErrorException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChangeSetStoreTest {

    private TestingServer server;
    private CuratorFramework curator;
    private ChangeSetStore store;

    @Before
    public void setUp() throws Exception {
        server = new TestingServer(true);
        curator = CuratorFrameworkFactory.newClient(server.getConnectString(),
              new RetryOneTime(100));
        curator.start();
        store = new ChangeSetStore();
        store.setCuratorFramework(curator);
    }

    @After
    public void tearDown() throws Exception {
        curator.close();
        server.close();
    }

    @Test
    public void shouldReadCreatedChangeSet() {
        JSONObject changeSet = changeSet("DRAFT");

        long id = store.create(changeSet);

        ChangeSetStore.Stored stored = store.read(id).orElseThrow();
        assertTrue(stored.changeSet().similar(changeSet));
    }

    @Test
    public void shouldReadNothingForUnknownId() {
        assertEquals(Optional.empty(), store.read(161));
    }

    @Test
    public void shouldUpdateWithCurrentVersion() {
        long id = store.create(changeSet("DRAFT"));
        int version = store.read(id).orElseThrow().version();

        int newVersion = store.update(id, changeSet("APPLIED"), version);

        ChangeSetStore.Stored stored = store.read(id).orElseThrow();
        assertEquals(version + 1, newVersion);
        assertEquals(newVersion, stored.version());
        assertEquals("APPLIED", stored.changeSet().getString("status"));
    }

    @Test
    public void shouldRejectUpdateWithStaleVersion() {
        long id = store.create(changeSet("DRAFT"));
        int version = store.read(id).orElseThrow().version();
        store.update(id, changeSet("APPLYING"), version);

        ClientErrorException e = assertThrows(ClientErrorException.class,
              () -> store.update(id, changeSet("APPLIED"), version));

        assertEquals(409, e.getResponse().getStatus());
        assertEquals("APPLYING", store.read(id).orElseThrow().changeSet().getString("status"));
    }

    private static JSONObject changeSet(String status) {
        return new JSONObject()
              .put("status", status)
              .put("description", "test");
    }
}
