package dmg.cells.network;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dmg.cells.nucleus.SerializationHandler;
import dmg.cells.nucleus.SystemCell;
import java.util.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkBase;
import org.apache.zookeeper.KeeperException;

/**
 * The cell domain used for testing. The cell domain is created when the enum is loaded.
 */
enum TestCellDomain {

    LOCAL("localDomain");

    private final String name;

    TestCellDomain(String name) {
        this.name = name;
        SystemCell.create(name, curatorWithoutAllowedCNs(), Optional.empty(),
              SerializationHandler.Serializer.JOS);
    }

    private static CuratorFrameworkBase curatorWithoutAllowedCNs() {
        CuratorFrameworkBase curator = mock(CuratorFrameworkBase.class);
        CuratorFramework facade = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
        try {
            when(facade.getChildren().forPath(anyString()))
                  .thenThrow(new KeeperException.NoNodeException());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        when(curator.usingNamespace(any())).thenReturn(facade);
        return curator;
    }

    String getCellDomainName() {
        return name;
    }
}
