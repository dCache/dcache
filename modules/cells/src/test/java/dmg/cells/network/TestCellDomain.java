package dmg.cells.network;

import static org.mockito.Mockito.mock;

import dmg.cells.nucleus.SerializationHandler;
import dmg.cells.nucleus.SystemCell;
import java.util.Optional;
import org.apache.curator.framework.imps.CuratorFrameworkBase;

/**
 * The cell domain used for testing. The cell domain is created when the enum is loaded.
 */
enum TestCellDomain {

    LOCAL("localDomain");

    private final String name;

    TestCellDomain(String name) {
        this.name = name;
        SystemCell.create(name, mock(CuratorFrameworkBase.class), Optional.empty(),
              SerializationHandler.Serializer.JOS);
    }

    String getCellDomainName() {
        return name;
    }
}
