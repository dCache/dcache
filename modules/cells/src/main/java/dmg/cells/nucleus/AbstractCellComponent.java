package dmg.cells.nucleus;

import java.util.Map;
import org.dcache.util.Args;

public class AbstractCellComponent
      implements CellMessageSender, CellIdentityAware, CellArgsAware {

    private CellEndpoint _endpoint;
    private CellAddressCore _cellAddress;
    private Args _cellArgs;

    /**
     * Implements CellMessageSender interface.
     */
    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _endpoint = endpoint;
    }

    /**
     * Implements CellMessageSender interface.
     */
    protected CellEndpoint getCellEndpoint() {
        return _endpoint;
    }

    /**
     * Sends <code>envelope</code>.
     *
     * @param envelope the cell message to be sent.
     * @throws SerializationException if the payload object of this message is not serializable.
     */
    protected void sendMessage(CellMessage envelope)
          throws SerializationException {
        _endpoint.sendMessage(envelope);
    }

    /**
     * Returns the address of the cell hosting this component.
     */
    protected CellAddressCore getCellAddress() {
        return _cellAddress;
    }

    /**
     * Returns the name of the cell hosting this component.
     */
    protected String getCellName() {
        return getCellAddress().getCellName();
    }

    /**
     * Returns the name of the domain hosting the cell hosting this component.
     */
    protected String getCellDomainName() {
        return getCellAddress().getCellDomainName();
    }

    /**
     * Returns the domain context. The domain context is shared by all cells in a domain.
     */
    protected Map<String, Object> getDomainContext() {
        return _endpoint.getDomainContext();
    }

    /**
     * Returns the cell command line arguments provided when the cell was created.
     */
    protected Args getArgs() {
        return _cellArgs;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        _cellAddress = address;
    }

    @Override
    public void setCellArgs(Args args) {
        _cellArgs = args;
    }
}
