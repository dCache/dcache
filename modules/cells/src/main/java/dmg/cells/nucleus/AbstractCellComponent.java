package dmg.cells.nucleus;

import java.io.PrintWriter;
import java.util.Map;

import org.dcache.util.Args;

public class AbstractCellComponent
    implements CellInfoProvider,
        CellSetupProvider,
        CellMessageSender,
        CellLifeCycleAware
{
    private CellEndpoint _endpoint;
    private CellAddressCore _cellAddress;

    /**
     * Implements CellInfoProvider interface.
     */
    @Override
    public void getInfo(PrintWriter pw) {}

    /**
     * Implements CellInfoProvider interface.
     */
    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    /**
     * Implements CellSetupProvider interface.
     */
    @Override
    public void printSetup(PrintWriter pw) {}

    /**
     * Implements CellSetupProvider interface.
     */
    @Override
    public void beforeSetup() {}

    /**
     * Implements CellSetupProvider interface.
     */
    @Override
    public void afterSetup() {}

    /**
     * Implements CellLifeCycleAware interface.
     */
    @Override
    public void afterStart() {}

    /**
     * Implements CellLifeCycleAware interface.
     */
    @Override
    public void beforeStop() {}

    /**
     * Implements CellMessageSender interface.
     */
    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
        CellInfo cellInfo = _endpoint.getCellInfo();
        _cellAddress = new CellAddressCore(cellInfo.getCellName(), cellInfo.getDomainName());
    }

    /**
     * Implements CellMessageSender interface.
     */
    protected CellEndpoint getCellEndpoint()
    {
        return _endpoint;
    }

    /**
     * Sends <code>envelope</code>.
     *
     * @param envelope the cell message to be sent.
     * @throws SerializationException if the payload object of this
     *         message is not serializable.
     * @throws NoRouteToCellException if the destination could not be
     *         reached.
     */
    protected void sendMessage(CellMessage envelope)
        throws SerializationException,
            NoRouteToCellException
    {
        _endpoint.sendMessage(envelope);
    }

    /**
     * Provides information about the host cell.
     *
     * Depending on the cell, a subclass of CellInfo with additional
     * information may be returned instead.
     *
     * @return The cell information encapsulated in a CellInfo object.
     */
    protected CellInfo getCellInfo()
    {
        return _endpoint.getCellInfo();
    }

    /**
     * Returns the address of the cell hosting this component.
     */
    protected CellAddressCore getCellAddress()
    {
        return _cellAddress;
    }

    /**
     * Returns the name of the cell hosting this component.
     */
    protected String getCellName()
    {
        return getCellAddress().getCellName();
    }

    /**
     * Returns the name of the domain hosting the cell hosting this
     * component.
     */
    protected String getCellDomainName()
    {
        return getCellAddress().getCellDomainName();
    }

    /**
     * Returns the domain context. The domain context is shared by all
     * cells in a domain.
     */
    protected Map<String,Object> getDomainContext()
    {
        return _endpoint.getDomainContext();
    }

    /**
     * Returns the cell command line arguments provided when the cell
     * was created.
     */
    protected Args getArgs()
    {
        return _endpoint.getArgs();
    }
}
