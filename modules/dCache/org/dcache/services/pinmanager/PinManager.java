package org.dcache.services.pinmanager;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.sql.SQLException;

import dmg.util.Args;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PinManagerMessage;
import org.dcache.services.AbstractCell;
import org.dcache.services.Option;

/**
 * This cell performs pinning and unpinning service on behalf of other
 * services.
 *
 * Pining/unpinning of the same resources by multiple requestors is
 * coalesced into a single pin.
 *
 * Pin requests are maintained in an external SQL database, although
 * the current implementation keeps all pin requests in memory. Old
 * requests are recovered at startup.
 *
 * Pinned files are marked as such in PNFS and flagged sticky on at
 * least one pool. This is likely to be changed soon.
 */
public class PinManager extends AbstractCell implements Runnable
{
    @Option(
        name = "maxPinDuration",
        description = "Max. lifetime of a pin",
        defaultValue = "86400000", // one day
        unit = "ms"
    )
    protected long _maxPinDuration;

    @Option(
        name = "pnfsManager",
        defaultValue = "PnfsManager",
        description = "PNFS manager name"
    )
    protected String _pnfsManager;

    @Option(
        name = "poolManager",
        defaultValue = "PoolManager",
        description = "Pool manager name"
    )
    protected String _poolManager;

    @Option(
        name = "jdbcUrl",
        required = true
    )
    protected String _jdbcUrl;

    @Option(
        name = "jdbcDriver",
        required = true
    )
    protected String _jdbcDriver;

    @Option(
        name = "dbUser",
        required = true
    )
    protected String _dbUser;

    @Option(
        name = "dbPass",
        log = false
    )
    protected String _dbPass;

    @Option(
        name = "pgPass"
    )
    protected String _pgPass;

    private final PinRequestDatabase _database;

    /**
     * Each pin is registered in this mapping.
     */
    private final Map<PnfsId,Pin> _pnfsIdToPins = new HashMap<PnfsId,Pin>();

    public PinManager(String name, String argString)
        throws SQLException
    {
        super(name, argString, false);

        try {
            _database = new PinRequestDatabase(this,
                                               _jdbcUrl,
                                               _jdbcDriver,
                                               _dbUser,
                                               _dbPass,
                                               _pgPass);

            getNucleus().newThread(this, "UpdateWaitQueueThread").start();
            start();
        } catch (SQLException e) {
            fatal("Error starting PinManager: " + e.getMessage());
            start();
            kill();
            throw e;
        }
    }

    public CellVersion getCellVersion()
    {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision: 1.42 $");
    }

    public PinRequestDatabase getDatabase()
    {
        return _database;
    }

    public long getMaxPinDuration()
    {
        return _maxPinDuration;
    }

    public CellPath getPnfsManager()
    {
        return new CellPath(_pnfsManager);
    }

    public CellPath getPoolManager()
    {
        return new CellPath(_poolManager);
    }

    /**
     * Returns the pin of a particular file. Creates a new pin if no
     * such pin exists.
     */
    synchronized public Pin getPin(PnfsId id)
    {
        if (_pnfsIdToPins.containsKey(id)) {
            return _pnfsIdToPins.get(id);
        }

        Pin pin = new Pin(this, id);
        _pnfsIdToPins.put(id, pin);
        return pin;
    }

    /**
     * Removes a pin from the collection of pins. This does not in any
     * way cause the file to be unpinned.
     */
    synchronized public void removePin(PnfsId id)
    {
        _pnfsIdToPins.remove(id);
    }

    /**
     * Provides status information about the cell.
     */
    public void getInfo(PrintWriter writer)
    {
        writer.println("PinManager");
        _database.getInfo(writer);
        writer.println("\tmaxPinDuration=" + _maxPinDuration + " milliseconds");
        writer.println("\tnumber of files pinned=" + _pnfsIdToPins.size());
    }

    /**
     * Forwards pin manager messages to the appropriate pin.
     */
    public void messageArrived(CellMessage envelope, PinManagerMessage message)
    {
        Pin pin = getPin(message.getPnfsId());
        pin.messageArrived(envelope, message);
    }

    /**
     * This thread will periodically trigger the nucleus to timeout
     * old message for which a response has not been received yet.
     */
    public void run()
    {
        try {
            for (;;) {
                getNucleus().updateWaitQueue();
                Thread.sleep(30000);
            }
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String hh_pin_file =
        "<pnfsId> <seconds> # pin a file by pnfsid for <seconds> seconds";
    public String ac_pin_file_$_2(Args args) throws NumberFormatException
    {
        PnfsId id = new PnfsId(args.argv(0));
        long lifetime = Long.parseLong(args.argv(1)) * 1000;

        if (lifetime <= 0)
            return "Lifetime must be positive.";

        Pin pin = getPin(id);
        pin.adminPin(lifetime);

        return "Pin request added";
    }

    public String hh_unpin_file =
        "<pinRequestId> <pnfsId> # unpin a a file by pinRequestId and by pnfsId";
    public String ac_unpin_file_$_2(Args args) throws NumberFormatException
    {
        long pinId = Long.parseLong(args.argv(0));
        PnfsId id = new PnfsId(args.argv(1));
        Pin pin = getPin(id);
        boolean found = (pin.getRequest(pinId) != null);
        pin.adminUnpin(pinId);

        return found ? "Pin request removed" : "Pin not found";
    }

    public String hh_set_max_pin_duration =
        " # sets new max pin duration value in milliseconds" ;
    public String ac_set_max_pin_duration_$_1(Args args)
        throws NumberFormatException
    {
        long newMaxPinDuration = Long.parseLong(args.argv(0));
        if (newMaxPinDuration <= 0) {
            return "Pin duration value must be positive";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("old max pin duration was ");
        sb.append(_maxPinDuration).append(" milliseconds\n");
        _maxPinDuration = newMaxPinDuration;
        sb.append("max pin duration value set to ");
        sb.append(_maxPinDuration).append(" milliseconds\n");
        return sb.toString();
    }

    public String hh_get_max_pin_duration =
        " # gets current max pin duration value" ;
    public String ac_get_max_pin_duration_$_0(Args args)
        throws NumberFormatException
    {
        return Long.toString(_maxPinDuration) + " milliseconds";
    }

    public String hh_ls =
        " [pnfsId] # lists all pins or specified pin" ;
    public String ac_ls_$_0_1(Args args)
    {
        if (args.argc() > 0) {
            PnfsId pnfsId = new PnfsId(args.argv(0));
            Pin pin = _pnfsIdToPins.get(pnfsId);
            if (pin == null) {
                return "Pin not found";
            }
            return pin.toString();
        }

        Collection<Pin> pins = _pnfsIdToPins.values();
        if (pins.isEmpty()) {
            return "no files are pinned";
        }

        StringBuilder sb = new StringBuilder();
        for (Pin pin : pins) {
            sb.append(pin.toString());
        }
        return sb.toString();
    }
}
