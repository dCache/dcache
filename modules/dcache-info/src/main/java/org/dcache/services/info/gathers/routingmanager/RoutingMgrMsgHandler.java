package org.dcache.services.info.gathers.routingmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;
import org.dcache.services.info.gathers.cells.CellInfoMsgHandler;

/**
 * This class handles incoming messages from the RoutingMgr cell of some specific domain.  The messages are the
 * result of an "ls -x" admin command, which returns a dump of that RoutingMgr's current routing knowledge.
 * <p>
 * The message is an array of three items: the domain Name (that the RoutingMgr cell is running within), the
 * Set of locally registered cells and the knowledge of remote cells.  The remote knowledge is in the form of a Map between
 * a domain's name and the Set of registered cells at that domain.
 * <p>
 * Given the set of locally registered and remote registered cells, a Map of well-known cells is derived, which maps the
 * well-known cell's name to its corresponding domain.  This assumes (and enforces) that registered cell names are unique.
 * If they are not unique, then the locally registered cell is chosen preferentially.  If two remote registered cells have
 * the same name then it is unspecified which one will be chosen.
 * <p>
 * The information is added under the RoutingMgr's domain, under the "routing" branch.  There are three
 * sub-branches of "routing" : named-cells, local and remote.  The following illustrates this:
 * <pre>
 *  [domains]
 *   |
 *   +--[&lt;domain name #1>]
 *   |   |
 *   |   +--[routing]
 *   |   |   |
 *   |   |   +--[named-cells]
 *   |   |   |   |
 *   |   |   |   +--[&lt;cell name #1>]
 *   |   |   |   |   |
 *   |   |   |   |   +--[&lt;domain name #1>]
 *   |   |   |   |
 *   |   |   |   +--[&lt;cell name #2>]
 *   |   |   |   |   |
 *   |   |   |   |   +--[&lt;domain name #2>]
 *   |   |   |   |
 *   |   |   |   +--[&lt;cell name #3>]
 *   |   |   |       |
 *   |   |   |       +--[&lt;domain name #2>]
 *   |   |   |
 *   |   |   +--[local]
 *   |   |   |   |
 *   |   |   |   +--[&lt;cell name #1>]
 *   |   |   |
 *   |   |   +--[remote]
 *   |   |   |   |
 *   |   |   |   +--[&lt;domain name #2>]
 *   |   |   |       |
 *   |   |   |       +--[&lt;cell name #2>]
 *   |   |   |       |
 *   |   |   |       +--[&lt;cell name #3>]
 *   |   |   |
 *   |   |   +--[cells]
 *   |   |   |   |
 * </pre>
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class RoutingMgrMsgHandler extends CellMessageHandlerSkel
{
    private static Logger _log = LoggerFactory.getLogger(CellInfoMsgHandler.class);

    private static final StatePath DOMAINS_PATH = new StatePath("domains");

    public RoutingMgrMsgHandler(StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgMetaRepo)
    {
        super(sum, msgMetaRepo);
    }


    @SuppressWarnings("unchecked")
    @Override
    public void process(Object msgPayload, long metricLifetime)
    {
        _log.debug("received msg.");

        if (!msgPayload.getClass().isArray()) {
            _log.error("received a reply message that isn't an array; type is " + msgPayload.getClass().getName());
            return;
        }

        Object result[] = (Object[]) msgPayload;

        // Extract the information from the msg payload.
        String domainName = (String) result[0];
        Set<String> localExports = (Set<String>) result[1];
        Map<String,Set<String>> domainHash = (Map<String,Set<String>>) result[2];

        // Construct our well-known cells map.
        Map<String,String> wellKnownCells = new HashMap<>();
        buildWellKnownCells(wellKnownCells, domainName, localExports, domainHash);

        if (wellKnownCells.isEmpty() && localExports.isEmpty() &&
                domainHash.isEmpty()) {
            _log.debug("Message from domain {} with no well-known cells", domainName);
            return;
        }

        // Build our new metrics

        StatePath routingPath = DOMAINS_PATH.newChild(domainName).newChild("routing");

        StateUpdate update = new StateUpdate();

        addWellKnownCells(update, routingPath, wellKnownCells, metricLifetime);
        addLocalCells(update, routingPath, localExports, metricLifetime);
        addRemoteCells(update, routingPath, domainHash, metricLifetime);

        applyUpdates(update);
    }


    /**
     * Build a well-known cells mapping.  This maps from a cell name to the corresponding domain.
     * If there is a name-clash, with two registered cells having the same name, the local cell
     * is chosen in preference to a remote cell.  If two remote cells have the same name, which
     * cell is chosen is not specified.  That all said, one should never see a name clash.
     * @param wellKnownCells the Map of wellKnownCells we are to update with information
     * @param domainName the name of the domain the RoutingMgr cell is running within
     * @param localExports the Set of cell names of all locally registered cells
     * @param domainHash the Map between a (remote) domain and the registered cells the RoutingMgr
     * knows about.
     */
    private void buildWellKnownCells(Map<String,String> wellKnownCells, String domainName,
            Set<String> localExports, Map<String,Set<String>> domainHash)
    {
        for (Map.Entry<String, Set<String>> entry : domainHash.entrySet()) {
            String thisDomainName = entry.getKey();

            for (String thisCellName : entry.getValue()) {
                wellKnownCells.put(thisCellName, thisDomainName);
            }
        }

        for (String cellName : localExports) {
            wellKnownCells.put(cellName, domainName);
        }
    }


    /**
     * Add the new metrics for the well-known cells.  This maps all well-known cell name to the
     * corresponding domain under the branch:
     * <pre>
     * domains.&lt;this domain name>.routing.well-known.&lt;cell name>.&lt;domain name>
     * </pre>
     * @param update the StateUpdate to append metrics to
     * @param routingPath the StatePath to the appropriate "routing" branch.
     * @param wellKnownCells the Map between cell name and domain name
     * @param metricLifetime how long, in seconds, these metrics should last.
     */
    private void addWellKnownCells(StateUpdate update, StatePath routingPath,
            Map<String,String> wellKnownCells, long metricLifetime)
    {
        StatePath namedCellsPath = routingPath.newChild("named-cells");

        for (Map.Entry<String, String> entry : wellKnownCells.entrySet()) {
            String thisCellName = entry.getKey();
            String thisDomainName = entry.getValue();

            StatePath thisEntryPath = namedCellsPath.newChild(thisCellName).newChild(thisDomainName);

            update.appendUpdate(thisEntryPath, new StateComposite(metricLifetime));
        }
    }


    /**
     * Add new metrics for the local registered cells.  These are recorded as metrics like:
     * <pre>
     * domains.&lt;this domain name>.routing.local.&lt;cell name>
     * </pre>
     * @param update the StateUpdate to append metrics to
     * @param routingPath the StatePath to the appropriate routing branch.
     * @param localCells the Set of cell names
     * @param metricLifetime how long, in seconds, the metrics should last.
     */
    private void addLocalCells(StateUpdate update, StatePath routingPath, Set<String> localCells,
            long metricLifetime)
    {
        StatePath localCellsPath = routingPath.newChild("local");

        for (String cellName : localCells) {
            StatePath thisCellPath = localCellsPath.newChild(cellName);

            update.appendUpdate(thisCellPath, new StateComposite(metricLifetime));
        }
    }


    /**
     * Add new metrics for the remote registered cells.  These are recorded as metrics like:
     * <pre>
     * domains.&lt;this domain name>.routing.remote.&lt;domain name>.&lt;cell name>
     * </pre>
     * @param update the StateUpdate to append metrics to
     * @param routingPath the StatePath to the appropriate routing branch.
     * @param domainHash the Map between a domain name and the Set of cell names
     * @param metricLifetime how long, in seconds, the metrics should last.
     */
    private void addRemoteCells(StateUpdate update, StatePath routingPath,
            Map<String,Set<String>> domainHash, long metricLifetime)
    {
        StatePath remoteCellsPath = routingPath.newChild("remote");

        for (Map.Entry< String, Set<String>> entry : domainHash.entrySet()) {

            StatePath domainPath = remoteCellsPath.newChild(entry.getKey());

            for (String cellName : entry.getValue()) {
                StatePath cellPath = domainPath.newChild(cellName);

                update.appendUpdate(cellPath, new StateComposite(metricLifetime));
            }
        }
    }
}
