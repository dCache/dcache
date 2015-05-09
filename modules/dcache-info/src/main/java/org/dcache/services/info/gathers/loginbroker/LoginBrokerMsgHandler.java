package org.dcache.services.info.gathers.loginbroker;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.services.login.LoginBrokerInfo;

import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.util.NetworkUtils;

import static com.google.common.net.InetAddresses.isInetAddress;
import static com.google.common.net.InetAddresses.toUriString;


/**
 * Collects LoginBrokerInfo messages.
 */
public class LoginBrokerMsgHandler implements CellMessageReceiver
{
    private static final StatePath PATH_TO_DOORS = new StatePath("doors");
    private static final double EXPIRATION_FACTOR = 2.5;

    private final StateUpdateManager _sum;

    public LoginBrokerMsgHandler(StateUpdateManager sum)
    {
        _sum = sum;
    }

    public void messageArrived(LoginBrokerInfo info)
    {
        StateUpdate update = new StateUpdate();
        addDoorInfo(update, PATH_TO_DOORS.newChild(info.getIdentifier()), info,
                    TimeUnit.MILLISECONDS.toSeconds((long) (EXPIRATION_FACTOR * info.getUpdateTime())));
        _sum.enqueueUpdate(update);
    }

    /**
     * Add additional state-update to record information about a door.
     * @param update the StateUpdate we are to add metrics to.
     * @param pathToDoor a StatePath under which we are to add data.
     * @param info the information about the door.
     * @param lifetime the duration, in seconds, for this information
     */
    private void addDoorInfo(StateUpdate update, StatePath pathToDoor,
            LoginBrokerInfo info, long lifetime)
    {
        StatePath pathToProtocol = pathToDoor.newChild("protocol");

        conditionalAddString(update, pathToProtocol, "engine",  info.getProtocolEngine(), lifetime);
        conditionalAddString(update, pathToProtocol, "family",  info.getProtocolFamily(), lifetime);
        conditionalAddString(update, pathToProtocol, "version", info.getProtocolVersion(), lifetime);
        conditionalAddString(update, pathToProtocol, "root", info.getRoot(), lifetime);

        update.appendUpdate(pathToDoor.newChild("load"),
                            new FloatingPointStateValue(info.getLoad(), lifetime));
        update.appendUpdate(pathToDoor.newChild("port"),
                new IntegerStateValue(info.getPort(), lifetime));
        update.appendUpdate(pathToDoor.newChild("cell"),
                new StringStateValue(info.getCellName(), lifetime));
        update.appendUpdate(pathToDoor.newChild("domain"),
                new StringStateValue(info.getDomainName(), lifetime));
        update.appendUpdate(pathToDoor.newChild("update-time"),
                            new IntegerStateValue(info.getUpdateTime(), lifetime));

        info.getAddresses().stream().forEach(
                i -> addInterfaceInfo(update, pathToDoor.newChild("interfaces"), i, lifetime));

        update.appendUpdateCollection(pathToDoor.newChild("tags"), info.getTags(), lifetime);
    }


    /**
     * Add a string metric at a specific point in the State tree if the value is not NULL.
     * @param update the StateUpdate to append with the metric definition
     * @param parentPath the path to the parent branch for this metric
     * @param name the name of the metric
     * @param value the metric's value, or null if the metric should not be added.
     * @param storeTime how long, in seconds the metric should be preserved.
     */
    private void conditionalAddString(StateUpdate update, StatePath parentPath,
            String name, String value, long storeTime)
    {
        if (value != null) {
            update.appendUpdate(parentPath.newChild(name),
                    new StringStateValue(value, storeTime));
        }
    }


    /**
     * Add a standardised amount of information about an interface.  This is in the form:
     * <pre>
     *     [interfaces]
     *       |
     *       |
     *       +--[ id ] (branch)
     *       |   |
     *       |   +-- "order"  (integer metric: 1 .. 2 ..)
     *       |   +-- "FQDN" (string metric: the host's FQDN, as presented by the door)
     *       |   +-- "address" (string metric: the host's address; e.g., "127.0.0.1")
     *       |   +-- "address-type"    (string metric: "IPv4", "IPv6" or "unknown")
     *       |   +-- "scope"    (string metric: "IPv4", "IPv6" or "unknown")
     *       |
     * </pre>
     * @param update The StateUpdate to append the new metrics.
     * @param parentPath the path that the id branch will be added.
     * @param lifetime how long the created metrics should last.
     */
    private void addInterfaceInfo(StateUpdate update, StatePath parentPath,
            InetAddress address, long lifetime)
    {
        StatePath pathToInterfaceBranch = parentPath.newChild(address.getHostAddress());

        String hostName = address.getHostName();
        update.appendUpdate(pathToInterfaceBranch.newChild("FQDN"), new StringStateValue(hostName, lifetime));

        String urlName = (isInetAddress(hostName)) ? toUriString(address) : hostName;
        update.appendUpdate(pathToInterfaceBranch.newChild("url-name"), new StringStateValue(urlName, lifetime));

        update.appendUpdate(pathToInterfaceBranch.newChild("address"), new StringStateValue(address.getHostAddress(), lifetime));
        update.appendUpdate(pathToInterfaceBranch.newChild("address-type"),
                                                new StringStateValue((address instanceof Inet4Address) ? "IPv4" : (address instanceof Inet6Address) ? "IPv6" : "unknown", lifetime));
        update.appendUpdate(pathToInterfaceBranch.newChild("scope"), new StringStateValue(NetworkUtils.InetAddressScope.of(address).toString().toLowerCase()));
    }
}
