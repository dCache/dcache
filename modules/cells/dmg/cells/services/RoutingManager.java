package dmg.cells.services;

import java.util.*;
import java.io.*;
import dmg.cells.nucleus.*;
import dmg.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  * The dmg.cells.services.RoutingManager is a ready to use
  * service Cell, performing the following services :
  * <ul>
  * <li>Watching a specified tunnel cell and setting the
  *     default route to this cell as soon as this tunnel
  *     cell establishes its domain route.
  * <li>Assembling downstream routing informations and
  *     the exportCell EventListener Event and maitaining
  *     a wellknown Cell list.
  * <li>Sending its wellknown cell list upstream as soon
  *     as a default route is available and whenever
  *     the wellknown cell list changes.
  * </ul>
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class RoutingManager
    extends CellAdapter
    implements CellEventListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(RoutingManager.class);

    private final CellNucleus _nucleus;
    private final Args _args;
    private final Set<String> _localExports = new HashSet();
    private final Map<String,Set<String>> _domainHash = new HashMap();
    private final String _watchCell;
    private boolean _defaultInstalled = false;

    public RoutingManager(String name, String args)
    {
        super(name,"System", args, false);
        _nucleus = getNucleus();
        _args = getArgs();

        _nucleus.addCellEventListener(this);
        _watchCell = _args.argc() == 0 ? null : _args.argv(0);

        start();
    }

    public synchronized void getInfo(PrintWriter pw)
    {
        pw.println(" Our routing knowledge :");
        pw.append(" Local : ").println(_localExports);

        for (Map.Entry<String,Set<String>> e : _domainHash.entrySet()) {
            pw.append(" ").append(e.getKey()).append(" : ").println(e.getValue());
        }
    }

    private synchronized void setDefaultInstalled(boolean value)
    {
        _defaultInstalled = value;
    }

    private synchronized boolean isDefaultInstalled()
    {
        return _defaultInstalled;
    }

    private void addWellknown(String cell, String domain)
    {
        if (cell.startsWith("@"))
            return;
        try {
            _nucleus.routeAdd(new CellRoute(cell,
                                             "*@"+domain,
                                             CellRoute.WELLKNOWN));
        } catch (IllegalArgumentException e) {
            _log.warn("Couldn't add wellknown route : " + e.getMessage());
        }
    }

    private void removeWellknown(String cell, String domain)
    {
        if (cell.startsWith("@"))
            return;
        try {
            _nucleus.routeDelete(new CellRoute(cell,
                                                "*@"+domain,
                                                CellRoute.WELLKNOWN));
        } catch (IllegalArgumentException e) {
            _log.warn("Couldn't delete wellknown route : " + e.getMessage());
        }
    }

    private synchronized void updateUpstream()
    {
        List<String> all = new ArrayList();
        _log.info("update requested to upstream Domains");
        //
        // the protocol requires the local DomainName
        // first
        //
        all.add(_nucleus.getCellDomainName());
        //
        // here we add our own exportables
        //
        all.addAll(_localExports);

        //
        // and now all the others
        //

        for (Set<String> cells : _domainHash.values()) {
            all.addAll(cells);
        }

        String destinationManager = _nucleus.getCellName();
        _log.info("Resending to " + destinationManager + " : " + all);
        try {
            CellPath path = new CellPath(destinationManager);
            String[] arr = all.toArray(new String[0]);
            _nucleus.resendMessage(new CellMessage(path, arr));
        } catch (NoRouteToCellException e) {
            /* This normally happens when there is no default route.
             */
            _log.info("Cannot send routing information to RoutingMgr: " + e.getMessage());
        }
    }

    private synchronized void addRoutingInfo(String[] info)
    {
        String domain = info[0];
        Set<String> oldCells = _domainHash.get(domain);
        Set<String> newCells = new HashSet<String>();
        for (int i = 1; i < info.length; i++){
            newCells.add(info[i]);
        }

        if (oldCells == null) {
            _log.info("Adding new domain : " + domain);
            for (String cell : newCells) {
                addWellknown(cell, domain);
            }
        } else {
            _log.info("Updating domain : " + domain);
            for (String cell : newCells) {
                _log.info("Adding : " + cell);
                if (!oldCells.remove(cell)) {
                    // entry not found, so make it
                    addWellknown(cell, domain);
                }
            }
            // all additional route added now, need to remove the rest
            for (String cell : oldCells) {
                _log.info("Removing : " + cell);
                removeWellknown(cell, domain);
            }
        }
        _domainHash.put(domain, newCells);
        if (isDefaultInstalled())
            updateUpstream();
    }

    private synchronized void removeRoutingInfo(String domain)
    {
        _log.info("Removing all routes to domain : " + domain);
        Set<String> cells = _domainHash.remove(domain);
        if (cells == null){
            _log.info("No entry found for domain : " + domain);
            return;
        }
        for (String cell : cells)
            removeWellknown(cell, domain);
    }

    public void messageArrived(CellMessage msg)
    {
        Object obj = msg.getMessageObject();
        if (obj instanceof String[]){
            String[] info = (String[])obj;
            if (info.length < 1){
                _log.warn("Protocol error 1 in routing info");
                return;
            }
            _log.info("Routing info arrived for Domain : " + info[0]);
            addRoutingInfo(info);
        } else {
            _log.warn("Unidentified message ignored : " + obj);
        }
    }

    public void cellCreated(CellEvent ce)
    {
        String name = (String)ce.getSource();
        _log.info("cellCreated : " + name);
    }

    public synchronized void cellDied(CellEvent ce)
    {
        String name = (String) ce.getSource();
        _log.info("cellDied : "+name);
        _localExports.remove(name);
        updateUpstream();
    }

    public synchronized void cellExported(CellEvent ce)
    {
        String name = (String)ce.getSource();
        _log.info("cellExported : " + name);
        _localExports.add(name);
        updateUpstream();
    }

    public void routeAdded(CellEvent ce)
    {
        CellRoute       cr   = (CellRoute)ce.getSource();
        CellAddressCore gate = new CellAddressCore(cr.getTargetName());
        _log.info("Got 'route added' event : " + cr);
        if (cr.getRouteType() == CellRoute.DOMAIN){
            if ((_watchCell != null) && gate.getCellName().equals(_watchCell)) {
                //
                // the upstream route (we only support one)
                //
                try {
                    CellRoute defRoute =
                        new CellRoute("",
                                       "*@"+cr.getDomainName(),
                                       CellRoute.DEFAULT);
                    _nucleus.routeAdd(defRoute);
                } catch (IllegalArgumentException e) {
                    _log.warn("Couldn't add default route : " + e.getMessage());
                }
            } else {
                //
                // possible downstream routes
                //
                // _log.info("Downstream route added : "+ cr);
                _log.info("Downstream route added to Domain : " + cr.getDomainName());
                //
                // If the locationManager takes over control
                // the default route may be installed before
                // the actual domainRouted is added. Therefore
                // we have to 'updateUpstream' for each route.
                updateUpstream();
            }
        } else if (cr.getRouteType() == CellRoute.DEFAULT) {
            _log.info("Default route was added");
            setDefaultInstalled(true);
            updateUpstream();
        }
    }

    public void routeDeleted(CellEvent ce)
    {
        CellRoute cr = (CellRoute)ce.getSource();
        CellAddressCore gate = new CellAddressCore(cr.getTargetName());
        if (cr.getRouteType() == CellRoute.DOMAIN) {
            if ((_watchCell != null) && gate.getCellName().equals(_watchCell)) {
                CellRoute defRoute =
                    new CellRoute("",
                                   "*@"+cr.getDomainName(),
                                   CellRoute.DEFAULT);
                _nucleus.routeDelete(defRoute);
            } else {
                removeRoutingInfo(cr.getDomainName());
            }
        } else if (cr.getRouteType() == CellRoute.DEFAULT) {
            setDefaultInstalled(false);
        }
    }

    public String ac_update(Args args)
    {
        updateUpstream();
        return "Done";
    }

    /**
     * This method returns the current state of the RoutingMgr cell as a (binary) Object.
     * <p>
     * NB. <b>This is a hack</b>.  The correct method of receiving information from a
     * Cell is via a Vehicle.  However, as the RoutingMgr is within the cells module (which
     * does not have the concept of Vehicles) this cannot be (easily) done.  Instead, we
     * use the existing mechanism of obtaining a binary object via the admin interface and
     * flag this functionality as something that should be improved later.
     *
     * @return a representation of the RoutingManager's little brain.
     */
    @Deprecated
    public Object ac_ls_$_0( Args args) {

    	Object info;

    	if (!args.hasOption("x")) {
    		// Throw together some meaningful output.
    		ByteArrayOutputStream os = new ByteArrayOutputStream();
    		PrintWriter pw = new PrintWriter( os);
        	getInfo( pw);
        	pw.flush();
        	info = os.toString();
        } else {
        	Object infoArray[] = new Object[3];

        	infoArray[0] = _nucleus.getCellDomainName();
        	infoArray[1] = _localExports;
        	infoArray[2] = _domainHash;

        	info = infoArray;
        }

    	return info;
    }

    public String hh_ls = "[-x]";

}
