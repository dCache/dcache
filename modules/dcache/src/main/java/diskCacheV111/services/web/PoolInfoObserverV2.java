// $Id: PoolInfoObserverV2.java,v 1.2 2006-06-08 15:23:27 patrick Exp $Cg
package diskCacheV111.services.web;

import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.PrintWriter;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.CommandExitException;
import diskCacheV111.pools.PoolCellInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PoolInfoObserverException extends Exception
{
    PoolInfoObserverException(String message)
    {
        super(message);
    }
}

public class PoolInfoObserverV2 extends CellAdapter implements Runnable
{
    private final static Logger _log =
        LoggerFactory.getLogger(PoolInfoObserverV2.class);

    private File    _configFile;
    private long    _interval       = 60000;
    private long    _counter        = 0;
    private String  _dCacheInstance = "?";
    private long    _poolManagerTimeout     = 30000L;
    private boolean _poolManagerUpdating    = false;
    private long    _configFileLastModified = 0L;
    private long    _poolManagerNextQuery   = 0L;
    private long    _poolManagerUpdate      = 5L * _interval;
    private String  _poolManagerName        = "PoolManager";

    private final CellNucleus _nucleus;
    private final Args _args;
    private final Map<String,CellQueryInfo> _infoMap =
        new HashMap<String,CellQueryInfo>();
    private Thread _collectThread;
    private Thread _senderThread;

    private final Object _lock = new Object();
    private final Object _infoLock = new Object();
    private final Object _poolManagerUpdateLock = new Object();

    private CellInfoContainer _container =
        new CellInfoContainer();
    private final SimpleDateFormat _formatter =
        new SimpleDateFormat ("MM/dd hh:mm:ss");

    public String hh_x_addto_pgroup =
        "<groupName>  <name> [-pattern[=<pattern>}] [-class=<className>]";
    public String ac_x_addto_pgroup_$_2(Args args)
    {
        String groupClass = (String)args.getOpt("view");
        groupClass = groupClass == null ? "default" : groupClass;
        String groupName = args.argv(0);
        String name      = args.argv(1);
        String pattern   = args.getOpt("pattern");

        if (pattern == null) {
            _container.addPool(groupClass, groupName, name);
        } else {
            if (pattern.length() == 0)
                pattern = name;
            _container.addPattern(groupClass, groupName, name, pattern);
        }

        return "";
    }

    public String hh_x_add = "<pool> <poolValue> # debug only";
    public String ac_x_add_$_2(Args args)
    {
        _container.addInfo(args.argv(0), args.argv(1));
        return "";
    }

    public String hh_x_removefrom =
        "<poolGroup> [-class=<className>] <name> [-pattern]";
    public String ac_x_removefrom_$_2(Args args)
    {
        String groupClass = (String)args.getOpt("view");
        groupClass = groupClass == null ? "default" : groupClass;
        String poolGroup = args.argv(0);
        String name      = args.argv(1);
        if (!args.hasOption("pattern")) {
            _container.removePool(groupClass, poolGroup, name);
        } else {
            _container.removePattern(groupClass, poolGroup, name);
        }
        return "";
    }

    public String ac_x_info(Args args)
    {
        return _container.getInfo();
    }

    public String hh_scan_poolmanager = "[<poolManager>]";
    public String ac_scan_poolmanager_$_0_1(Args args)
    {
        final String poolManagerName = args.argc() == 0 ?
            "PoolManager":args.argv(0);

        _nucleus.newThread(new Runnable() {
                               public void run() {
                                   _log.info("Starting pool manager (" +poolManagerName+ ") scan");
                                   try {
                                       collectPoolManagerPoolGroups(poolManagerName);
                                   } catch (Exception e) {
                                       _log.warn("Problem in collectPoolManagerPoolGroups : " + e);
                                   } finally {
                                       _log.info("collectPoolManagerPoolGroups done");
                                   }
                               }
                           }).start();
        return "Scan initialed (check pinboard for results)";
    }

    public String hh_addto_pgroup =
        "<poolGroup> [-class=<poolClass>] <poolName> | /poolNamePattern/ [...]";
    public String ac_addto_pgroup_$_2_999(Args args)
    {
        StringBuffer  sb = new StringBuffer();
        String groupName = args.argv(0);
        String className = args.getOpt("view");

        if (className == null)
            className = "default";
        if (className.length() == 0)
            throw new
                IllegalArgumentException("class name must not be \"\"");

        CellInfoContainer container = _container;
        synchronized(container) {
            for (int i = 1, n = args.argc(); i < n; i++) {
                String name = args.argv(i);
                if (name.startsWith("/")) {
                    if (name.length() < 3 || !name.endsWith("/")) {
                        sb.append("Not a valid pattern : ")
                            .append(name)
                            .append("\n");
                        continue;
                    }
                    name = name.substring(1, name.length() - 1);
                    container.addPattern(className, groupName, name, name);
                } else {
                    container.addPool(className, groupName, name);
                }
            }
        }
        String result = sb.toString();
        if (result.length() != 0)
            throw new
                IllegalArgumentException(result);

        return "";
    }

    private void collectPoolManagerPoolGroups(String poolManager)
        throws PoolInfoObserverException, NoRouteToCellException,
               InterruptedException
    {
        synchronized (_poolManagerUpdateLock) {
            if (_poolManagerUpdating) {
                _log.info("PoolManager update already in progress");
                return;
            }
            _poolManagerUpdating = true;
        }
        try {
            _collectPoolManagerPoolGroups(poolManager);
        } finally {
            synchronized(_poolManagerUpdateLock) {
                _poolManagerUpdating = false;
            }
        }
    }

    private void _collectPoolManagerPoolGroups(String poolManager)
        throws PoolInfoObserverException, NoRouteToCellException,
               InterruptedException
    {
        CellPath path = new CellPath(poolManager);
        CellMessage message =
            new CellMessage(path, "psux ls pgroup");
        message = sendAndWait(message, _poolManagerTimeout);
        if (message == null)
            throw new PoolInfoObserverException("Request to "
                                                + poolManager
                                                + " timed out");

        Object result = message.getMessageObject();
        if (result instanceof Exception)
            throw new PoolInfoObserverException("Pool manager returned: " + result);

        if (! (result instanceof Object []))
            throw new
                PoolInfoObserverException("Illegal Reply on 'psux ls pgroup");

        for (Object o : (Object[])result) {
            if (o == null)
                continue;
            String pgroupName = o.toString();
            String request = "psux ls pgroup " +pgroupName;
            message = new CellMessage(path, request);
            message = sendAndWait(message, _poolManagerTimeout);
            if (message == null) {
                _log.warn("Request to " +poolManager+ " timed out");
                continue;
            }
            result = message.getMessageObject();
            if (! (result instanceof Object[])) {
                _log.warn("Illegal reply (1) on " + request + " "
                     + result.getClass().getName());
                continue;
            }
            Object [] props = (Object [])result;
            if ((props.length < 3) ||
                (! (props[0] instanceof String)) ||
                (! (props[1] instanceof Object []))) {
                _log.warn("Illegal reply (2) on " +request);
                continue;
            }
            CellInfoContainer container = _container;
            synchronized(container) {
                for (Object p : (Object [])props[1]) {
                    container.addPool("PoolManager", pgroupName, p.toString());
                }
            }
        }
    }

    private class CellQueryInfo
    {
        private String      _destination = null;
        private long        _diff        = -1;
        private long        _start       = 0;
        private CellInfo    _info        = null;
        private CellMessage _message     = null;
        private long        _lastMessage = 0;

        private CellQueryInfo(String destination)
        {
            _destination = destination;
            _message = new CellMessage(new CellPath(_destination),
                                       "xgetcellinfo");
        }

        private String getName()
        {
            return _destination;
        }

        private CellInfo getCellInfo()
        {
            return _info;
        }

        private long getPingTime()
        {
            return _diff;
        }

        private long getArrivalTime()
        {
            return _lastMessage;
        }

        private CellMessage getCellMessage()
        {
            _start = System.currentTimeMillis();
            return _message;
        }

        private String getCellName()
        {
            return _info.getCellName();
        }

        private String getDomainName()
        {
            return _info.getDomainName();
        }

        private void infoArrived(CellInfo info)
        {
            _info = info;
            _lastMessage = System.currentTimeMillis();
            _diff = _lastMessage - _start;
        }

        private boolean isOk()
        {
            return (System.currentTimeMillis() - _lastMessage) < (3 * _interval);
        }

        public String toString()
        {
            return "[" +_destination + "(" + (_diff/1000L) + ")"
                + (_info == null ? "NOINFO" : _info.toString()) +  ")]";
        }
    }

    public PoolInfoObserverV2(String name, String args)
    {
        super(name,PoolInfoObserverV1.class.getName(), args, false);
        _args    = getArgs();
        _nucleus = getNucleus();

        String instance = _args.getOpt("dCacheInstance");

        _dCacheInstance = (instance == null) ||
            (instance.length() == 0) ?
            _dCacheInstance : instance;

        String configName = _args.getOpt("config");
        if ((configName != null) && (! configName.equals("")))
            _configFile = new File(configName);

        String intervalString = _args.getOpt("pool-refresh-time");
        if (intervalString != null) {
            try {
                _interval = Long.parseLong(intervalString) * 1000L;
            } catch (NumberFormatException iee) {
            }
        }
        intervalString = _args.getOpt("poolManager-refresh-time");
        if (intervalString != null) {
            try {
                _poolManagerUpdate = Long.parseLong(intervalString) * 1000L;
            } catch (NumberFormatException iee) {
            }
        }
        for (int i = 0; i < _args.argc(); i++)
            addQuery(_args.argv(i)) ;

        _senderThread  = _nucleus.newThread(this, "sender");
        _senderThread.start();

        _log.info("Sender started");

        _log.info("Collector will be started a bit delayed");

        _nucleus.newThread(new DoDelayedOnStartup(), "init").start();

        start();
    }

    private class DoDelayedOnStartup implements Runnable
    {
        public void run() {
            /*
             * wait for awhile before starting startup processes
             */
            _log.info("Collector will be delayed by " +_interval/2000L+ " Seconds");

            try {
                Thread.currentThread().sleep(_interval/2);
            } catch (InterruptedException ee) {
                Thread.currentThread().interrupt();
                return;
            }

            _collectThread =
                _nucleus.newThread(PoolInfoObserverV2.this, "collector");
            _collectThread.start();

            _log.info("Collector now started as well");
            _log.info("Getting pool groups from PoolManager");

            try {
                collectPoolManagerPoolGroups("PoolManager");
            } catch (Exception e) {
                _log.warn("Problem in collectPoolManagerPoolGroups : "  + e);
            }
            _log.info("collectPoolManagerPoolGroups done");
        }
    }

    private boolean loadConfigFile()
    {
        if (_configFile == null || !_configFile.exists() || !_configFile.canRead())
            return false;

        long accessTime = _configFile.lastModified();

        if (_configFileLastModified >= accessTime)
            return false;

        /*
         * save current setup
         */
        CellInfoContainer c = _container;
        /*
         * install new (empty) setup
         */
        _container = new CellInfoContainer();

        try {
            BufferedReader br =
                new BufferedReader(new FileReader(_configFile));
            try {
                String line;
                while ((line = br.readLine()) != null) {
                    _log.info(line);
                    command(line);
                }
            } catch (IOException e) {
                _log.warn(e.toString(), e);
            } finally {
                try {
                    br.close();
                } catch (IOException e) {
                }
            }
            _configFileLastModified = accessTime;
        } catch (FileNotFoundException e) {
            _log.warn("Could not open " + _configFile + " due to "  + e);
            _container = c;
            return false;
        } catch (CommandExitException e) {
            _log.warn("Could not execute " + _configFile + " due to "  + e);
            _container = c;
            return false;
        }

        return true;
    }

    private synchronized void addQuery(String destination)
    {
        if (_infoMap.get(destination) == null) {
            _log.info("Adding " + destination);
            _infoMap.put(destination, new CellQueryInfo(destination));
        }
    }

    private synchronized void removeQuery(String destination)
    {
        _log.info("Removing " + destination);
        _infoMap.remove(destination);
    }

    public void run()
    {
        Thread x = Thread.currentThread();
        if (x == _senderThread) {
            runSender();
        } else {
            runCollector();
        }
    }

    private void runCollector()
    {
        while (!Thread.currentThread().interrupted()) {
            synchronized (_infoLock) {
                _log.debug("Updating info in context poolgroup-map.ser");
                flushTopologyMap("poolgroup-map.ser");
                _log.debug("Updating info in context Done");
            }
            try {
                Thread.currentThread().sleep(_interval);
            } catch (InterruptedException iie) {
                _log.info("Collector Thread interrupted");
                break;
            }
        }

    }

    private void runSender()
    {
        _poolManagerNextQuery =
            System.currentTimeMillis() + _poolManagerUpdate;

        while (! Thread.currentThread().interrupted()) {
            _counter++;

            synchronized (_infoLock) {
                for (CellQueryInfo info : _infoMap.values()) {
                    try {
                        CellMessage cellMessage = info.getCellMessage();
                        _log.debug("Sending message to " +cellMessage.getDestinationPath());
                        sendMessage(info.getCellMessage());
                    } catch (NoRouteToCellException e) {
                        _log.warn("Problem in sending message : " + e);
                    }
                }
            }

            /*
             * if a new ConfigFile has been loaded we need to reget the
             * the poolManager pool groups.
             */
            long now = System.currentTimeMillis();
            if (loadConfigFile() || (_poolManagerNextQuery < now)) {
                try {
                    _log.debug("collectPoolManagerPoolGroups started on "
                         + _poolManagerName);
                    collectPoolManagerPoolGroups(_poolManagerName);
                    _poolManagerNextQuery = now + _poolManagerUpdate;
                } catch (Exception e) {
                    _log.warn("Problems reported by 'collectPoolManagerPoolGroups' : " + e);
                }
            }
            try {
                Thread.currentThread().sleep(_interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                _log.info("Sender Thread interrupted");
                break;
            }
        }

    }

    public String hh_show_context = "<contextName>";
    public String ac_show_context_$_1(Args args)
    {
        String contextName = args.argv(0);
        Object o = _nucleus.getDomainContext(contextName);
        if (o == null)
            throw new IllegalArgumentException("Context not found : "
                                               + contextName);

        return o.toString();
    }

    public void messageArrived(CellMessage message)
    {
        CellPath path = message.getSourcePath();
        Object  reply = message.getMessageObject();

        _log.info("Message arrived : " + reply.getClass().getName()
            + " from " + path);
        String destination = (String)path.getCellName();
        CellQueryInfo info = (CellQueryInfo)_infoMap.get(destination);
        if (info == null) {
            _log.debug("Unexpected reply arrived from : " +path);
            return;
        }
        //
        // generic cell info
        //
        if (reply instanceof CellInfo) {
            info.infoArrived((CellInfo)reply);
            CellInfoContainer container = _container;
            synchronized(container) {
                container.addInfo(info.getName(), info);
            }
        }
        //
        // special pool manager cell info (without the pool manager cellinfo
        // this cell won't do anything.
        //
        if (reply instanceof diskCacheV111.poolManager.PoolManagerCellInfo) {
            String [] poolList =
                ((diskCacheV111.poolManager.PoolManagerCellInfo)reply).getPoolList();
            synchronized(_infoLock) {
                for (String pool : poolList)
                    addQuery(pool);
            }
        }
    }

    public void cleanUp()
    {
        _log.info("Clean Up sequence started");
        //
        // wait for the worker to be done
        //
        _log.info("Waiting for collector thread to be finished");
        _collectThread.interrupt();
        _senderThread.interrupt();

        _log.info("Clean Up sequence done");
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println("                    Version : $Id: PoolInfoObserverV2.java,v 1.2 2006-06-08 15:23:27 patrick Exp $");
        pw.println("       Pool Update Interval : " +_interval+ " [msec]");
        pw.println("PoolManager Update Interval : " +_poolManagerUpdate+ " [msec]");
        pw.println(" Update Counter : " +_counter);
        pw.println("       Watching : " +_infoMap.size()+ " cells");
    }

    private Map scanTopologyMap(String topoMapString)
    {
        Map allClasses   = new HashMap();
        Map currentClass = null;
        Map currentGroup = null;
        for (String line : topoMapString.split("\n")) {
            if (line.length() == 0)
                continue;
            if (line.startsWith("++")) {
                if (currentGroup == null)
                    continue;
                currentGroup.put(line.substring(2), null);
            } else if (line.startsWith("+")) {
                if (currentClass == null)
                    continue;
                currentClass.put(line.substring(1), currentGroup = new HashMap());
            } else {
                allClasses.put(line.trim(), currentClass = new HashMap());
            }
        }
        return allClasses;
    }

    private void flushTopologyMap(String topologyMapName)
    {
        PoolCellQueryContainer container = new PoolCellQueryContainer();

        synchronized (_infoLock) {
            for (CellQueryInfo info : _infoMap.values()) {
                CellInfo cellInfo = info.getCellInfo();
                if (cellInfo instanceof PoolCellInfo) {
                    container.put(info.getCellName(),
                                  new PoolCellQueryInfo((PoolCellInfo)cellInfo,
                                                        info.getPingTime(),
                                                        info.getArrivalTime()));
                }
            }
        }
        Map<String,Map<String,Map<String,Object>>> allClasses
            = _container.createExternalTopologyMap();
        for (Map<String,Map<String,Object>> groupMap:
                 allClasses.values()) {
            for (Map<String,Object> tableMap : groupMap.values()) {
                for (String poolName : tableMap.keySet()) {
                    tableMap.put(poolName,
                                 container.getInfoByName(poolName));
                }
            }
        }
        container.setTopology(allClasses);
        _nucleus.setDomainContext(topologyMapName, container);
    }
}
