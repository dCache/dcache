// $Id: WebCollectorV3.java,v 1.30 2007-10-29 14:19:08 behrmann Exp $Cg

package diskCacheV111.cells;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.HTMLBuilder;

import dmg.cells.network.PingMessage;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;

import org.dcache.util.Args;

import static org.dcache.util.ByteUnit.BYTES;

public class WebCollectorV3 extends CellAdapter implements Runnable
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(WebCollectorV3.class);

    protected static final String OPTION_REPEATHEADER = "repeatHeader";

    private final CellNucleus _nucleus;
    private final Args       _args;
    @GuardedBy("_infoLock")
    private final Map<CellAddressCore,CellQueryInfo> _infoMap
        = new TreeMap<>(); // TreeMap because we need it sorted
    @GuardedBy("_infoLock")
    private final Set<CellAddressCore> _queues = new HashSet<>();
    private final Object     _infoLock  = new Object();
    private Thread     _collectThread;
    private Thread     _senderThread;
    private long       _counter;
    private int        _repeatHeader    = 30;

    private static class SleepHandler
    {
        private boolean _enabled         = true;
        private boolean _mode            = true;
        private long    _started;
        private long    _shortPeriod     = 20000L;
        private long    _regularPeriod   = 120000L;
        private final long    _retentionFactor = 4;

        private SleepHandler(boolean aggressive)
        {
            _enabled = aggressive;
            _mode    = aggressive;
        }

        private synchronized void sleep() throws InterruptedException
        {
            long start = System.currentTimeMillis();
            wait(_mode ? _shortPeriod / 2 : _regularPeriod / 2);
            LOGGER.debug("Woke up after {} millis", (System.currentTimeMillis() - start));
        }

        private synchronized void setShortPeriod(long shortPeriod)
        {
            _shortPeriod = shortPeriod;
            notifyAll();
        }

        private synchronized void setRegularPeriod(long regularPeriod)
        {
            _regularPeriod = regularPeriod;
            notifyAll();
        }

        private synchronized void topologyChanged(boolean modified)
        {
            // _log.info("Topology changed : {}", modified);
            if (!_enabled) {
                return;
            }
            if (modified) {
                _started = System.currentTimeMillis();
                if (!_mode) {
                    _mode = true;
                    notifyAll();
                    LOGGER.info("Aggressive changed to ON");
                }

            } else if (_mode &&
                      (System.currentTimeMillis() - _started) >
                       (_shortPeriod * _retentionFactor)) {
                _mode = false;
                notifyAll();
                LOGGER.info("Aggressive changed to OFF");
            }

        }

        @Override
        public String toString()
        {
            return "E=" + _enabled +
                   ";A=" + _mode +
                   ";S=" + _shortPeriod +
                   ";Ret" + _retentionFactor +
                   ";R=" + _regularPeriod;
        }
    }

    private SleepHandler     _sleepHandler;
    private final SimpleDateFormat _formatter    = new SimpleDateFormat ("MM/dd HH:mm:ss");

    private class CellQueryInfo
    {
        private final CellAddressCore _destination;
        private       long        _diff        = -1;
        private       long        _start;
        private       CellInfo    _info;
        private       long        _lastMessage;
        private       boolean     _present;

        private CellQueryInfo(CellAddressCore destination)
        {
            _destination = destination;
        }

        private CellAddressCore getDestination()
        {
            return _destination;
        }

        private String      getName()
        {
            return _destination.getCellName();
        }

        private CellInfo    getCellInfo()
        {
            return _info;
        }

        private long        getPingTime()
        {
            return _diff;
        }

        private CellMessage getCellMessage()
        {
            _start = System.currentTimeMillis();
            return new CellMessage(_destination, "xgetcellinfo");
        }

        private void infoArrived(CellInfo info)
        {
            _info = info;
            _diff = (_lastMessage = System.currentTimeMillis()) - _start;
            _present = true;
        }

        private boolean isOk()
        {
            return (System.currentTimeMillis() - _lastMessage) <
                (3 * _sleepHandler._regularPeriod);
        }

        private boolean isPresent()
        {
            return _present;
        }
    }

    public WebCollectorV3(String name, String args)
    {
        super(name, WebCollectorV3.class.getName(), args);
        _args = getArgs();
        _nucleus = getNucleus();
    }

    @Override
    protected void starting()
    {
        if (_args.hasOption(OPTION_REPEATHEADER)) {
            String optionString = null;
            try {
                optionString = _args.getOpt(OPTION_REPEATHEADER);
                _repeatHeader = Math.max(0, Integer.parseInt(optionString));
            } catch (NumberFormatException e) {
                LOGGER.warn("Parsing error in in {} command : {}", OPTION_REPEATHEADER, optionString);
            }
            LOGGER.info("Repeat header set to {}", _repeatHeader);
        }

        String optionString = _args.getOpt("aggressive");
        boolean aggressive =
                (optionString != null) &&
                (optionString.equals("off") || optionString.equals("false"));

        aggressive = !aggressive;

        LOGGER.info("Aggressive mode : {}", aggressive);

        _sleepHandler = new SleepHandler(aggressive);
    }

    @Override
    protected void started()
    {
        synchronized (_infoLock) {
            for (int i = 0; i < _args.argc(); i++) {
                addQuery(new CellAddressCore(_args.argv(i)));
            }
        }
        _senderThread = _nucleus.newThread(this, "sender");
        _senderThread.start();
        LOGGER.info("Sender started");
        LOGGER.info("Collector will be started a bit delayed");
        _collectThread = _nucleus.newThread(WebCollectorV3.this, "collector");
        _collectThread.start();
    }

    @GuardedBy("_infoLock")
    private boolean addQuery(CellAddressCore address)
    {
        if (_infoMap.containsKey(address) || _queues.contains(address)) {
            return false;
        }
        LOGGER.debug("Adding {)", address);
        if (address.isLocalAddress()) {
            _queues.add(address);
            sendPing(address);
        } else {
            CellQueryInfo info = new CellQueryInfo(address);
            _infoMap.put(address, info);
            sendQuery(info);
        }
        return true;
    }

    private void removeQuery(String destination)
    {
        LOGGER.debug("Removing {}", destination);
        synchronized (_infoLock) {
            _infoMap.remove(new CellAddressCore(destination));
        }
    }

    @Override
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
        try {
            Thread.sleep(30000L);
        } catch (InterruptedException e1) {
            return;
        }
        LOGGER.info("Collector now started as well");

        try {
            while (!Thread.interrupted()) {
                synchronized (_infoLock) {
                    preparePage();
                }
                _sleepHandler.sleep();
            }
        } catch (InterruptedException e) {
            LOGGER.info("Collector Thread interrupted");
        }
    }

    private void runSender()
    {
        //CellMessage loginBrokerMessage = new CellMessage(new CellPath

        try {
            while (!Thread.interrupted()) {
                _counter++;
                _sleepHandler.sleep();
                synchronized (_infoLock) {
                    for (CellAddressCore queue : _queues) {
                        sendPing(queue);
                    }
                    for (CellQueryInfo info : _infoMap.values()) {
                        sendQuery(info);
                    }
                }
            }
        } catch (InterruptedException iie) {
            LOGGER.info("Sender Thread interrupted");
        }
    }

    private void sendPing(CellAddressCore address)
    {
        LOGGER.debug("Sending ping to : {}", address);
        sendMessage(new CellMessage(address, new PingMessage()));
    }

    private void sendQuery(CellQueryInfo info)
    {
        LOGGER.debug("Sending query to : {}", info.getDestination());
        sendMessage(info.getCellMessage());
    }

    @Override
    public void messageArrived(CellMessage message)
    {
        Object reply = message.getMessageObject();

        int modified = 0;

        if (reply instanceof LoginBrokerInfo) {
            LoginBrokerInfo brokerInfo = (LoginBrokerInfo) reply;
            synchronized (_infoLock) {
                LOGGER.debug("Login broker reports: {}@{}", brokerInfo.getCellName(), brokerInfo.getDomainName());
                if (addQuery(new CellAddressCore(brokerInfo.getCellName(), brokerInfo.getDomainName()))) {
                    modified++;
                }
            }
        } else if (reply instanceof PingMessage) {
            synchronized (_infoLock) {
                addQuery(message.getSourceAddress());
            }
        } else {
            CellPath path = message.getSourcePath();
            CellAddressCore address = path.getSourceAddress();
            CellQueryInfo info;
            synchronized (_infoLock) {
                info = _infoMap.get(address);
                if (info == null) {
                    // We may have registered the cell as a well known cell
                    info = _infoMap.get(new CellAddressCore(address.getCellName()));
                    if (info == null) {
                        LOGGER.info("Unexpected reply arrived from: {}", path);
                        return;
                    }
                }
            }

            if (reply instanceof CellInfo) {
                LOGGER.debug("CellInfo: {}", ((CellInfo) reply).getCellName());
                info.infoArrived((CellInfo) reply);
            }
            if (reply instanceof PoolManagerCellInfo) {
                Set<CellAddressCore> pools = ((PoolManagerCellInfo) reply).getPoolCells();
                synchronized (_infoLock) {
                    for (CellAddressCore pool : pools) {
                        if (addQuery(pool)) {
                            modified++;
                        }
                    }
                }
            }
        }

        _sleepHandler.topologyChanged(modified > 0);
    }

    public static final String hh_set_repeat_header = "<repeatHeaderCount>|0";
    public synchronized String ac_set_repeat_header_$_1(Args args)
    {
        _repeatHeader = Integer.parseInt(args.argv(0));
        return "";
    }

    private final Map<String,Map<?,?>> _poolGroup = new HashMap<>();
    public static final String hh_define_poolgroup = "<poolgroup> [poolName | /regExpr/ ] ... ";
    public String ac_define_poolgroup_$_1_99(Args args)
    {
        String poolGroupName = args.argv(0);
        synchronized (_poolGroup) {
            Map<?,?> map = _poolGroup.get( poolGroupName);
            if (map == null) {
                _poolGroup.put(poolGroupName, map = new HashMap<>());
            }

            for (int i = 0, n = args.argc() - 1; i < n; i++) {
                String poolName = args.argv(i);
            }
        }
        return "";
    }

    public static final String hh_watch = "<CellAddress> [...]";
    public String ac_watch_$_1_99(Args args)
    {
        synchronized (_infoLock) {
            for (int i = 0; i < args.argc(); i++) {
                addQuery(new CellAddressCore(args.argv(i)));
            }
        }
        return "";
    }

    public static final String hh_dump_info = "[minPingTime] # dumps all info about watched cells";
    public String ac_dump_info_$_0_1(Args args)
    {
        long     minPingTime = 0;
        StringBuilder buf = new StringBuilder();
        if (args.argc() > 0) {
            minPingTime  = Long.parseLong(args.argv(0));
        }

        for (CellQueryInfo info : _infoMap.values()) {
            CellInfo cellInfo = info.getCellInfo();
            long pingTime = info.getPingTime();
            if (pingTime > minPingTime) {
                if (info.isOk()) {
                    buf.append("").append(cellInfo.getDomainName()).append(" ")
                            .append(cellInfo).append(" ").append(pingTime)
                            .append("\n");
                } else if (info.isPresent()) {
                    buf.append("").append(cellInfo.getDomainName()).append(" ")
                            .append(cellInfo).append("\n");
                }
            }
        }
        return buf.toString();
    }

    public static final String hh_unwatch = "<CellAddress> [...]";
    public String ac_unwatch_$_1_99(Args args)
    {
        for (int i = 0; i < args.argc(); i++) {
            removeQuery(args.argv(i));
        }
        return "";
    }

    public static final String hh_set_interval = "[<pingInteval/sec>] [-short=<aggressiveInterval>]";
    public String ac_set_interval_$_1(Args args)
    {

        if (args.argc() > 0) {
            _sleepHandler.setRegularPeriod(1000L * Long.parseLong(args.argv(0)));
        }
        String opt = args.getOpt("short");
        if (opt != null) {
            _sleepHandler.setShortPeriod(1000L * Long.parseLong(opt));
        }
        long _interval = 1000 * Long.parseLong(args.argv(0));
        return "Interval set to "+_interval+" [msecs]";
    }

    private static final int HEADER_TOP    = 0;
    private static final int HEADER_MIDDLE = 1;
    private static final int HEADER_BOTTOM = 2;

    private static class ActionHeaderExtension
    {
        private final TreeMap<String,int[]> _map;  // TreeMap because we need it sorted
        private ActionHeaderExtension(TreeMap<String,int[]> map)
        {
            _map = map == null ? new TreeMap<>() : map;
        }

        @Override
        public String toString()
        {
            return _map.toString();
        }

        int [] [] getSortedMovers(
                Map<String, PoolCostInfo.NamedPoolQueueInfo> moverMap)
        {
            int[][] rows = new int[_map.size()][];
            if (moverMap == null) {
                for (int i = 0; i < _map.size(); i++) {
                    rows[i] = new int[]{-1, -1, -1};
                }
            } else {
                int i = 0;
                for (String key : _map.keySet()) {
                    PoolCostInfo.PoolQueueInfo mover = moverMap.get(key);
                    if (mover == null ) {
                        rows[i] = new int[] { -1, -1, -1 };
                    } else {
                        rows[i] = new int[] {
                            mover.getActive(),
                            mover.getMaxActive(),
                            mover.getQueued()
                        };
                    }
                    i++;
                }
            }
            return rows;
        }

        public Set<String> getSet()
        {
            return _map.keySet();
        }

        public Map<String,int[]> getTotals()
        {
            return _map;
        }
    }

    private void printPoolActionTableHeader(HTMLBuilder page, ActionHeaderExtension ext,
                                            int position)
    {
        assert HEADER_TOP    == 0;
        assert HEADER_MIDDLE == 1;
        assert HEADER_BOTTOM == 2;

        int[][] program = {
            { 0, 1, 2, 3 },
            { 0, 3, 2, 1, 2, 3 },
            { 0, 3, 2, 1 }
        };

        Set<String> moverSet = ext != null ? ext.getSet() : null;
        int diff = moverSet == null ? 0 : moverSet.size();

        for (int i : program[position]) {
            switch (i) {
            case 0:
                int rowspan = program[position].length / 2;
                page.beginRow();
                page.th(rowspan, 1, "cell",   "CellName");
                page.th(rowspan, 1, "domain", "DomainName");
                break;

            case 1:
                page.th(3, null, "Movers");
                page.th(3, null, "Restores");
                page.th(3, null, "Stores");
                page.th(3, null, "P2P-Server");
                page.th(3, null, "P2P-Client");

                if (moverSet != null) {
                    for (String s : moverSet) {
                        page.th(3, null, s);
                    }
                }
                page.endRow();
                break;

            case 2:
                page.beginRow();
                break;

            case 3:
                for (int h = 0, n = 5 + diff; h < n; h++) {
                    page.th("active", "Active");
                    page.th("max",    "Max");
                    page.th("queued", "Queued");
                }
                page.endRow();
                break;
            }
        }
    }

    private void printCellInfoRow(CellInfo info, long ping, HTMLBuilder page)
    {
        page.beginRow(null, "odd");
        page.td("cell",   info.getCellName());
        page.td("domain", info.getDomainName());
        page.td("rp",     info.getEventQueueSize());
        page.td("th",     info.getThreadCount());
        page.td("ping",   ping + " msec");
        page.td("time",   _formatter.format(info.getCreationTime()));
        try {
            page.td("version", info.getCellVersion());
        } catch (NoSuchMethodError e) {
            page.td("version", "not-implemented");
        }
        page.endRow();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //               the pool queue info table(s)
    //
    /**
     *       convert the pool cost info (xgetcellinfo) into the
     *       int [] []   array.
     */
    private int [] []  decodePoolCostInfo(PoolCostInfo costInfo)
    {

        PoolCostInfo.PoolQueueInfo mover = costInfo.getMoverQueue();
        PoolCostInfo.PoolQueueInfo restore = costInfo.getRestoreQueue();
        PoolCostInfo.PoolQueueInfo store = costInfo.getStoreQueue();
        PoolCostInfo.PoolQueueInfo p2pServer = costInfo.getP2pQueue();
        PoolCostInfo.PoolQueueInfo p2pClient = costInfo.getP2pClientQueue();

        int[][] rows = new int[5][];

        rows[0] = new int[]{
                    mover.getActive(),
                    mover.getMaxActive(),
                    mover.getQueued()
                };
        rows[1] = new int[]{
                    restore.getActive(),
                    -1,
                    restore.getQueued()
                };
        rows[2] = new int[]{
                    store.getActive(),
                    -1,
                    store.getQueued()
                };

        if (p2pServer == null) {
            rows[3] = null;
        } else {
            rows[3] = new int[]{
                        p2pServer.getActive(),
                        p2pServer.getMaxActive(),
                        p2pServer.getQueued()
                    };
        }

        rows[4] = new int[]{
                    p2pClient.getActive(),
                    -1,
                    -1
                };

        return rows;
    }

    private double round(double value)
    {
        return Math.floor(value * 10) / 10.0;
    }

    private void printPoolInfoRow(PoolCellInfo cellInfo, HTMLBuilder page)
    {
        PoolCostInfo.PoolSpaceInfo info =
            cellInfo.getPoolCostInfo().getSpaceInfo();


        if (cellInfo.getErrorCode() == 0) {
            long total     = info.getTotalSpace();
            long freespace = info.getFreeSpace();
            long precious  = info.getPreciousSpace();
            long removable = info.getRemovableSpace();

            double red     = round(100 * precious / (float)total);
            double green   = round(100 * removable / (float)total);
            double yellow  = round(100 * freespace / (float)total);
            double magenta = Math.max(0, 100 - red - green - yellow);

            LOGGER.info(cellInfo.getCellName() + " : " +
                ";total=" + total + ";free=" + freespace +
                ";precious=" + precious + ";removable=" + removable);

            page.beginRow(null, "odd");
            page.td("cell",     cellInfo.getCellName());
            page.td("domain",   cellInfo.getDomainName());
            page.td("total",    BYTES.toMiB(total));
            page.td("free",     BYTES.toMiB(freespace));
            page.td("precious", BYTES.toMiB(precious));
            page.td("layout",
                    "<div>",
                    "<div class=\"layout_precious\" style=\"width: ", String.format(Locale.US, "%.1f", red), "%\"></div>",
                    "<div class=\"layout_sticky\" style=\"width: ", String.format(Locale.US, "%.1f", magenta), "%\"></div>",
                    "<div class=\"layout_cached\" style=\"width: ", String.format(Locale.US, "%.1f", green), "%\"></div>",
                    "<div class=\"layout_free\" style=\"width: ", String.format(Locale.US, "%.1f", yellow), "%\"></div>",
                    "</div>");
            page.endRow();
        } else {
            page.beginRow(null, "odd");
            page.td("cell",      cellInfo.getCellName());
            page.td("domain",    cellInfo.getDomainName());
            page.td("errorcode", "[", cellInfo.getErrorCode(), "]");
            page.td(3, "errormessage", cellInfo.getErrorMessage());
            page.endRow();
        }
    }

    private void printPoolActionRow(PoolCostEntry info,
            ActionHeaderExtension ext,
            HTMLBuilder page) {
        page.beginRow(null, "odd");
        page.td("cell", info._cellName);
        page.td("domain", info._domainName);

        for (int[] row : info._row) {
            if (row == null) {
                page.td(3, "integrated", "Integrated");
            } else {
                page.td("active", row[0]);
                if (row[1] >= 0) {
                    page.td("max", row[1]);
                } else {
                    page.td("max");
                }
                if (row[2] > 0) {
                    page.td("queued", row[2]);
                } else if (row[2] == 0) {
                    page.td("idle", 0);
                } else {
                    page.td("idle");
                }
            }
        }

        if (ext != null) {
            for (int[] row : ext.getSortedMovers(info._movers)) {
                page.td("active", row[0]);
                page.td("max", row[1]);
                if (row[2] > 0) {
                    page.td("queued", row[2]);
                } else if (row[2] == 0) {
                    page.td("idle", 0);
                } else {
                    page.td("idle");
                }
            }
        }
        page.endRow();
    }

    private void printOfflineCellInfoRow(String name, String domain,
                                         HTMLBuilder page)
    {
        page.beginRow(null, "odd");
        page.td("cell",   name);
        page.td("domain", domain);
        page.td(5, "offline", "OFFLINE");
        page.endRow();
    }

    private void printCellInfoTable(HTMLBuilder page)
    {
        page.beginTable("sortable",
                "cell", "CellName",
                "domain", "DomainName",
                "rp", "RP",
                "th", "TH",
                "ping", "Ping",
                "time", "Creation Time",
                "version", "Version");

        for (CellQueryInfo info : _infoMap.values()) {
            CellInfo cellInfo = info.getCellInfo();
            long pingTime = info.getPingTime();
            if (info.isOk()) {
                printCellInfoRow(cellInfo, pingTime, page);
            } else if (info.isPresent()) {
                printOfflineCellInfoRow(info.getName(),
                        (cellInfo == null ||
                         cellInfo.getDomainName().isEmpty())
                        ? "&lt;unknown&gt"
                        : cellInfo.getDomainName(),
                        page);
            }
        }
        page.endTable();
    }

    private synchronized void printPoolInfoTable(HTMLBuilder page)
    {
        page.beginTable("sortable",
                        "cell",     "CellName",
                        "domain",   "DomainName",
                        "total",    "Total Space/MiB",
                        "free",     "Free Space/MiB",
                        "precious", "Precious Space/MiB",
                        "layout",   "<span>Layout   " +
                          "(<span class=\"layout_precious\">precious/</span>" +
                          "<span class=\"layout_sticky\">sticky/</span>" +
                          "<span class=\"layout_cached\">cached/</span>" +
                          "<span class=\"layout_free\">free</span>)</span>");

        for (CellQueryInfo info : _infoMap.values()) {
            CellInfo cellInfo = info.getCellInfo();
            if (info.isOk() && (cellInfo instanceof PoolCellInfo)) {
                printPoolInfoRow((PoolCellInfo) cellInfo, page);
            }

        }
        page.endTable();
    }

    private static class PoolCostEntry
    {
        final String  _cellName;
        final String  _domainName;
        final int[][] _row;
        final Map<String,PoolCostInfo.NamedPoolQueueInfo> _movers;

        PoolCostEntry(String name, String domain, int[][] row, Map<String, PoolCostInfo.NamedPoolQueueInfo> movers)
        {
            _cellName   = name;
            _domainName = domain;
            _row        = row;
            _movers     = movers;
        }
    }

    private synchronized List<PoolCostEntry> preparePoolCostTable()
    {
        List<PoolCostEntry> list = new ArrayList<>();

        for (CellQueryInfo info : _infoMap.values()) {

            CellInfo cellInfo = info.getCellInfo();
            if (info.isOk() && (cellInfo instanceof PoolCellInfo)) {
                PoolCellInfo pci = (PoolCellInfo) cellInfo;
                int[][] status = decodePoolCostInfo(pci.getPoolCostInfo());

                if (status != null) {
                    list.add(new PoolCostEntry(pci.getCellName(),
                                               pci.getDomainName(),
                                               status,
                                               pci.getPoolCostInfo().getExtendedMoverHash()));
                }
            }
        }
        return list;
    }

    private synchronized void printPoolActionTable2(HTMLBuilder page)
    {
        // get the translated list
        LOGGER.debug("Preparing pool cost table");
        List<PoolCostEntry> list = preparePoolCostTable();
        LOGGER.debug("Preparing pool cost table done {}", list.size());
        // calculate the totals ...
        TreeMap<String, int[]> moverMap = new TreeMap<>();
        int[][] total = new int[5][3];

        for (PoolCostEntry e : list) {
            if (e._movers != null) {
                for (Map.Entry<String, PoolCostInfo.NamedPoolQueueInfo> entry : e._movers.entrySet()) {
                    String    queueName = entry.getKey();
                    int [] t = moverMap.get(queueName);
                    if (t == null) {
                        moverMap.put(queueName, t = new int[3]);
                    }
                    PoolCostInfo.PoolQueueInfo mover = entry.getValue();

                    t[0] += mover.getActive();
                    t[1] += mover.getMaxActive();
                    t[2] += mover.getQueued();
                }
            }
            int[][] status = e._row;
            for (int j = 0; j < total.length; j++) {
                for (int l = 0; l < total[j].length; l++) {
                    if (status[j] != null) {
                        total[j][l] += status[j][l];
                    }
                }
            }
        }

        ActionHeaderExtension extension = new ActionHeaderExtension(moverMap);

        page.beginTable(null);
        printPoolActionTableHeader(page, extension, HEADER_TOP);
        printPoolActionTableTotals(page, extension, total);

        int i = 0;
        for (PoolCostEntry e : list) {
            i++;
            printPoolActionRow(e, extension, page);
            if ((_repeatHeader != 0) && (i % _repeatHeader) == 0) {
                printPoolActionTableHeader(page, extension, HEADER_MIDDLE);
            }
        }
        printPoolActionTableTotals(page, extension, total);
        printPoolActionTableHeader(page, extension, HEADER_BOTTOM);
        page.endTable();

    }

    private void printPoolActionTableTotals(HTMLBuilder page,
                                            ActionHeaderExtension extension,
                                            int [] [] total)
    {
        page.beginRow("total");
        page.th(2, null, "Total");

        for (int[] row : total) {
            page.td("active", row[0]);
            if (row[1] >= 0) {
                page.td("max", row[1]);
            } else {
                page.td("max");
            }
            if (row[2] >= 0) {
                page.td("queued", row[2]);
            } else {
                page.td("queued");
            }
        }

        Map<String,int[]> map =
            extension == null ? null : extension.getTotals();
        if (map != null) {
            for (int[] row : map.values()) {
                page.td("active", row[0]);
                page.td("max", row[1]);
                page.td("queued", row[2]);
            }
        }
        page.endRow();
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////
    //
    //                Prepare the info tables in the context
    //
    private void preparePage()
    {
            HTMLBuilder page;
            // cell info tabel (request, threads, ping and creating time)
            page = new HTMLBuilder(_nucleus.getDomainContext());
            page.addHeader("/styles/cellInfo.css", "Services");
            printCellInfoTable(page);
            page.addFooter(getClass().getName());
            page.writeToContext("cellInfoTable.html");
            // disk usage page
            page = new HTMLBuilder(_nucleus.getDomainContext());
            page.addHeader("/styles/usageInfo.css", "Disk Space Usage");
            printPoolInfoTable(page);
            page.addFooter(getClass().getName());
            page.writeToContext("poolUsageTable.html");
            // pool queue page
            page = new HTMLBuilder(_nucleus.getDomainContext());
            page.addHeader("/styles/queueInfo.css", "Pool Request Queues");
            printPoolActionTable2(page);
            page.addFooter(getClass().getName());
            page.writeToContext("poolQueueTable.html");
    }

    @Override
    protected void stopping()
    {
        LOGGER.info("Clean Up sequence started");
        //
        // wait for the worker to be done
        //
        LOGGER.info("Waiting for collector thread to be finished");
        _collectThread.interrupt();
        _senderThread.interrupt();
        try {
            _collectThread.join();
            _senderThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stopped()
    {
        _nucleus.getDomainContext().remove("cellInfoTable.html");
        LOGGER.info("cellInfoTable.html removed from domain context");

        LOGGER.info("Clean Up sequence done");
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("        Version : $Id: WebCollectorV3.java,v 1.30 2007-10-29 14:19:08 behrmann Exp $");
        pw.println("Update Interval : "+_sleepHandler);
        pw.println("        Updates : "+_counter);
        pw.println("       Watching : "+_infoMap.size()+" cells");
    }
}
