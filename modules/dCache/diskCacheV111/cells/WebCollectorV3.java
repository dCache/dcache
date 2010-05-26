// $Id: WebCollectorV3.java,v 1.30 2007-10-29 14:19:08 behrmann Exp $Cg

package diskCacheV111.cells;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.HTMLBuilder;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.util.Args;

public class WebCollectorV3 extends CellAdapter implements Runnable
{
    protected static final String OPTION_REPEATHEADER = "repeatHeader";

    private final CellNucleus _nucleus;
    private final Args       _args;
    private final Map<String,CellQueryInfo> _infoMap
        = new TreeMap<String,CellQueryInfo>(); // TreeMap because we need it sorted
    private final Object     _infoLock  = new Object();
    private Thread     _collectThread;
    private Thread     _senderThread;
    private long       _counter         = 0;
    private boolean    _debug           = false;
    private int        _repeatHeader    = 30;
    private String[]   _loginBrokerTable;

    private class SleepHandler
    {
        private boolean _enabled         = true;
        private boolean _mode            = true;
        private long    _started         = 0L;
        private long    _shortPeriod     = 20000L;
        private long    _regularPeriod   = 120000L;
        private long    _retentionFactor = 4;

        private SleepHandler(boolean aggressive)
        {
            _enabled = aggressive;
            _mode    = aggressive;
        }

        private synchronized void sleep() throws InterruptedException
        {
            long start = System.currentTimeMillis();
            wait(_mode ? _shortPeriod / 2 : _regularPeriod / 2);
            dsay("Woke up after " + (System.currentTimeMillis() - start)
                 + " millis");
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
            // say("Topology changed : "+modified);
            if (!_enabled) return;
            if (modified) {
                _started = System.currentTimeMillis();
                if (!_mode) {
                    _mode = true;
                    notifyAll();
                    say("Aggressive changed to ON");
                }

            } else if (_mode &&
                      (System.currentTimeMillis() - _started) >
                       (_shortPeriod * _retentionFactor)) {
                _mode = false;
                notifyAll();
                say("Aggressive changed to OFF");
            }

        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("E=").append(_enabled).
                append(";A=").append(_mode).
                append(";S=").append(_shortPeriod).
                append(";Ret").append(_retentionFactor).
                append(";R=").append(_regularPeriod);
            return sb.toString();
        }
    };

    private SleepHandler     _sleepHandler = null;
    private SimpleDateFormat _formatter    = new SimpleDateFormat ("MM/dd HH:mm:ss");

    private class CellQueryInfo
    {
        private final String      _destination;
        private       long        _diff        = -1;
        private       long        _start       = 0;
        private       CellInfo    _info;
        private final CellMessage _message;
        private       long        _lastMessage = 0;
        private       boolean     _present;

        private CellQueryInfo(String destination)
        {
            _destination = destination;
            _message = new CellMessage(new CellPath(_destination), "xgetcellinfo");
        }

        private String      getName()
        {
            return _destination;
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
            return _message;
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

    public WebCollectorV3(String name, String args) throws Exception
    {
        super(name, WebCollectorV3.class.getName(), args, false);
        _args    = getArgs();
        _nucleus = getNucleus();
        try {
            _debug = _args.getOpt("debug") != null;

            if( _args.getOpt( OPTION_REPEATHEADER) != null) {
                String optionString = null;
                try {
                    optionString  = _args.getOpt( OPTION_REPEATHEADER);
                    _repeatHeader = Math.max(0, Integer.parseInt(optionString));
                } catch (NumberFormatException e) {
                    esay("Parsing error in " + OPTION_REPEATHEADER + " command : " + optionString);
                }
                say("Repeat header set to "+_repeatHeader);
            }

            String optionString = _args.getOpt("aggressive");
            boolean aggressive =
                (optionString != null) &&
                (optionString.equals("off") || optionString.equals("false"));

            aggressive = !aggressive;

            say("Agressive mode : "+aggressive);

            _sleepHandler = new SleepHandler(aggressive);

            for (int i = 0; i < _args.argc(); i++)addQuery(_args.argv(i));

            String loginBrokers = _args.getOpt("loginBroker");
            if ((loginBrokers != null) && (loginBrokers.length() > 0)) {
                _loginBrokerTable = loginBrokers.split(",");
                for (String cellName : _loginBrokerTable) {
                    say("Login Broker : " + cellName);
                    addQuery(cellName);
                }
            }
            (_senderThread  = _nucleus.newThread(this, "sender")).start();
            say("Sender started");
            say("Collector will be started a bit delayed");
            _nucleus.newThread(new Runnable() {
                                   public void run() {
                                       try {
                                           Thread.sleep(30000L);
                                       } catch (InterruptedException e) {
                                           return;
                                       }
                                       _collectThread = _nucleus.newThread(WebCollectorV3.this, "collector");
                                       _collectThread.start();
                                       say("Collector now started as well");
                                   }
                               },
                               "init"
                              ).start();


        } catch (Exception e) {
            esay("<init> of WebCollector reports : " + e.getMessage());
            esay(e);
            start();
            kill();
            throw e;
        }
        start();
    }

    public void dsay(String message)
    {
        if (_debug)
            say(message);
    }

    private synchronized boolean addQuery(String destination)
    {
        if (_infoMap.get(destination) != null)
            return false;
        say("!!!Adding "+destination);
        _infoMap.put(destination, new CellQueryInfo(destination));
        return true;
    }

    private synchronized void removeQuery(String destination)
    {
        say("Removing "+destination);
        _infoMap.remove(destination);
    }

    public void run()
    {
        Thread x = Thread.currentThread();
        if (x == _senderThread)
            runSender();
        else
            runCollector();
    }

    private void runCollector()
    {
        try {
            while (!Thread.interrupted()) {
                synchronized (_infoLock) {
                    preparePage();
                }
                _sleepHandler.sleep();
            }
        } catch (InterruptedException e) {
            say("Collector Thread interrupted");
        }
    }

    private void runSender()
    {
        //CellMessage loginBrokerMessage = new CellMessage(new CellPath

        try {
            while (!Thread.interrupted()) {
                _counter++;
                if (_loginBrokerTable != null) {
                    for (String broker : _loginBrokerTable) {
                        try {
                            CellPath path = new CellPath(broker);
                            dsay("Sending LoginBroker query to : " + path);
                            sendMessage(new CellMessage(path, "ls -binary"));
                        } catch (Exception ee) {

                        }
                    }
                }
                //sendMessage(loginBrokerMessage);
                synchronized (_infoLock) {
                    for (CellQueryInfo info : _infoMap.values()) {
                        try {
                            dsay("Sending query to : " + info.getName());
                            sendMessage(info.getCellMessage());
                        } catch (Exception e) {

                        }
                    }
                }
                _sleepHandler.sleep();
            }
        } catch (InterruptedException iie) {
            say("Sender Thread interrupted");
        }
    }

    @Override
    public void messageArrived(CellMessage message)
    {
        CellPath path = message.getSourcePath();
        String destination = path.getCellName();
        CellQueryInfo info = _infoMap.get(destination);
        if (info == null) {
            dsay("Unexpected reply arrived from : "+path);
            return;
        }
        Object reply = message.getMessageObject();

        int modified = 0;

        if (reply instanceof CellInfo) {
            dsay("CellInfo : "+((CellInfo)reply).getCellName());
            info.infoArrived((CellInfo)reply);
        }
        if (reply instanceof diskCacheV111.poolManager.PoolManagerCellInfo) {
            String[] poolList = ((PoolManagerCellInfo)reply).getPoolList();
            synchronized (_infoLock) {
                for (String pool : poolList)
                    if (addQuery(pool))
                        modified++;
            }
        }

        if (reply instanceof dmg.cells.services.login.LoginBrokerInfo[]) {
            dsay("Login broker reply : "+((dmg.cells.services.login.LoginBrokerInfo[])reply).length);
            LoginBrokerInfo [] brokerInfos = (LoginBrokerInfo [])reply;
            synchronized (_infoLock) {
                for (LoginBrokerInfo brokerInfo : brokerInfos) {
                    String dest = brokerInfo.getCellName();
                    dsay("Login broker reports : " + dest);
                    if (addQuery(dest))
                        modified++;
                }
            }
        }
        _sleepHandler.topologyChanged(modified > 0);
    }

    public String hh_set_repeat_header = "<repeatHeaderCount>|0";
    public synchronized String ac_set_repeat_header_$_1(Args args)
    {
        _repeatHeader = Integer.parseInt(args.argv(0));
        return "";
    }

    private Map<String,Map> _poolGroup = new HashMap<String,Map>();
    public String hh_define_poolgroup = "<poolgroup> [poolName | /regExpr/ ] ... ";
    public String ac_define_poolgroup_$_1_99(Args args)
    {
        String poolGroupName = args.argv(0);
        synchronized (_poolGroup) {
            Map map = _poolGroup.get( poolGroupName);
            if (map == null)_poolGroup.put(poolGroupName, map = new HashMap());

            for (int i = 0, n = args.argc() - 1; i < n; i++) {
                String poolName = args.argv(i);
            }
        }
        return "";
    }

    public String hh_watch = "<CellAddress> [...]";
    public String ac_watch_$_1_99(Args args)
    {
        for (int i = 0; i < args.argc(); i++)addQuery(args.argv(i));
        return "";
    }

    public String hh_dump_info = "[minPingTime] # dumps all info about watched cells";
    public String ac_dump_info_$_0_1(Args args)
    {
        long     minPingTime = 0;
        StringBuilder buf = new StringBuilder();
        if (args.argc() > 0) {
            minPingTime  = Long.parseLong(args.argv(0));
        }

        for (CellQueryInfo info : _infoMap.values()) {
            try {
                CellInfo cellInfo = info.getCellInfo();
                long     pingTime = info.getPingTime();
                if (pingTime > minPingTime) {
                    if (info.isOk()) {
                        buf.append("" + cellInfo.getDomainName() + " " + cellInfo + " " + pingTime + "\n");
                    } else if (info.isPresent()) {
                        buf.append("" + cellInfo.getDomainName() + " " + cellInfo + "\n");
                    }
                }
            } catch (Exception e) {
                esay(e);
            }

        }
        return buf.toString();
    }

    public String hh_unwatch = "<CellAddress> [...]";
    public String ac_unwatch_$_1_99(Args args)
    {
        for (int i = 0; i < args.argc(); i++)
            removeQuery(args.argv(i));
        return "";
    }

    public String hh_set_interval = "[<pingInteval/sec>] [-short=<aggressiveInterval>]";
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

    private class ActionHeaderExtension
    {
        private TreeMap<String,int[]> _map;  // TreeMap because we need it sorted
        private ActionHeaderExtension(TreeMap<String,int[]> map)
        {
            _map = map == null ? new TreeMap<String,int[]>() : map;
        }

        @Override
        public String toString()
        {
            return _map.toString();
        }

        int [] [] getSortedMovers(Map<String,PoolCostInfo.PoolQueueInfo> moverMap)
        {
            int[][] rows = new int[_map.size()][];
            if (moverMap == null) {
                for (int i = 0; i < _map.size(); i++)
                    rows[i] = new int[] { -1, -1, -1 };
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

        try {

            PoolCostInfo.PoolQueueInfo mover     = costInfo.getMoverQueue();
            PoolCostInfo.PoolQueueInfo restore   = costInfo.getRestoreQueue();
            PoolCostInfo.PoolQueueInfo store     = costInfo.getStoreQueue();
            PoolCostInfo.PoolQueueInfo p2pServer = costInfo.getP2pQueue();
            PoolCostInfo.PoolQueueInfo p2pClient = costInfo.getP2pClientQueue();

            int[][] rows = new int[5][];

            rows[0] = new int[] {
                    mover.getActive(),
                    mover.getMaxActive(),
                    mover.getQueued()
            };
            rows[1] = new int[] {
                restore.getActive(),
                restore.getMaxActive(),
                restore.getQueued()
            };
            rows[2] = new int[] {
                store.getActive(),
                store.getMaxActive(),
                store.getQueued()
            };

            if (p2pServer == null) {
                rows[3] = null;
            } else {
                rows[3] = new int[] {
                    p2pServer.getActive(),
                    p2pServer.getMaxActive(),
                    p2pServer.getQueued()
                };
            }

            rows[4] = new int[] {
                p2pClient.getActive(),
                p2pClient.getMaxActive(),
                p2pClient.getQueued()
            };

            return rows;
        } catch (Exception e) {
            esay(e);
            return null;
        }
    }

    private double round(double value)
    {
        return Math.floor(value * 10) / 10.0;
    }

    private void printPoolInfoRow(PoolCellInfo cellInfo, HTMLBuilder page)
    {
        long mb     = 1024 * 1024;

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
            double blue    = Math.max(0, 100 - red - green - yellow);

            say(cellInfo.getCellName() + " : " +
                ";total=" + total + ";free=" + freespace +
                ";precious=" + precious + ";removable=" + removable);

            page.beginRow(null, "odd");
            page.td("cell",     cellInfo.getCellName());
            page.td("domain",   cellInfo.getDomainName());
            page.td("total",    total / mb);
            page.td("free",     freespace / mb);
            page.td("precious", precious / mb);
            page.td("layout",
                    "<div>",
                    "<div class=\"layout_precious\" style=\"width: ", String.format("%.1f", red), "%\"></div>",
                    "<div class=\"layout_rest\" style=\"width: ", String.format("%.1f", blue), "%\"></div>",
                    "<div class=\"layout_used\" style=\"width: ", String.format("%.1f", green), "%\"></div>",
                    "<div class=\"layout_free\" style=\"width: ", String.format("%.1f", yellow), "%\"></div>",
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
                                    HTMLBuilder page)
    {
        try {
            page.beginRow(null, "odd");
            page.td("cell",   info._cellName);
            page.td("domain", info._domainName);

            for (int[] row : info._row) {
                if (row == null) {
                    page.td(3, "integrated", "Integrated");
                } else {
                    page.td("active", row[0]);
                    page.td("max",    row[1]);
                    if (row[2] > 0) {
                        page.td("queued", row[2]);
                    } else {
                        page.td("idle", 0);
                    }
                }
            }

            if (ext != null) {
                for (int[] row : ext.getSortedMovers(info._movers)) {
                    page.td("active", row[0]);
                    page.td("max", row[1]);
                    if (row[2] > 0) {
                        page.td("queued", row[2]);
                    } else {
                        page.td("idle", 0);
                    }
                }
            }
            page.endRow();
        } catch (Exception e) {
            esay(e);
        }
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
                        "cell",    "CellName",
                        "domain",  "DomainName",
                        "rp",      "RP",
                        "th",      "TH",
                        "ping",    "Ping",
                        "time",    "Creation Time",
                        "version", "Version");

        for (CellQueryInfo info : _infoMap.values()) {
            try {
                CellInfo cellInfo = info.getCellInfo();
                long     pingTime = info.getPingTime();
                if (info.isOk()) {
                    printCellInfoRow(cellInfo, pingTime, page);
                } else if( info.isPresent()) {
                    printOfflineCellInfoRow(info.getName(),
                                            (cellInfo == null ||
                                             cellInfo.getDomainName().length() == 0)
                                            ? "&lt;unknown&gt"
                                            : cellInfo.getDomainName(),
                                            page);
                }
            } catch(Exception e) {
                esay(e);
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
                          "<span class=\"layout_used\">used/</span>" +
                          "<span class=\"layout_free\">free</span>)</span>");

        for (Object i : _infoMap.values()) {
            try {
                CellQueryInfo info = (CellQueryInfo)i;
                CellInfo cellInfo  = info.getCellInfo();
                if (info.isOk() && (cellInfo instanceof PoolCellInfo)) {
                    printPoolInfoRow((PoolCellInfo)cellInfo, page);
                }
            } catch (Exception ee) {
                esay(ee);
            }
        }
        page.endTable();
    }

    private class PoolCostEntry
    {
        String  _cellName;
        String  _domainName;
        int[][] _row;
        Map     _movers;

        PoolCostEntry(String name, String domain, int[][] row)
        {
            _cellName   = name;
            _domainName = domain;
            _row        = row;
            _movers     = null;
        }

        PoolCostEntry(String name, String domain, int[][] row, Map movers)
        {
            _cellName   = name;
            _domainName = domain;
            _row        = row;
            _movers     = movers;
        }
    }

    private synchronized List<PoolCostEntry> preparePoolCostTable()
    {
        List<PoolCostEntry> list = new ArrayList<PoolCostEntry>();

        int[][] total = new int[5][3];

        for (CellQueryInfo info : _infoMap.values()) {
            try {
                CellInfo cellInfo = info.getCellInfo();
                if (info.isOk() && (cellInfo instanceof PoolCellInfo)) {
                    PoolCellInfo pci = (PoolCellInfo)cellInfo;
                    int [] [] status = decodePoolCostInfo(pci.getPoolCostInfo());

                    if (status != null)
                        list.add(new PoolCostEntry(pci.getCellName(),
                                                   pci.getDomainName(),
                                                   status,
                                                   pci.getPoolCostInfo().getExtendedMoverHash()));
                }
            } catch (Exception e) {
                esay(e);
            }
        }
        return list;
    }

    private synchronized void printPoolActionTable2(HTMLBuilder page)
    {
        //
        // get the translated list
        //
        dsay("Preparing pool cost table");
        List<PoolCostEntry> list = preparePoolCostTable();
        dsay("Preparing pool cost table done " + list.size());
        //
        // calculate the totals ...
        //
        TreeMap<String, int[]> moverMap = new TreeMap<String,int[]>();
        int[][] total = new int[5][3];

        for (PoolCostEntry e : list) {
            if (e._movers != null) {
                for (Map.Entry entry : (Set<Map.Entry>)e._movers.entrySet()) {
                    String    queueName = (String)entry.getKey();
                    int [] t = moverMap.get(queueName);
                    if (t == null)moverMap.put(queueName, t = new int[3]);
                    PoolCostInfo.PoolQueueInfo mover =
                        (PoolCostInfo.PoolQueueInfo)entry.getValue();

                    t[0] += mover.getActive();
                    t[1] += mover.getMaxActive();
                    t[2] += mover.getQueued();
                }
            }
            int[][] status = e._row;
            for (int j = 0; j < total.length; j++) {
                for (int l = 0; l < total[j].length; l++) {
                    if (status[j] != null)
                        total[j][l] += status[j][l];
                }
            }
        }

        ActionHeaderExtension extension = new ActionHeaderExtension(moverMap);
        //say("EXTENTION : "+extension);

        page.beginTable(null);
        printPoolActionTableHeader(page, extension, HEADER_TOP);
        printPoolActionTableTotals(page, extension, total);

        int i = 0;
        for (PoolCostEntry e : list) {
            i++;
            printPoolActionRow(e, extension, page);
            if ((_repeatHeader != 0) && (i % _repeatHeader) == 0)
                printPoolActionTableHeader(page, extension, HEADER_MIDDLE);
        }
        printPoolActionTableTotals(page, extension, total);
        printPoolActionTableHeader(page, extension, HEADER_BOTTOM);
        page.endTable();

        //say("Creating pool cost table ready");
    }

    private void printPoolActionTableTotals(HTMLBuilder page,
                                            ActionHeaderExtension extension,
                                            int [] [] total)
    {
        page.beginRow("total");
        page.th(2, null, "Total");

        for (int[] row : total) {
            page.td("active", row[0]);
            page.td("max", row[1]);
            page.td("queued", row[2]);
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
        try {
            HTMLBuilder page;

            //
            // cell info tabel (request, threads, ping and creating time)
            //
            page = new HTMLBuilder(_nucleus.getDomainContext());
            page.addHeader("/styles/cellInfo.css", "Services");
            printCellInfoTable(page);
            page.addFooter(getClass().getName() + " [$Revision: 1.30 $]");
            page.writeToContext("cellInfoTable.html");

            //
            // disk usage page
            //
            page = new HTMLBuilder(_nucleus.getDomainContext());
            page.addHeader("/styles/usageInfo.css", "Disk Space Usage");
            printPoolInfoTable(page);
            page.addFooter(getClass().getName() + " [$Revision: 1.30 $]");
            page.writeToContext("poolUsageTable.html");

            //
            // pool queue page
            //
            page = new HTMLBuilder(_nucleus.getDomainContext());
            page.addHeader("/styles/queueInfo.css", "Pool Request Queues");
            printPoolActionTable2(page);
            page.addFooter(getClass().getName() + " [$Revision: 1.30 $]");
            page.writeToContext("poolQueueTable.html");

        } catch (Exception e) {
            esay(e);
            esay("Collector reported : " + e.getMessage());
        }
    }

    @Override
    public void cleanUp()
    {
        say("Clean Up sequence started");
        //
        // wait for the worker to be done
        //
        say("Waiting for collector thread to be finished");
        _collectThread.interrupt();
        _senderThread.interrupt();
        _nucleus.getDomainContext().remove("cellInfoTable.html");

        say("cellInfoTable.html removed from domain context");

        say("Clean Up sequence done");

    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("        Version : $Id: WebCollectorV3.java,v 1.30 2007-10-29 14:19:08 behrmann Exp $");
        pw.println("Update Interval : "+_sleepHandler);
        pw.println("        Updates : "+_counter);
        pw.println("       Watching : "+_infoMap.size()+" cells");
        pw.println("     Debug Mode : "+(_debug?"ON":"OFF"));
    }
}
