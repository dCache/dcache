package diskCacheV111.cells;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.HTMLBuilder;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginManagerChildrenInfo;

import org.dcache.cells.CellStub;
import org.dcache.util.Args;
import org.dcache.util.NetworkUtils;
import org.dcache.util.TransferCollector;
import org.dcache.util.TransferCollector.Transfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class TransferObserverV1
    extends CellAdapter
    implements Runnable
{
    private final Logger _log =
        LoggerFactory.getLogger(TransferObserverV1.class);

    private final CellNucleus   _nucleus;
    private final CellStub      _cellStub;
    private final Args          _args;
    private final TransferCollector _collector;
    private final String        _loginBroker;
    private final Thread        _workerThread;
    private       long          _update         = 120000L;
    private       long          _timeUsed;
    private       long          _processCounter;
    private final FieldMap      _fieldMap;
    private final Map<String,TableEntry> _tableHash
        = new HashMap<>();


    private static String[] __className =
    {
        "cell",               //   0
        "domain",             //   1
        "sequence",           //   2
        "protocol",           //   3
        "owner",              //   4
        "process",            //   5
        "pnfs",               //   6
        "pool",               //   7
        "host",               //   8
        "status",             //   9
        "wait",               //  10
        "state",              //  11
        "submitted",          //  12
        "time",               //  13
        "transferred",        //  14
        "speed",              //  15
        "started"             //  16
    };

    private static String[] __listHeader =
    {
        "Cell",               //   0
        "Domain",             //   1
        "Seq",                //   2
        "Prot",               //   3
        "Owner",              //   4
        "Proc",               //   5
        "PnfsId",             //   6
        "Pool",               //   7
        "Host",               //   8
        "State",              //   9
        "Since",              //  10
        "JobState",           //  11
        "Submitted",          //  12
        "Time",               //  13
        "Trans.&nbsp;(KB)",   //  14
        "Speed&nbsp;(KB/s)",  //  15
        "Started"             //  16
    };

    private class FieldMap
    {
        private final Class<?>[] _conArgsClass = { Args.class };
        private Class<?>       _mapClass;
        private Constructor<?> _constructor;
        private Object      _master;
        private Method      _mapOwner;

        private FieldMap(String className , Args args)
        {
            if (className == null) {
                _log.info("FieldMap : 'fieldMap' not defined");
                return;
            }

            Object[] conArgs   = { args } ;
            Class<?> [] classArgs = { String.class } ;
            try {
                _mapClass     = Class.forName(className) ;
                _constructor  = _mapClass.getConstructor(_conArgsClass) ;
                _master       = _constructor.newInstance(conArgs);
                _mapOwner     = _mapClass.getMethod("mapOwner", classArgs ) ;
                //
                // only if this is ok, we can load the commandlistener ...
                //
                addCommandListener( _master ) ;
                //
                _log.info("FieldMap : " + _mapClass.getName() + " loaded");
            } catch (Exception ee) {
                _log.warn("FieldMap : Creating map class Failed : " + ee);
            }
        }

        private String mapOwner(String owner)
        {
            Object [] args = { owner };
            if (_mapOwner == null) {
                return owner;
            }

            try {
                return (String)_mapOwner.invoke(_master, args);
            } catch (Exception ee) {
                _log.warn("Problem invoking 'mapOwner' : " + ee);
                return owner;
            }
        }
    }

    public TransferObserverV1(String name, String  args) throws Exception
    {
        super(name, TransferObserverV1.class.getName(), args);

        _nucleus = getNucleus();
        _cellStub = new CellStub(this, null, 30, SECONDS);
        _args    = getArgs();
        _loginBroker = _args.getOpt("loginBroker");
        _collector = new TransferCollector(_cellStub,
                                           Arrays.stream(_loginBroker.split(",")).map(CellPath::new).collect(toList()));

        try {
            if (_args.argc() < 0) {
                throw new IllegalArgumentException("Usage : ... ");
            }

            String updateString = _args.getOpt("update");
            try {
                if (updateString != null) {
                    _update = Long.parseLong(updateString) * 1000L;
                }
            } catch (NumberFormatException e) {
                _log.warn("Illegal value for -update: " + updateString);
            }

            //
            // if login broker is defined, the
            // worker will add the 'fixed' door list to the
            // list provided by the loginBroker.
            //
            //
            _fieldMap = new FieldMap(_args.getOpt("fieldMap"), _args);
            //
            _workerThread = _nucleus.newThread(this, "worker");
            _workerThread.start();
            //
        } catch (Exception e) {
            start();
            kill();
            throw e;
        }
        useInterpreter(true);
        start();
    }

    private static class TableEntry
    {
        private String  _tableName;
        private int []  _fields;
        private String  _title;
        private long    _olderThan;
        private boolean _ifNotYetStarted;
        private boolean _ifMoverMissing;

        private TableEntry(String tableName, int [] fields)
        {
            _tableName = tableName;
            _fields    = fields;
        }

        private TableEntry(String tableName, int [] fields, String title)
        {
            _tableName = tableName;
            _fields    = fields;
            _title     = title;
        }

        private int [] getFields()
        {
            return _fields;
        }

        private String getName()
        {
            return _tableName;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(_tableName).append(" = ");
            if (_fields.length > 0) {
                for (int i = 0; i < (_fields.length - 1); i++) {
                    sb.append(_fields[i]).append(",");
                }
                sb.append(_fields[_fields.length - 1]);
            }
            if (_title != null) {
                sb.append("  \"").append(_title).append("\"");
            }
            return sb.toString();
        }

        private boolean ifNotYetStarted()
        {
            return _ifNotYetStarted;
        }

        private boolean ifMoverMissing()
        {
            return _ifMoverMissing;
        }

        private long getOlderThan()
        {
            return _olderThan;
        }
    }

    @Override
    public void cleanUp()
    {
        if (_workerThread != null) {
            _workerThread.interrupt();
        }
        super.cleanUp();
    }

    public static final String hh_table_help = "";
    public String ac_table_help(Args args)
    {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String header : __listHeader) {
            sb.append(i++).append(" ").append(header).append("\n");
        }
        return sb.toString();
    }

    public static final String hh_table_define = "<tableName> <n>[,<m>[,...]] [<tableHeader>]" ;
    public synchronized String ac_table_define_$_2_3(Args args)
        throws NumberFormatException
    {
        String     tableName = args.argv(0);
        String        header = args.argc() > 2 ? args.argv(2) : null;
        String[]        list = args.argv(1).split(",");

        int[] array = new int[list.length];
        for (int i = 0; i < list.length; i++) {
            array[i] = Integer.valueOf(list[i]);
        }

        _tableHash.put(tableName, new TableEntry(tableName, array, header));

        return "";
    }

    public static final String hh_table_undefine = "<tableName>";
    public synchronized String ac_table_undefine_$_1(Args args)
    {
        String tableName = args.argv(0);
        _tableHash.remove(tableName);
        _nucleus.getDomainContext().remove(tableName + ".html");
        return "";
    }

    public static final String hh_table_ls = "[<tableName>]";
    public synchronized String ac_table_ls_$_0_1(Args args)
    {
        StringBuilder sb = new StringBuilder();
        if (args.argc() == 0) {
            for (TableEntry entry : _tableHash.values()) {
                sb.append(entry.toString()).append("\n");
            }
        } else {
            String tableName = args.argv(0);
            TableEntry entry = _tableHash.get(tableName);
            if (entry == null) {
                throw new NoSuchElementException("Not found : " + tableName);
            }
            sb.append(entry.toString()).append("\n");
        }
        return sb.toString();
    }

    public static final String hh_set_update = "<updateTime/sec>";
    public String ac_set_update_$_1(Args args)
    {
        long update = Long.parseLong(args.argv(0)) * 1000L;
        if (update < 10000L) {
            throw new
                    IllegalArgumentException("Update time must exceed 10 seconds");
        }

        synchronized (this) {
            _update = update;
            notifyAll();
        }
        return "Update time set to " + args.argv(0) + " seconds";
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("    Update Time : " + (_update / 1000L) + " seconds");
        pw.println("        Counter : " + _processCounter);
        pw.println(" Last Time Used : " + _timeUsed + " milliseconds");
    }

    @Override
    public void run()
    {
        try {
            while (!Thread.interrupted()) {
                try {
                    _processCounter++;
                    long start = System.currentTimeMillis();
                    collectDataSequentially();
                    _timeUsed = System.currentTimeMillis() - start;
                } catch (RuntimeException ee) {
                    _log.error(ee.toString(), ee);
                }

                synchronized (this) {
                    wait(_update);
                }
            }
        } catch (InterruptedException e) {
            _log.info("Data collector interrupted");
        }
    }

    public String ac_go(Args args )
    {
        synchronized (this) {
            notifyAll();
        }
        return "Update started.";
    }

    private void collectDataSequentially() throws InterruptedException
    {
        try {
            Collection<LoginBrokerInfo> loginBrokerInfos =
                    _collector.collectLoginBrokerInfo().get();
            Collection<LoginManagerChildrenInfo> loginManagerInfos =
                    _collector.collectLoginManagerInfo(TransferCollector.getLoginManagers(loginBrokerInfos)).get();
            Collection<IoDoorInfo> doorInfos =
                    _collector.collectDoorInfo(TransferCollector.getDoors(loginManagerInfos)).get();
            Collection<IoJobInfo> movers =
                    _collector.collectMovers(TransferCollector.getPools(doorInfos)).get();

            List<Transfer> transfers = TransferCollector.getTransfers(doorInfos, movers);
            transfers.sort(new TransferCollector.ByDoorAndSequence());

            Map<String, Object> domainContext = _nucleus.getDomainContext();
            domainContext.put("doors.html", createDoorPage(loginBrokerInfos));
            domainContext.put("transfers.list", transfers);
            domainContext.put("transfers.html", createHtmlTable(transfers));
            domainContext.put("transfers.txt", createAsciiTable(transfers));

            synchronized (this) {
                for (TableEntry entry : _tableHash.values()) {
                    domainContext.put(entry.getName() + ".html", createDynamicTable(transfers, entry.getFields()));
                }
            }
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    //
    // the html stuff.
    //

    private String createDoorPage(Collection<LoginBrokerInfo> doors)
    {
        HTMLBuilder page = new HTMLBuilder(_nucleus.getDomainContext());

        page.addHeader("/styles/doors.css", "Doors");
        page.beginTable("sortable",
                        "cell",     "Cell",
                        "domain",   "Domain",
                        "protocol", "Protocol",
                        "version",  "Version",
                        "host",     "Host",
                        "port",     "Port",
                        "load",     "Load");

        for (LoginBrokerInfo door : doors) {
            InetAddress address =
                    door.getAddresses().stream().max(Comparator.comparing(NetworkUtils.InetAddressScope::of)).get();
            page.beginRow(null, "odd");
            page.td("cell", door.getCellName());
            page.td("domain",   door.getDomainName());
            page.td("protocol", door.getProtocolFamily());
            page.td("version",  door.getProtocolVersion());
            page.td("host", address.getHostName());
            page.td("port",     door.getPort());
            page.td("load", (int) (door.getLoad() * 100.0));
            page.endRow();
        }
        page.endTable();

        return page.toString();
    }

    private String createDynamicTable(List<Transfer> transfers, int [] fields)
    {
        HTMLBuilder page = new HTMLBuilder(_nucleus.getDomainContext());

        page.addHeader("/styles/transfers.css", "Active Transfers");

        page.beginTable("sortable");
        page.beginTHead();
        for (int field : fields) {
            page.th(__className[field], __listHeader[field]);
        }
        page.endTHead();

        for (Transfer transfer : transfers) {
            List<String> values = createFieldList(transfer);
            for (int field : fields) {
                if (field >= values.size()) {
                    page.td(__className[field], "");
                } else {
                    page.td(__className[field], values.get(field));
                }
            }
        }
        page.endTable();
        page.addFooter(getClass().getName());
        return page.toString();
    }

    public static final String hh_ls_iolist = "";
    public synchronized String ac_ls_iolist(Args args)
    {
        return Objects.toString(_nucleus.getDomainContext().get("transfers.txt"), "");
    }

    private String createAsciiTable(List<Transfer> transfers)
    {
        long          now = System.currentTimeMillis();
        StringBuilder sb  = new StringBuilder();

        for (Transfer transfer : transfers) {
            List<String> args = new ArrayList<>();
            args.add(transfer.door().getCellName());
            args.add(transfer.door().getDomainName());
            args.add(String.valueOf(transfer.session().getSerialId()));
            args.add(transfer.door().getProtocolFamily() + "-" + transfer.door().getProtocolVersion());
            args.add(transfer.door().getOwner());
            args.add(transfer.door().getProcess());
            args.add(Objects.toString(transfer.session().getPnfsId(), ""));
            args.add(Objects.toString(transfer.session().getPool(), ""));
            args.add(transfer.session().getReplyHost());
            args.add(Objects.toString(transfer.session().getStatus(), ""));
            args.add(String.valueOf(now - transfer.session().getWaitingSince()));

            IoJobInfo mover = transfer.mover();
            if (mover == null) {
                args.add("No-mover()-Found");
            } else {
                args.add(mover.getStatus());
                if (mover.getStartTime() > 0L) {
                    long transferTime     = mover.getTransferTime();
                    long bytesTransferred = mover.getBytesTransferred();
                    args.add(String.valueOf(transferTime));
                    args.add(String.valueOf(bytesTransferred));
                    args.add(String.valueOf(transferTime > 0 ? ((double) bytesTransferred / (double) transferTime) : 0));
                    args.add(String.valueOf(now - mover.getStartTime()));
                }
            }
            sb.append(new Args(args)).append('\n');
        }
        return sb.toString();
    }

    private List<String> createFieldList(Transfer transfer)
    {
        long         now = System.currentTimeMillis();
        List<String> out = new ArrayList<>(20);

        PnfsId pnfsid = transfer.session().getPnfsId();
        String status = transfer.session().getStatus();
        out.add(transfer.door().getCellName());
        out.add(transfer.door().getDomainName());
        out.add(String.valueOf(transfer.session().getSerialId()));
        out.add(transfer.door().getProtocolFamily()+"-"+
                transfer.door().getProtocolVersion());
        out.add(_fieldMap.mapOwner(transfer.door().getOwner()));
        out.add(transfer.door().getProcess());
        out.add(pnfsid == null ? "" : pnfsid.toString());
        out.add(transfer.session().getPool());
        out.add(transfer.session().getReplyHost());
        out.add(status == null ? "" : status.replace(" ", "&nbsp;"));
        out.add(getTimeString(now - transfer.session().getWaitingSince()));

        if (transfer.mover() != null) {
            out.add(transfer.mover().getStatus());
            out.add(getTimeString(now - transfer.mover().getSubmitTime()));
            if (transfer.mover().getStartTime() > 0L) {
                long transferTime     = transfer.mover().getTransferTime();
                long bytesTransferred = transfer.mover().getBytesTransferred();
                out.add(getTimeString(transferTime));
                out.add(Long.toString(bytesTransferred / 1024));
                out.add(transferTime > 0 ?
                        String.valueOf((1000 * bytesTransferred) / (1024 * transferTime)) :
                        "-");
                out.add(getTimeString(now - transfer.mover().getStartTime()));
            }
        }
        return out;
    }

    private String getTimeString(long msec)
    {
        int sec  =  (int) ( msec / 1000L );
        int min  =  sec / 60; sec  = sec  % 60;
        int hour =  min / 60; min  = min  % 60;
        int day  = hour / 24; hour = hour % 24;

        String sS = Integer.toString(sec);
        String mS = Integer.toString(min);
        String hS = Integer.toString(hour);

        StringBuilder sb = new StringBuilder();
        if (day > 0) {
            sb.append(day).append(" d ");
        }
        sb.append(hS.length() < 2 ? ("0" + hS) : hS).append(":");
        sb.append(mS.length() < 2 ? ("0" + mS) : mS).append(":");
        sb.append(sS.length() < 2 ? ("0" + sS) : sS);

        return sb.toString();
    }

    private void createHtmlTableRow(HTMLBuilder page, Transfer transfer)
    {
        long now = System.currentTimeMillis();

        page.beginRow(null, "odd");
        page.td("door", transfer.door().getCellName());
        page.td("domain", transfer.door().getDomainName());
        page.td("sequence", transfer.session().getSerialId());
        page.td("protocol", transfer.door().getProtocolFamily() +
                     "-" + transfer.door().getProtocolVersion());

        String tmp = transfer.door().getOwner() ;
        tmp = tmp.contains("known") ? "?" : _fieldMap.mapOwner(tmp) ;
        page.td("owner", tmp);

        tmp = transfer.door().getProcess() ;
        tmp = tmp.contains("known") ? "?" : tmp ;
        page.td("process", tmp);

        String poolName = transfer.session().getPool() ;
        if (poolName == null || poolName.equals("<unknown>")) {
            poolName = "N.N.";
        }
        page.td("pnfs", transfer.session().getPnfsId());
        page.td("pool", poolName);
        page.td("host", transfer.session().getReplyHost());
        String status = transfer.session().getStatus();
        page.td("status", status != null ? status.replace(" ", "&nbsp;") : "");
        page.td("wait", getTimeString(now - transfer.session().getWaitingSince()));

        if (transfer.mover() == null) {
            if (poolName.equals("N.N.")) {
                page.td(3, "staging", "Staging");
            }else{
                page.td(3, "missing", "No Mover found");
            }
        } else {
            page.td("state", transfer.mover().getStatus());
            if (transfer.mover().getStartTime() > 0L) {
                long transferTime     = transfer.mover().getTransferTime() ;
                long bytesTransferred = transfer.mover().getBytesTransferred() ;
                //  appendCells(sbtransferTime/1000L);
                page.td("transferred", bytesTransferred / 1024);
                page.td("speed", transferTime > 0 ?
                             (1000 * bytesTransferred) / (1024 * transferTime) :
                             "-");
                //  appendCells(sb, (now-io.getMover().getStartTime())/1000L);
            } else {
                page.td("transferred", "-");
                page.td("speed", "-");
            }
        }
        page.endRow();
    }

    public String createHtmlTable(List<Transfer> transfers)
    {
        HTMLBuilder page = new HTMLBuilder(_nucleus.getDomainContext());

        page.addHeader("/styles/transfers.css", "Active Transfers");
        page.beginTable("sortable",
                        "door", "Door",
                        "domain", "Domain",
                        "sequence", "Seq",
                        "protocol", "Prot",
                        "owner", "Owner",
                        "process", "Proc",
                        "pnfs", "PnfsId",
                        "pool", "Pool",
                        "host", "Host",
                        "status", "Status",
                        "wait", "Since",
                        "state", "S",
                        "transferred", "Trans.&nbsp;(KB)",
                        "speed", "Speed&nbsp;(KB/s)");
        for (Transfer transfer : transfers) {
            createHtmlTableRow(page, transfer);
        }
        page.endTable();
        page.addFooter(getClass().getName());
        return page.toString();
    }

    public String createErrorHtmlTable(List<Transfer> transfers)
    {
        HTMLBuilder page = new HTMLBuilder(_nucleus.getDomainContext());

        page.addHeader("/styles/transfers.css", "Active Transfers");
        page.beginTable("sortable",
                        "door", "Door",
                        "domain", "Domain",
                        "sequence", "Seq",
                        "protocol", "Prot",
                        "owner", "Owner",
                        "process", "Proc",
                        "pnfs", "PnfsId",
                        "pool", "Pool",
                        "host", "Host",
                        "status", "Status",
                        "wait", "Since",
                        "state", "S",
                        "transferred", "Trans.&nbsp;(KB)",
                        "speed", "Speed&nbsp;(KB/s)");

        for (Transfer transfer : transfers) {
            if (transfer.mover() == null) {
                createHtmlTableRow(page, transfer);
            }
        }
        page.endTable();
        page.addFooter(getClass().getName());

        return page.toString();
    }
}
