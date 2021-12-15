package diskCacheV111.cells;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.util.ByteUnit.BYTES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import diskCacheV111.util.HTMLBuilder;
import diskCacheV111.util.TransferInfo;
import diskCacheV111.util.UserInfo;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginBrokerSubscriber;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.cells.CellStub;
import org.dcache.util.Args;
import org.dcache.util.NetworkUtils;
import org.dcache.util.TransferCollector;
import org.dcache.util.TransferCollector.Transfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferObserverV1
      extends CellAdapter
      implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransferObserverV1.class);

    private final CellNucleus _nucleus;
    private final CellStub _cellStub;
    private final Args _args;
    private TransferCollector _collector;
    private Thread _workerThread;
    private LoginBrokerSubscriber _loginBrokerSource;
    private long _update = 120000L;
    private long _timeUsed;
    private long _processCounter;
    private final Map<String, TableEntry> _tableHash = new HashMap<>();

    private static final String[] __className = {"cell",               //   0
          "domain",             //   1
          "sequence",           //   2
          "protocol",           //   3
          "uid",                //   4
          "gid",                //   5
          "vomsGroup",          //   6
          "process",            //   7
          "pnfs",               //   8
          "pool",               //   9
          "host",               //  10
          "status",             //  11
          "waiting",            //  12
          "state",              //  13
          "submitted",          //  14
          "time",               //  15
          "transferred",        //  16
          "speed",              //  17
          "running"             //  18
    };

    private static final String[] __listHeader = {"Cell",               //   0
          "Domain",             //   1
          "Seq",                //   2
          "Prot",               //   3
          "UID",                //   4
          "GID",                //   5
          "VOMS Group",         //   6
          "Proc",               //   7
          "PnfsId",             //   8
          "Pool",               //   9
          "Host",               //  10
          "State",              //  11
          "Waiting",            //  12
          "JobState",           //  13
          "Submitted",          //  14
          "Time",               //  15
          "Trans.&nbsp;(KiB)",   //  16
          "Speed&nbsp;(KiB/s)",  //  17
          "Running"             //  18
    };

    private static class TableEntry {

        private final String _tableName;
        private final int[] _fields;
        private final String _title;
        private long _olderThan;
        private boolean _ifNotYetStarted;
        private boolean _ifMoverMissing;

        private TableEntry(String tableName, int[] fields, String title) {
            _tableName = tableName;
            _fields = fields;
            _title = title;
        }

        private int[] getFields() {
            return _fields;
        }

        private String getName() {
            return _tableName;
        }

        @Override
        public String toString() {
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

        private boolean ifNotYetStarted() {
            return _ifNotYetStarted;
        }

        private boolean ifMoverMissing() {
            return _ifMoverMissing;
        }

        private long getOlderThan() {
            return _olderThan;
        }
    }

    /**
     * <p>Used to produce JSON representation, but also provides methods
     * for conversion into .txt and .html table entries.</p>
     */
    private static class TransferBean extends TransferInfo {

        private static final long serialVersionUID = 3132576086376551676L;

        private final long now;

        public TransferBean() {
            now = System.currentTimeMillis();
        }

        public TransferBean(Transfer transfer, long now) {
            this.now = now;
            cellName = transfer.door().getCellName();
            domainName = transfer.door().getDomainName();
            serialId = transfer.session().getSerialId();
            setProtocol(transfer.door().getProtocolFamily(),
                  transfer.door().getProtocolVersion());
            Subject subject = transfer.session().getSubject();
            if (subject == null) {
                setUserInfo(new UserInfo());
            } else {
                setSubject(subject);
            }
            process = transfer.door().getProcess();
            pnfsId = Objects.toString(transfer.session().getPnfsId(), "");
            path = transfer.session().getPath();
            pool = Objects.toString(transfer.session().getPool(), "");
            replyHost = Objects.toString(transfer.session().getReplyHost(), "");
            sessionStatus = Objects.toString(transfer.session().getStatus(), "");
            waitingSince = transfer.session().getWaitingSince();

            IoJobInfo moverInfo = transfer.mover();
            if (moverInfo == null) {
                setMoverStatus(null);
            } else {
                moverId = moverInfo.getJobId();
                setMoverStatus(moverInfo.getStatus());
                moverSubmit = moverInfo.getSubmitTime();
                if (moverInfo.getStartTime() > 0L) {
                    transferTime = moverInfo.getTransferTime();
                    bytesTransferred = moverInfo.getBytesTransferred();
                    moverStart = moverInfo.getStartTime();
                }
            }
        }

        void toHtmlRow(List<String> out) {
            out.add(cellName);
            out.add(domainName);
            out.add(String.valueOf(serialId));
            out.add(protocol);
            out.add(userInfo.getUid());
            out.add(userInfo.getGid());
            out.add(userInfo.getPrimaryVOMSGroup());
            out.add(process);
            out.add(pnfsId);
            out.add(pool);
            out.add(replyHost);
            out.add(sessionStatus == null ? ""
                  : sessionStatus.replace(" ", "&nbsp;"));
            out.add(timeWaiting(now, true));

            if (moverStatus != null) {
                out.add(moverStatus.name());
                out.add(timeElapsedSinceSubmitted(now, true));
                addMoverInfo(out, true);
            }
        }

        void toAsciiLine(StringBuilder builder) {
            List<String> args = new ArrayList<>();
            args.add(cellName);
            args.add(domainName);
            args.add(String.valueOf(serialId));
            args.add(protocol);
            args.add(userInfo.getUid());
            args.add(process);
            args.add(pnfsId);
            args.add(pool);
            args.add(replyHost);
            args.add(sessionStatus);
            args.add(timeWaiting(now, false));

            if (moverStatus == null) {
                args.add("No-mover()-Found");
            } else {
                args.add(moverStatus.name());
                addMoverInfo(args, false);
            }

            Joiner.on(" ").appendTo(builder, args);

            builder.append('\n');
        }

        String timeWaiting(boolean display) {
            return timeWaiting(now, true);
        }

        private void addMoverInfo(List<String> list, boolean display) {
            if (moverStart != null) {
                list.add(getTimeString(transferTime, display));
                list.add(String.valueOf(bytesTransferred));
                if (display) {
                    list.add(transferTime > 0 ?
                          String.valueOf((1000 * bytesTransferred) / (1024 * transferTime))
                          : "-");
                } else {
                    list.add(String.valueOf(transferTime > 0 ?
                          ((double) bytesTransferred / (double) transferTime)
                          : 0));
                }
                list.add(timeRunning(now, display));
            }
        }
    }

    public TransferObserverV1(String name, String args) {
        super(name, TransferObserverV1.class.getName(), args);
        _nucleus = getNucleus();
        _cellStub = new CellStub(this, null, 30, SECONDS);
        _args = getArgs();
    }

    @Override
    protected void starting() throws Exception {
        if (_args.argc() < 0) {
            throw new IllegalArgumentException("Usage : ... ");
        }

        _loginBrokerSource = new LoginBrokerSubscriber();
        addCellEventListener(_loginBrokerSource);
        addCommandListener(_loginBrokerSource);
        _loginBrokerSource.setCellEndpoint(this);
        _loginBrokerSource.setTopic(_args.getOpt("loginBroker"));

        _collector = new TransferCollector(_cellStub, _loginBrokerSource.doors());

        String updateString = _args.getOpt("update");
        try {
            if (updateString != null) {
                _update = Long.parseLong(updateString) * 1000L;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Illegal value for -update: {}", updateString);
        }

        useInterpreter(true);
    }

    @Override
    protected void started() {
        _workerThread = _nucleus.newThread(this, "worker");
        _workerThread.start();
        _loginBrokerSource.afterStart();
    }

    @Override
    public void stopped() {
        if (_workerThread != null) {
            _workerThread.interrupt();
        }
        _loginBrokerSource.beforeStop();
    }

    public static final String hh_table_help = "";

    public String ac_table_help(Args args) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String header : __listHeader) {
            sb.append(i++).append(" ").append(header).append("\n");
        }
        return sb.toString();
    }

    public static final String hh_table_define = "<tableName> <n>[,<m>[,...]] [<tableHeader>]";

    public synchronized String ac_table_define_$_2_3(Args args)
          throws NumberFormatException {
        String tableName = args.argv(0);
        String header = args.argc() > 2 ? args.argv(2) : null;
        String[] list = args.argv(1).split(",");

        int[] array = new int[list.length];
        for (int i = 0; i < list.length; i++) {
            array[i] = Integer.parseInt(list[i]);
        }

        _tableHash.put(tableName, new TableEntry(tableName, array, header));

        return "";
    }

    public static final String hh_table_undefine = "<tableName>";

    public synchronized String ac_table_undefine_$_1(Args args) {
        String tableName = args.argv(0);
        _tableHash.remove(tableName);
        _nucleus.getDomainContext().remove(tableName + ".html");
        return "";
    }

    public static final String hh_table_ls = "[<tableName>]";

    public synchronized String ac_table_ls_$_0_1(Args args) {
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

    public String ac_set_update_$_1(Args args) {
        long update = Long.parseLong(args.argv(0)) * 1000L;
        if (update < 10000L) {
            throw new IllegalArgumentException(
                  "Update time must exceed 10 seconds");
        }

        synchronized (this) {
            _update = update;
            notifyAll();
        }
        return "Update time set to " + args.argv(0) + " seconds";
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println("    Update Time : " + (_update / 1000L) + " seconds");
        pw.println("        Counter : " + _processCounter);
        pw.println(" Last Time Used : " + _timeUsed + " milliseconds");
    }

    @Override
    public void messageArrived(CellMessage envelope) {
        Serializable message = envelope.getMessageObject();
        if (message instanceof LoginBrokerInfo) {
            _loginBrokerSource.messageArrived((LoginBrokerInfo) message);
        } else if (message instanceof NoRouteToCellException) {
            _loginBrokerSource.messageArrived((NoRouteToCellException) message);
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                try {
                    _processCounter++;
                    long start = System.currentTimeMillis();
                    collectDataSequentially();
                    _timeUsed = System.currentTimeMillis() - start;
                } catch (RuntimeException ee) {
                    LOGGER.error(ee.toString(), ee);
                }

                synchronized (this) {
                    wait(_update);
                }
            }
        } catch (InterruptedException e) {
            LOGGER.info("Data collector interrupted");
        }
    }

    public String ac_go(Args args) {
        synchronized (this) {
            notifyAll();
        }
        return "Update started.";
    }

    private void collectDataSequentially() throws InterruptedException {
        try {
            Collection<LoginBrokerInfo> loginBrokerInfos = _collector.getLoginBrokerInfo();
            Collection<LoginManagerChildrenInfo> loginManagerInfos = _collector.collectLoginManagerInfo(
                  TransferCollector.getLoginManagers(loginBrokerInfos)).get();
            Collection<IoDoorInfo> doorInfos = _collector.collectDoorInfo(
                  TransferCollector.getDoors(loginManagerInfos)).get();
            Collection<IoJobInfo> movers = _collector.collectMovers(
                  TransferCollector.getPools(doorInfos)).get();

            List<Transfer> transfers = TransferCollector.getTransfers(doorInfos,
                  movers);
            transfers.sort(new TransferCollector.ByDoorAndSequence());

            Map<String, Object> domainContext = _nucleus.getDomainContext();
            domainContext.put("doors.html", createDoorPage(loginBrokerInfos));
            domainContext.put("transfers.list", transfers);

            long now = System.currentTimeMillis();
            List<TransferBean> beans = transfers
                  .stream()
                  .map((t) -> new TransferBean(t, now))
                  .collect(Collectors.toList());

            domainContext.put("transfers.html", createHtmlTable(beans));
            domainContext.put("transfers.txt", createAsciiTable(beans));
            domainContext.put("transfers.json",
                  new ObjectMapper().writerWithDefaultPrettyPrinter()
                        .writeValueAsString(beans));

            synchronized (this) {
                for (TableEntry entry : _tableHash.values()) {
                    domainContext.put(entry.getName() + ".html",
                          createDynamicTable(transfers, entry.getFields()));
                }
            }
        } catch (ExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    //
    // the html stuff.
    //

    private String createDoorPage(Collection<LoginBrokerInfo> doors) {
        HTMLBuilder page = new HTMLBuilder(_nucleus.getDomainContext());

        page.addHeader("/styles/doors.css", "Doors");
        page.beginTable("sortable",
              "cell", "Cell",
              "domain", "Domain",
              "protocol", "Protocol",
              "version", "Version",
              "host", "Host",
              "port", "Port",
              "load", "Load");

        for (LoginBrokerInfo door : doors) {
            InetAddress address = door.getAddresses().stream().max(
                  Comparator.comparing(NetworkUtils.InetAddressScope::of)).get();
            page.beginRow(null, "odd");
            page.td("cell", door.getCellName());
            page.td("domain", door.getDomainName());
            page.td("protocol", door.getPreferredProtocolFamily());
            page.td("version", door.getProtocolVersion());
            page.td("host", address.getHostName());
            page.td("port", door.getPort());
            page.td("load", (int) (door.getLoad() * 100.0));
            page.endRow();
        }
        page.endTable();

        return page.toString();
    }

    private String createDynamicTable(List<Transfer> transfers, int[] fields) {
        HTMLBuilder page = new HTMLBuilder(_nucleus.getDomainContext());

        page.addHeader("/styles/transfers.css", "Active Transfers");

        page.beginTable("sortable");
        page.beginTHead();
        for (int field : fields) {
            page.th(__className[field], __listHeader[field]);
        }
        page.endTHead();

        long now = System.currentTimeMillis();

        for (Transfer transfer : transfers) {
            List<String> values = new ArrayList<>();
            new TransferBean(transfer, now).toHtmlRow(values);
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

    public synchronized String ac_ls_iolist(Args args) {
        return Objects.toString(_nucleus.getDomainContext().get("transfers.txt"), "");
    }

    private String createAsciiTable(List<TransferBean> transfers) {
        StringBuilder sb = new StringBuilder();
        transfers.stream().forEach((t) -> t.toAsciiLine(sb));
        return sb.toString();
    }

    private void createHtmlTableRow(HTMLBuilder page, TransferBean transfer) {
        page.beginRow(null, "odd");
        page.td("door", transfer.getCellName());
        page.td("domain", transfer.getDomainName());
        page.td("sequence", transfer.getSerialId());
        page.td("protocol", transfer.getProtocol().replaceAll("[<]unknown[>]", "?"));
        page.td("uid", transfer.getUserInfo().getUid());
        page.td("gid", transfer.getUserInfo().getGid());
        page.td("vomsGroup", transfer.getUserInfo().getPrimaryVOMSGroup());

        String tmp = transfer.getProcess();
        tmp = tmp.contains("known") ? "?" : tmp;
        page.td("process", tmp);

        String poolName = Strings.emptyToNull(transfer.getPool());
        if (poolName == null || poolName.equals("<unknown>")) {
            poolName = "N.N.";
        }

        page.td("pnfs", transfer.getPnfsId());
        page.td("pool", poolName);
        page.td("host", transfer.getReplyHost());
        String status = transfer.getSessionStatus();
        page.td("status", status != null ? status.replace(" ", "&nbsp;") : "");
        page.td("waiting", transfer.timeWaiting(true));

        if (transfer.getMoverStatus() == null) {
            if (poolName.equals("N.N.")) {
                page.td(3, "staging", "Staging");
            } else {
                page.td(3, "missing", "No Mover found");
            }
        } else {
            page.td("state", transfer.getMoverStatus());
            if (transfer.getMoverStart() != null) {
                page.td("transferred", BYTES.toKiB(transfer.getBytesTransferred()));
            } else {
                page.td("transferred", "-");
            }
            page.td("speed", getTransferRate(transfer));
        }
        page.endRow();
    }

    private Double getTransferRate(TransferInfo info) {
        Long bytes = info.getBytesTransferred();
        if (bytes == null) {
            return 0.0;
        }

        Long time = info.getTransferTime();
        if (time == null) {
            return 0.0;
        }

        double secs = time / 1000.0;
        return secs > 0.0 ? BYTES.toKiB((double) bytes) / secs : 0.0;
    }

    private String createHtmlTable(List<TransferBean> transfers) {
        HTMLBuilder page = new HTMLBuilder(_nucleus.getDomainContext());

        page.addHeader("/styles/transfers.css", "Active Transfers");
        page.beginTable("sortable",
              "door", "Door",
              "domain", "Domain",
              "sequence", "Seq",
              "protocol", "Prot",
              "uid", "UID",
              "gid", "GID",
              "vomsGroup", "VOMS Group",
              "process", "Proc",
              "pnfs", "PnfsId",
              "pool", "Pool",
              "host", "Host",
              "status", "Status",
              "waiting", "Waiting",
              "state", "S",
              "transferred", "Trans.&nbsp;(KiB)",
              "speed", "Speed&nbsp;(KiB/s)");
        transfers.stream().forEach((t) -> createHtmlTableRow(page, t));
        page.endTable();
        page.addFooter(getClass().getName());
        return page.toString();
    }
}
