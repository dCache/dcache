// $Id: TransferObserverV1.java,v 1.18 2007-09-27 15:03:17 behrmann Exp $

package diskCacheV111.cells;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.util.HTMLBuilder;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginManagerChildrenInfo;

import org.dcache.cells.CellStub;
import org.dcache.util.Args;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TransferObserverV1
    extends CellAdapter
    implements Runnable
{
    private final Logger _log =
        LoggerFactory.getLogger(TransferObserverV1.class);

    private final CellNucleus   _nucleus;
    private final CellStub      _cellStub;
    private final Args          _args;
    private final DoorHandler   _doors;
    private final String        _loginBroker;
    private       List<IoEntry> _ioList;
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

    private static class DoorHandler
    {
        private final Map<CellAddressCore, Entry> _doors = new ConcurrentHashMap<>();

        private synchronized Entry defineDoor(CellAddressCore address)
        {
            Entry entry = _doors.get(address);
            if (entry == null) {
                entry = new Entry(address, true);
                _doors.put(address, entry);
            }
            return entry;
        }

        private Set<CellAddressCore> doors()
        {
            return _doors.keySet();
        }

        private Collection<Entry> entries()
        {
            return _doors.values();
        }

        private Entry undefineDoor(CellAddressCore address)
        {
            Entry entry = _doors.get(address);
            if (entry != null) {
                entry.setFixed(false);
            }
            return entry;
        }

        private synchronized Entry addDoor(CellAddressCore door)
        {
            Entry entry = _doors.get(door);
            if (entry == null) {
                entry = new Entry(door, false);
                _doors.put(door, entry);
            }
            return entry;
        }

        private Entry setDoorInfo(LoginManagerChildrenInfo info)
        {
            Entry entry =
                    addDoor(new CellAddressCore(info.getCellName(), info.getCellDomainName()));
            entry.setChildInfo(info);
            return entry;
        }

        private synchronized void clear()
        {
            Iterator<Entry> i = _doors.values().iterator();
            while (i.hasNext()) {
                Entry entry = i.next();
                if (entry.isFixed()) {
                    entry.setChildInfo(null);
                } else {
                    i.remove();
                }
            }
        }

        private static class Entry
        {
            private boolean _isFixed;
            private CellAddressCore _doorAddress;
            private LoginManagerChildrenInfo _info;

            private Entry(CellAddressCore doorAddress, boolean isFixed)
            {
                _isFixed  = isFixed;
                _doorAddress = doorAddress;
            }

            private LoginManagerChildrenInfo getChildInfo()
            {
                return _info;
            }

            private void setChildInfo(LoginManagerChildrenInfo info)
            {
                _info = info;
            }

            private boolean isFixed()
            {
                return _isFixed;
            }

            private void setFixed(boolean fixed)
            {
                _isFixed = fixed;
            }
        }
    }

    public TransferObserverV1(String name, String  args) throws Exception
    {
        super(name, TransferObserverV1.class.getName(), args, false);

        _nucleus = getNucleus();
        _cellStub = new CellStub(this, null, 30, SECONDS);
        _args    = getArgs();
        _doors   = new DoorHandler();

        try {
            if (_args.argc() < 0) {
                throw new IllegalArgumentException("Usage : ... ");
            }

            //
            // check for 'doors' option. If present,
            // load them into the doors (fixed)
            //
            String doorList = _args.getOpt("doors");
            if (doorList != null) {
                for (String s : doorList.split(",")) {
                    _doors.defineDoor(new CellAddressCore(s));
                }
            }
            //

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
            _loginBroker = _args.getOpt("loginBroker");
            //
            _fieldMap = new FieldMap(_args.getOpt("fieldMap"), _args);
            //
            _nucleus.newThread(this, "worker").start();
            //
        } catch (Exception e) {
            start();
            kill();
            throw e;
        }
        useInterpreter(true);
        start();
        export();
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
        pw.println("    $Id: TransferObserverV1.java,v 1.18 2007-09-27 15:03:17 behrmann Exp $");
        pw.println("    Update Time : "+(_update/1000L)+" seconds");
        pw.println("        Counter : "+_processCounter);
        pw.println(" Last Time Used : "+_timeUsed+" msec's");
    }

    @Override
    public void run()
    {
        try {
            while (true) {
                try {
                    _processCounter++;
                    long start = System.currentTimeMillis();
                    collectDataSequentially();
                    _timeUsed = System.currentTimeMillis() - start;
                } catch (Exception ee) {
                    _log.warn(ee.toString(), ee);
                }

                synchronized (this) {
                    wait(_update);
                }
            }
        } catch (InterruptedException e) {
            _log.info("Data collector interrupted");
        }
    }
    //
    // lowest priority transfer observer.
    //
     private static class IoEntry implements Comparable<IoEntry>
     {
         private final IoDoorInfo _ioDoorInfo ;
         private final IoDoorEntry _ioDoorEntry ;
         private IoJobInfo   _ioJobInfo;

         private IoEntry(IoDoorInfo info, IoDoorEntry entry)
         {
             _ioDoorInfo = info ;
             _ioDoorEntry = entry ;
         }

         @Override
         public int compareTo(IoEntry other)
         {
             int tmp = _ioDoorInfo.getDomainName().compareTo(other._ioDoorInfo.getDomainName()) ;
             if (tmp != 0) {
                 return tmp;
             }
             tmp = _ioDoorInfo.getCellName().compareTo(other._ioDoorInfo.getCellName()) ;
             if (tmp != 0) {
                 return tmp;
             }
             return Long.valueOf(_ioDoorEntry.getSerialId()).
                 compareTo(other._ioDoorEntry.getSerialId());
         }

         @Override
         public boolean equals(Object obj)
         {
             if( obj == this ) {
                 return true;
             }
             if( !(obj instanceof IoEntry ) ) {
                 return false;
             }

             IoEntry other = (IoEntry)obj;
             return _ioDoorInfo.getDomainName().equals(other._ioDoorInfo.getDomainName()) &&
                 _ioDoorInfo.getCellName().equals(other._ioDoorInfo.getCellName()) &&

                 (_ioDoorEntry.getSerialId() == other._ioDoorEntry.getSerialId());
         }

         @Override
         public int hashCode() {
             // required to by some Collections
             return 17;
         }
    }

    public static final String hh_go = "[-parallel]";
    public String ac_go(Args args )
    {
        if (args.hasOption("parallel")) {
            _nucleus.newThread(new Runnable(){
                    @Override
                    public void run()
                    {
                        collectDataSequentially();
                    }
                }, "worker").start();
            return "Started";
        } else {
            synchronized (this) {
                notifyAll();
            }
            return "Process Notified";
        }
    }

    private void getBrokerInfo()
    {
        //
        // ask the broker for doors.
        //
        if (_loginBroker != null) {
            List<LoginBrokerInfo> infoList = new ArrayList<>();

            for (String loginBroker : _loginBroker.split(",")) {
                _log.info("Requesting doorInfo from LoginBroker " + loginBroker);
                try {
                    CellAddressCore brokerAddress = new CellAddressCore(loginBroker);
                    LoginBrokerInfo [] infos =
                            _cellStub.sendAndWait(new CellPath(brokerAddress), "ls -binary -all",
                                                  LoginBrokerInfo[].class);

                    StringBuilder sb = new StringBuilder();
                    sb.append("LoginBroker (").append(loginBroker)
                            .append(") : ");
                    for (LoginBrokerInfo info : infos) {
                        CellAddressCore doorAddress =
                                new CellAddressCore(info.getCellName(), info.getDomainName());
                        _doors.addDoor(doorAddress);
                        sb.append(doorAddress).append(",");
                    }
                    _log.info(sb.toString());
                    infoList.addAll(Arrays.asList(infos));
                } catch (Exception e) {
                    _log.info("Error from sendAndWait : " + e);
                }
            }
            updateDoorPage(infoList.toArray(new LoginBrokerInfo[infoList.size()])) ;
        }
    }

    private void collectDataSequentially()
    {
        _doors.clear();

        getBrokerInfo();

        _log.info("Asking doors for 'doorClientList' (one by one)");
        for (CellAddressCore doorAddress : _doors.doors()) {
            _log.info("Requesting client list from : {}", doorAddress);
            try {
                LoginManagerChildrenInfo info =
                        _cellStub.sendAndWait(new CellPath(doorAddress), "get children -binary",
                                              LoginManagerChildrenInfo.class);
                _log.info(doorAddress + " reported about {} children", info.getChildrenCount());
                _doors.setDoorInfo(info);
            } catch (Exception e) {
                _doors.undefineDoor(doorAddress);
                _log.info("Exception : " + e);
            }
        }
        //
        // now we got all our Children ...
        //
        Map<String, IoEntry> ioList   = new HashMap<>();
        Set<String>          poolHash = new HashSet<>();
        for (DoorHandler.Entry entry : _doors.entries()) {
            LoginManagerChildrenInfo info = entry.getChildInfo();

            if (info == null) {
                continue;
            }

            for (String child: info.getChildren()) {
                CellAddressCore childDoor = new CellAddressCore(child, info.getCellDomainName());

                _log.info("Requesting client info from: {}", childDoor);
                try {
                    IoDoorInfo ioDoorInfo =
                            _cellStub.sendAndWait(new CellPath(childDoor), "get door info -binary", IoDoorInfo.class);

                    _log.info(childDoor + " reply ok");

                    List<IoDoorEntry> ioDoorEntries = ioDoorInfo.getIoDoorEntries();
                    if (ioDoorEntries.size() == 0) {
                        continue;
                    }

                    for (IoDoorEntry ioDoorEntry : ioDoorEntries) {
                        _log.info("Adding ioEntry: {}", ioDoorEntry);
                        ioList.put(childDoor + "#" + ioDoorEntry.getSerialId(),
                                new IoEntry(ioDoorInfo, ioDoorEntry));
                        String pool = ioDoorEntry.getPool();
                        if (pool != null && pool.length() > 0 && !pool
                                .startsWith("<")) {
                            poolHash.add(pool);
                        }
                    }

                } catch (Exception e) {
                    _log.info("Exception: {}", e);
                }
            }
        }
        _log.info("Asking pools for io info");
        for (String poolName : poolHash) {
            _log.info("Asking pool: {}", poolName);
            try {
                IoJobInfo[] infos =
                        _cellStub.sendAndWait(new CellPath(new CellAddressCore(poolName)), "mover ls -binary",
                                              IoJobInfo[].class);

                _log.info("{} reply ok", poolName);

                //
                // where is our client
                //
                for (IoJobInfo info : infos) {
                    String client = info.getClientName()+"#"+
                        info.getClientId() ;
                    IoEntry ioEntry = ioList.get(client);
                    if (ioEntry == null) {
                        _log.info("No entry found for {}", client);
                    } else {
                        ioEntry._ioJobInfo = info;
                    }
                }
            } catch (Exception e) {
                _log.info("Exception: {}", e);
            }
        }
        List<IoEntry> resultList;
        synchronized (this) {
            _ioList = new ArrayList<>(new TreeSet<>(ioList.values()));
            _nucleus.getDomainContext().put("transfers.list", _ioList);

            resultList = _ioList;
        }
        _nucleus.getDomainContext().put("transfers.html",
                                        createHtmlTable(resultList));
        _nucleus.getDomainContext().put("transfers.txt",
                                        createAsciiTable(resultList));

        createDynamicTables(resultList);
    }

    //
    // the html stuff.
    //

    private void updateDoorPage(LoginBrokerInfo [] infos)
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

        for (LoginBrokerInfo info : infos) {
            page.beginRow(null, "odd");
            page.td("cell",     info.getCellName());
            page.td("domain",   info.getDomainName());
            page.td("protocol", info.getProtocolFamily());
            page.td("version",  info.getProtocolVersion());
            page.td("host",     info.getHost());
            page.td("port",     info.getPort());
            page.td("load",     (int)(info.getLoad()*100.0));
            page.endRow();
        }
        page.endTable();

        page.addFooter(getClass().getName() + " [$Revision: 1.18 $]");
        page.writeToContext("doors.html");
    }

    private synchronized void createDynamicTables(List<IoEntry> list)
    {
        for (TableEntry entry : _tableHash.values()) {
            String tableName = entry.getName();
            int [] array     = entry.getFields();
            _nucleus.getDomainContext().
                put(tableName + ".html", createDynamicTable(list, array));
        }
    }

    private String createDynamicTable(List<IoEntry> ioList, int [] fields)
    {
        HTMLBuilder page = new HTMLBuilder(_nucleus.getDomainContext());

        page.addHeader("/styles/transfers.css", "Active Transfers");

        page.beginTable("sortable");
        page.beginTHead();
        for (int field : fields) {
            page.th(__className[field], __listHeader[field]);
        }
        page.endTHead();

        for (IoEntry entry : ioList) {
            List<String> values = createFieldList(entry);
            for (int field : fields) {
                if (field >= values.size()) {
                    page.td(__className[field], "");
                } else {
                    page.td(__className[field], values.get(field));
                }
            }
        }
        page.endTable();
        page.addFooter(getClass().getName() + " [$Revision: 1.18 $]");
        return page.toString();
    }

    public static final String hh_ls_iolist = "";
    public synchronized String ac_ls_iolist(Args args)
    {
        if (_ioList == null) {
            return "";
        }
        return createAsciiTable(_ioList);
    }

    private String createAsciiTable(List<IoEntry> ioList)
    {
        long          now = System.currentTimeMillis();
        StringBuilder sb  = new StringBuilder();

        for (IoEntry io : ioList) {
            sb.append(io._ioDoorInfo.getCellName()).append(" ").
                append(io._ioDoorInfo.getDomainName()).append(" ");
            sb.append(io._ioDoorEntry.getSerialId()).append(" ");
            sb.append(io._ioDoorInfo.getProtocolFamily()).append("-").
                append(io._ioDoorInfo.getProtocolVersion()).append(" ");
            sb.append(io._ioDoorInfo.getOwner()).append(" ").
                append(io._ioDoorInfo.getProcess()).append(" ");
            sb.append(io._ioDoorEntry.getPnfsId()).append(" ").
                append(io._ioDoorEntry.getPool()).append(" ").
                append(io._ioDoorEntry.getReplyHost()).append(" ").
                append(io._ioDoorEntry.getStatus()).append(" ").
                append((now -io._ioDoorEntry.getWaitingSince())).append(" ");

            if (io._ioJobInfo == null) {
                sb.append("No-Mover-Found");
            } else {
                sb.append(io._ioJobInfo.getStatus()).append(" ");
                if (io._ioJobInfo.getStartTime() > 0L) {
                    long transferTime     = io._ioJobInfo.getTransferTime();
                    long bytesTransferred = io._ioJobInfo.getBytesTransferred();
                    sb.append(transferTime).append(" ").
                        append(bytesTransferred).append(" ").
                        append( transferTime > 0 ? ( (double)bytesTransferred/(double)transferTime ) : 0 ).
                        append(" ");
                    sb.append((now-io._ioJobInfo.getStartTime())).append(" ");
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private List<String> createFieldList(IoEntry io)
    {
        long         now = System.currentTimeMillis();
        List<String> out = new ArrayList<>(20);

        PnfsId pnfsid = io._ioDoorEntry.getPnfsId();
        String status = io._ioDoorEntry.getStatus();
        out.add(io._ioDoorInfo.getCellName());
        out.add(io._ioDoorInfo.getDomainName());
        out.add(String.valueOf(io._ioDoorEntry.getSerialId()));
        out.add(io._ioDoorInfo.getProtocolFamily()+"-"+
                io._ioDoorInfo.getProtocolVersion());
        out.add(_fieldMap.mapOwner(io._ioDoorInfo.getOwner()));
        out.add(io._ioDoorInfo.getProcess());
        out.add(pnfsid == null ? "" : pnfsid.toString());
        out.add(io._ioDoorEntry.getPool());
        out.add(io._ioDoorEntry.getReplyHost());
        out.add(status == null ? "" : status.replace(" ", "&nbsp;"));
        out.add(getTimeString(now - io._ioDoorEntry.getWaitingSince()));

        if (io._ioJobInfo != null) {
            out.add(io._ioJobInfo.getStatus());
            out.add(getTimeString(now - io._ioJobInfo.getSubmitTime()));
            if (io._ioJobInfo.getStartTime() > 0L) {
                long transferTime     = io._ioJobInfo.getTransferTime();
                long bytesTransferred = io._ioJobInfo.getBytesTransferred();
                out.add(getTimeString(transferTime));
                out.add(Long.toString(bytesTransferred / 1024));
                out.add(transferTime > 0 ?
                        String.valueOf((1000 * bytesTransferred) / (1024 * transferTime)) :
                        "-");
                out.add(getTimeString(now - io._ioJobInfo.getStartTime()));
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

    private void createHtmlTableRow(HTMLBuilder page, IoEntry io)
    {
        long now = System.currentTimeMillis();

        page.beginRow(null, "odd");
        page.td("door", io._ioDoorInfo.getCellName());
        page.td("domain", io._ioDoorInfo.getDomainName());
        page.td("sequence", io._ioDoorEntry.getSerialId());
        page.td("protocol", io._ioDoorInfo.getProtocolFamily() +
                     "-" + io._ioDoorInfo.getProtocolVersion());

        String tmp = io._ioDoorInfo.getOwner() ;
        tmp = tmp.contains("known") ? "?" : _fieldMap.mapOwner(tmp) ;
        page.td("owner", tmp);

        tmp = io._ioDoorInfo.getProcess() ;
        tmp = tmp.contains("known") ? "?" : tmp ;
        page.td("process", tmp);

        String poolName = io._ioDoorEntry.getPool() ;
        if (poolName == null || poolName.equals("<unknown>")) {
            poolName = "N.N.";
        }
        page.td("pnfs", io._ioDoorEntry.getPnfsId());
        page.td("pool", poolName);
        page.td("host", io._ioDoorEntry.getReplyHost());
        String status = io._ioDoorEntry.getStatus();
        page.td("status", status != null ? status.replace(" ", "&nbsp;") : "");
        page.td("wait", getTimeString(now - io._ioDoorEntry.getWaitingSince()));

        if (io._ioJobInfo == null) {
            if (poolName.equals("N.N.")) {
                page.td(3, "staging", "Staging");
            }else{
                page.td(3, "missing", "No Mover found");
            }
        } else {
            page.td("state", io._ioJobInfo.getStatus());
            if (io._ioJobInfo.getStartTime() > 0L) {
                long transferTime     = io._ioJobInfo.getTransferTime() ;
                long bytesTransferred = io._ioJobInfo.getBytesTransferred() ;
                //  appendCells(sbtransferTime/1000L);
                page.td("transferred", bytesTransferred / 1024);
                page.td("speed", transferTime > 0 ?
                             (1000 * bytesTransferred) / (1024 * transferTime) :
                             "-");
                //  appendCells(sb, (now-io._ioJobInfo.getStartTime())/1000L);
            } else {
                page.td("transferred", "-");
                page.td("speed", "-");
            }
        }
        page.endRow();
    }

    public String createHtmlTable(List<IoEntry> ioList)
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
        for (IoEntry io : ioList) {
            createHtmlTableRow(page, io);
        }
        page.endTable();
        page.addFooter(getClass().getName() + " [$Revision: 1.18 $]");
        return page.toString();
    }

    public String createErrorHtmlTable(List<IoEntry> ioList)
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

        for (IoEntry io : ioList) {
            if (io._ioJobInfo == null) {
                createHtmlTableRow(page, io);
            }
        }
        page.endTable();
        page.addFooter(getClass().getName() + " [$Revision: 1.18 $]");

        return page.toString();
    }
}
