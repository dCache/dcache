package diskCacheV111.poolManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.HTMLWriter;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.hsmControl.HsmControlGetBfDetailsMsg;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.AgingHash;
import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.Partition;
import org.dcache.util.Args;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static java.util.concurrent.TimeUnit.SECONDS;

public class HttpPoolMgrEngineV3 implements HttpResponseEngine, Runnable,
        CellInfoProvider
{
    private static final String PARAMETER_DCACHE = "dcache";
    private static final String PARAMETER_GREP = "grep";
    private static final String PARAMETER_LINKGROUP = "linkGroup";
    private static final String PARAMETER_NET = "net";
    private static final String PARAMETER_PROTOCOL = "protocol";
    private static final String PARAMETER_SORT = "sort";
    private static final String PARAMETER_STORE = "store";
    private static final String PARAMETER_TYPE = "type";
    private final static Logger _log =
        LoggerFactory.getLogger(HttpPoolMgrEngineV3.class);

    private final static long TIMEOUT = 20000;

    private final Thread _restoreCollector;
    private final CellStub _poolManager;
    private final CellStub _pnfsManager;
    private final CellStub _hsmController;

    private final CellEndpoint _endpoint;
    private final AgingHash   _pnfsPathMap     = new AgingHash(500);
    private final AgingHash _fileAttributesMap = new AgingHash(500);
    private final boolean     _takeAll         = true;
    private final SimpleDateFormat _formatter  = new SimpleDateFormat ("MM.dd HH:mm:ss");
    private volatile List<Object[]> _lazyRestoreList =
        new ArrayList<>();
    private long        _collectorUpdate = 60000L;
    private long        _errorCounter;
    private long        _requestCounter;
    private final Object      _updateLock      = new Object();
    private boolean     _addStorageInfo;
    private boolean     _addHsmInfo;
    private String[]    _siDetails;
    private String      _cssFile         = "/poolInfo/css/default.css";

    public HttpPoolMgrEngineV3(CellEndpoint endpoint, String[] argsString)
    {
        _endpoint = endpoint;
        // FIXME: Do not hardcode cell addresses
        _poolManager = new CellStub(endpoint, new CellPath("PoolManager"), TIMEOUT, SECONDS);
        _pnfsManager = new CellStub(endpoint, new CellPath("PnfsManager"), TIMEOUT, SECONDS);
        _hsmController = new CellStub(endpoint, new CellPath("HsmManager"), TIMEOUT, SECONDS);

        for (int i = 0; i < argsString.length; i++) {
            _log.info("HttpPoolMgrEngineV3 : argument : "+i+" : "+argsString[i]);
            if (argsString[i].equals("addStorageInfo")) {
                _addStorageInfo = true;
                _log.info("Option accepted : addStorageInfo");
            } else if (argsString[i].equals("addHsmInfo")) {
                _addHsmInfo = true;
                _log.info("Option accepted : addHsmInfo");
            } else if (argsString[i].startsWith("details=")) {
                _log.info("Details for lazy restore : "+argsString[i]);
                decodeDetails(argsString[i]);
            } else if (argsString[i].startsWith("css=")) {
                decodeCss(argsString[i].substring(4));
            }
        }
        _restoreCollector = new Thread(this, "restore-collector");
        _log.info("Using CSS file : "+_cssFile);
    }

    @Override
    public void startup()
    {
        _restoreCollector.start();
    }

    @Override
    public void shutdown()
    {
        _restoreCollector.interrupt();
        try {
            _restoreCollector.join();
        } catch (InterruptedException e) {
            _log.warn("Interrupted while waiting for restore-collector to terminate");
        }
    }

    private void decodeCss(String cssDetails)
    {
        cssDetails = cssDetails.trim();

        if ((cssDetails.length() > 0) && !cssDetails.equals("default")) {
            _cssFile = cssDetails;
        }

    }

    private void decodeDetails(String details)
    {
        if (details.startsWith("details=") && (details.length() >= 9)) {
            _siDetails = details.substring(8).split(",");
        }
    }

    @Override
    public void run()
    {
        _log.info("Restore Collector Thread started");
        try {
            while (!Thread.interrupted()) {
                try {
                    synchronized (_updateLock) {
                        _updateLock.wait(_collectorUpdate);
                    }
                    runRestoreCollector();
                } catch(NoRouteToCellException e) {
                    _log.warn("Restore Collector got : " + e, e);
                }
            }
        } catch (InterruptedException e) {
            _log.debug("Restore Collector interrupted");
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(" PoolManagerEngine : [$Id: HttpPoolMgrEngineV3.java,v 1.26 2007-08-16 20:20:56 behrmann Exp $]");
        pw.println("   Request Counter : "+_requestCounter);
        pw.println("     Error Counter : "+_errorCounter);
        pw.println("  Collector Update : "+(_collectorUpdate/1000L)+" seconds");
        pw.println("    addStorageInfo : "+_addStorageInfo);
        pw.println("        addHsmInfo : "+_addHsmInfo);
    }

    public static final String hh_set_update = "[<updateTime/sec>]";
    public String ac_set_update_$_0_1(Args args)
    {
        if (args.argc() == 0) {
            synchronized (_updateLock) {
                _updateLock.notifyAll();
            }
        } else {
            long x = Long.parseLong(args.argv(0));
            if (x < 30) {
                throw new
                        IllegalArgumentException("<updateTime> must be > 30");
            }
            synchronized (_updateLock) {
                _collectorUpdate = x * 1000;
                _updateLock.notifyAll();
            }
        }
        return "";
    }

    private void runRestoreCollector()
        throws NoRouteToCellException, InterruptedException
    {
        RestoreHandlerInfo[] infos;
        try {
            infos = _poolManager.sendAndWait("xrc ls", RestoreHandlerInfo[].class);
        } catch (CacheException e) {
            _log.warn("runRestoreCollector : failure reply from PoolManager : " + e.getMessage());
            return;
        }

        List<Object[]> agedList = new ArrayList<>();
        long      cut      = System.currentTimeMillis() - (1000L * 60L * 2L);
        for (RestoreHandlerInfo info : infos) {
            if (_takeAll || (info.getStartTime() < cut)) {
                try {
                    Object[] a = new Object[3];
                    a[0] = info;
                    String  name   = info.getName();
                    String  pnfsId = name.substring(0,name.indexOf('@'));
                    //
                    // collect the pathes
                    //
                    String  path   = (String)_pnfsPathMap.get(pnfsId);
                    if (path == null) {
                        path = getPathByPnfsId(pnfsId);
                    }
                    if (path == null) {
                        a[1] = pnfsId;
                    } else {
                        _pnfsPathMap.put(pnfsId, a[1] = path);
                    }
                    //
                    // collect the storage infos
                    //
                    if (_addStorageInfo) {
                        FileAttributes fileAttributes = (FileAttributes) _fileAttributesMap.get(pnfsId);
                        if (fileAttributes == null) {
                            fileAttributes = getFileAttributesByPnfsId(pnfsId);
                        }
                        if (fileAttributes != null) {
                            StorageInfo storageInfo = fileAttributes.getStorageInfo();
                            if (_addHsmInfo) {
                                StorageInfo si = getHsmInfoByStorageInfo(pnfsId,storageInfo);
                                if (si != null) {
                                    storageInfo = si;
                                    fileAttributes.setStorageInfo(si);
                                }
                            }
                            if (_siDetails != null) { // allows to select items
                                storageInfo.setKey("size",""+fileAttributes.getSize());
                                storageInfo.setKey("new",""+storageInfo.isCreatedOnly());
                                storageInfo.setKey("stored",""+storageInfo.isStored());
                                storageInfo.setKey("sClass",storageInfo.getStorageClass());
                                storageInfo.setKey("cClass",storageInfo.getCacheClass());
                                storageInfo.setKey("hsm",storageInfo.getHsm());
                            }
                            _fileAttributesMap.put(pnfsId, a[2] = storageInfo);
                        }

                    }

                    agedList.add(a);
                } catch (Exception e) {
                    _log.warn(e.toString(), e);
                }
            }
        }
        _lazyRestoreList = agedList;
    }

    private StorageInfo getHsmInfoByStorageInfo(String pnfsId, StorageInfo storageInfo)
    {
        try {
            HsmControlGetBfDetailsMsg msg =
                new HsmControlGetBfDetailsMsg(new PnfsId(pnfsId),storageInfo,"default");
            return _hsmController.sendAndWait(msg).getStorageInfo();
        } catch (InterruptedException | CacheException e) {
            _log.warn(e.toString(), e);
            return null;
        }
    }

    private String getPathByPnfsId(String pnfsId)
    {
        try {
            PnfsMapPathMessage msg = new PnfsMapPathMessage(new PnfsId(pnfsId));
            return _pnfsManager.sendAndWait(msg).getGlobalPath();
        } catch (InterruptedException | CacheException e) {
            _log.warn(e.toString());
            return null;
        }
    }

    private FileAttributes getFileAttributesByPnfsId(String pnfsId)
    {
        try {
            PnfsGetFileAttributes msg =
                    new PnfsGetFileAttributes(new PnfsId(pnfsId), EnumSet.of(FileAttribute.SIZE, FileAttribute.STORAGEINFO));
            return _pnfsManager.sendAndWait(msg).getFileAttributes();
        } catch (InterruptedException | CacheException e) {
            _log.warn(e.toString());
            return null;
        }
    }

    private void printMenu(PrintWriter pw, String sort, String grep)
    {
        String action         = "detail";
        String tableDataColor = "#eeeeee";
        String tableColor     = "#dddddd";
        String[] sel  =
            { "PnfsId"      , "i.name",
              "Time"        , "i.start",
              "Pool"        , "i.pool",
              "Status"      , "i.status",
              "Tape"        , "hsm.osm.volumeName",
              "StorageClass", "sclass",
              "Path"        , "path"
            };
        pw.println("<form name=\"input\" action=\""+action+"\" method=\"get\">");
        pw.println("<center><table border=0 cellspacing=2 width=\"95%\" bgcolor=\""+tableColor+"\">");
        pw.println("   <tr>");
        pw.println("      <td align=center bgcolor=\""+tableDataColor+"\">");
        pw.println("         <input type=\"submit\" value=\"Select\" name=\"otto\">");
        pw.println("      </td>");
        pw.println("      <td align=center bgcolor=\""+tableDataColor+"\">");
        pw.println("         Sort by");
        pw.println("      </td>");
        String radio   = "type=\"radio\" name=\"sort\"";
        String checked = "checked=\"checked\"  ";
        for (int i = 0, n = sel.length ; i < n; i+=2) {
            pw.println("      <td align=center bgcolor=\""+tableDataColor+"\">");
            pw.println("         <input "+radio+" id=\""+sel[i]+"\" value=\""+
                       sel[i+1]+"\" "+
                       ((((sort==null)&&(i==0))||
                         ((sort!=null)&&sel[i+1].equals(sort))) ?  checked : "")+"/>");
            pw.println("         <label for=\""+sel[i]+"\">"+sel[i]+"</label>");
            pw.println("      </td>");

        }
        pw.println("      <td align=center bgcolor=\""+tableDataColor+"\">");
        pw.println("         Search <input type=\"text\" value=\""+
                   (grep == null ? "" : grep)
                   +"\" name=\"grep\">");
        pw.println("      </td>");
        pw.println("   </tr>");
        pw.println("</table></center>");

    }

    private void printCssFile(PrintWriter pw, String filename)
    {
        if (filename.equals("test.html")) {
            //
            // table test for css (internal and external)
            //
            pw.println("<html>");
            pw.println("<head>");
            pw.println("<title>Titel der Datei</title>");
            pw.println("<link rel=\"stylesheet\" type=\"text/css\" href=\""+_cssFile+"\">");
            pw.println("</head>");
            pw.println("<body>");
            pw.println("<h1>This is a header - 1 </h1>");
            pw.println("<h2>This is a header - 2 </h2>");
            pw.println("<center>");
            pw.println("<table class=\"s-table\">");
            pw.println("<tr class=\"s-table\"><th class=\"s-table\">Header 1</th><th class=\"s-table\">Header 2</th></tr>");
            pw.println("<tr class=\"s-table-a\"><td class=\"s-table\">entry-1</td><td class=\"s-table\">entry-2</td></tr>");
            pw.println("<tr class=\"s-table-b\"><td class=\"s-table\">entry-3</td><td class=\"s-table\">entry-4</td></tr>");
            pw.println("</table>");
            pw.println("</body>");
            pw.println("</html>");

        } else if (filename.equals("default.css")) {
            printInternalCssFile(pw);
        }
    }

    private void printInternalCssFile(PrintWriter pw)
    {
        pw.println("body { background-color:orange; }");
        pw.println("table.s-table { width:90%; border:1px; border-style:solid; border-spacing:0px; border-collapse:collapse; }");
        pw.println("tr.s-table   { background-color:#115259; color:white; font-size:18; }");
        pw.println("tr.s-table-a { background-color:#bebebe; text-align:center; font-size:16; }");
        pw.println("tr.s-table-b { background-color:#efefef; text-align:center; font-size:16; }");
        pw.println("tr.s-table-e { background-color:red; text-align:center; font-size:16; }");
        pw.println("td.s-table {  border:1px ; border-style:solid; border-spacing:1px; padding:3; }");
        pw.println("th.s-table {  border:1px ; border-style:solid; border-spacing:1px;}");
        pw.println("td.s-table-disabled {  border:1px ; border-style:solid; border-spacing:0px; padding:3;}");
        pw.println("td.s-table-e {  background-color:red;}");
        pw.println("td.s-table-regular  {  border:1px ; border-style:solid; border-spacing:0px; padding:3;}");
        pw.println("span.s-table-disabled { color:gray ; }");
        pw.println("span.s-table-regular  { color:black ; }");
        pw.println("a.s-table:visited  { text-decoration:none; color:blue; }");
        pw.println("a.s-table:link     { text-decoration:none; color:blue; }");
        pw.println("table.m-table { width:90%; border:0px; border-style:none; border-spacing:0px; border-collapse:collapse; }");
        pw.println("td.m-table {  background-color:white; text-align:center; border:1px ; border-style:solid; border-spacing:1px;}");
        pw.println("a.m-table:visited        { text-decoration:none; }");
        pw.println("a.m-table-active:visited { text-decoration:none; color:red; }");
        pw.println("a.m-table:link        { text-decoration:none; }");
        pw.println("a.m-table-active:link { text-decoration:none; color:red; }");
        pw.println("table.l-table { width:90%; color:black; table-layout:auto;");
        pw.println("    border:1px; border-style:none; border-spacing:0px; border-collapse:collapse; }");
        pw.println("tr.l-table { background-color:green; }");
        pw.println("td.l-table { background-color:white; color:black;");
        pw.println("width:10.5%;");
        pw.println("padding:4; text-align:center;");
        pw.println("border:1px; border-style:solid; border-spacing:0px; border-collapse:collapse; }");
        pw.println("span.l-table { font-size:16;}");
        pw.println("a.l-table:visited  { text-decoration:none; color:blue; }");
        pw.println("a.l-table:link     { text-decoration:none; color:blue; }");
        pw.println("table.f-table-a { width:90%; color:black; table-layout:auto; background-color:white;");
        pw.println("border:1px; border-style:solid; border-spacing:0px; border-collapse:collapse;}");
        pw.println("td.f-table-a { text-align:center; padding:10; }");
        pw.println("span.f-table-a { text-align:center; font-size:20px }");
        pw.println("table.f-table-b { width:100%;  color:black; table-layout:auto; ");
        pw.println(" border:1px; border-style:none; border-spacing:0px; border-collapse:collapse; }");
        pw.println("th.f-table-b { width:20%;}");
        pw.println("td.f-table-b { width:20%; background-color:#eeeeee; color:black;");
        pw.println("padding:4; text-align:center; border:1px; border-style:solid; border-spacing:0px; }");
        pw.println("span.m-title { font-size:18; color=red; }");
        pw.println("a.big-link:visited  { text-decoration:none; color:blue; }");
        pw.println("a.big-link:link  { text-decoration:none; color:blue; }");
        pw.println("span.big-link { font-size:24; text-align:center }");
    }

    @Override
    public void queryUrl(HttpRequest request)  throws HttpException
    {
        OutputStream out    = request.getOutputStream();
        PrintWriter pw      = request.getPrintWriter();
        String[]   urlItems = request.getRequestTokens();

        request.printHttpHeader(0);
        _requestCounter ++;
        try {
            if (urlItems.length < 1) {
                return;
            }

            if ((urlItems.length > 1) && (urlItems[1].equals("css"))) {
                //
                // the internal css stuff (if nothing else is specifed)
                //
                if (urlItems.length > 2) {
                    printCssFile(pw, urlItems[2]);
                }
                //
            } else if ((urlItems.length > 1) && (urlItems[1].equals("parameterHandler"))) {
                //
                // the parameter handler (dCache partitioning)
                //
                if (urlItems.length > 2) {
                    //
                    // the paramter set
                    //
                    if (urlItems[2].equals("set")) {
                        printParameter(pw, urlItems.length > 3 ? urlItems[3] : "*");
                    }

                }
            } else if ((urlItems.length > 1) && (urlItems[1].equals("restoreHandler"))) {


                if (urlItems.length > 2) {

                    if (urlItems[2].equals("lazy")) {
                        //
                        //  LAZY retore queue
                        //
                        if ((urlItems.length > 3) &&  urlItems[3].startsWith("detail")) {
                            String sort = request.getParameter(PARAMETER_SORT);
                            String grep = request.getParameter(PARAMETER_GREP);
                            printMenu(pw,  sort, grep);
                            printLazyRestoreInfo(out, sort, grep);
                        } else {
                            //
                            // regular restore queue
                            //
                            printLazyRestoreInfo(out, null,null);
                        }
                    } else if (urlItems[2].startsWith("detail")) {
                        String sort = request.getParameter(PARAMETER_SORT);
                        String grep = request.getParameter(PARAMETER_GREP);
                        printMenu(pw, sort, grep);
                        printRestoreInfo(out, sort, grep);
                    } else {
                        printRestoreInfo(out,  null, null);
                    }
                } else {
                    printRestoreInfo(out,  null, null);
                }

            } else {
                printConfigurationPages(pw, urlItems, request);
            }

        } catch (HttpException httpe) {
            _errorCounter ++;
            throw httpe;
        } catch (Exception e) {
            _errorCounter ++;
            showProblem(pw, e.getMessage());
            pw.println("<ul>");
            for (int i = 0; i < urlItems.length; i++) {
                pw.println("<li> ["+i+"] ");
                pw.println(urlItems[i]);
            }
            pw.println("</ul>");
        } finally {
            pw.println("</body>");
            pw.println("</html>");
        }
    }

    private void printConfigurationHeader(PrintWriter pw)
    {
	pw.println("<html>");
	pw.println("<head>");
        pw.println("<title>PoolManager (Pool SelectionUnit) Configuration</title>");
        pw.println("<link rel=\"stylesheet\" type=\"text/css\" href=\""+_cssFile+"\">");
        pw.println("</head>");

	pw.println("<body class=\"m-body\">");

    }

    private void printConfigurationPages(PrintWriter pw, String[] urlItems, HttpRequest request)
        throws HttpException, NoRouteToCellException, InterruptedException
    {
        printConfigurationHeader(pw);

        printPoolManagerHeader(pw, null);

        if (urlItems.length < 2) {
            showDirectory(pw);
        } else if (urlItems[1].equals("pools")) {
            showDirectory(pw, 1);
            queryPool(pw, urlItems[2]);
        } else if (urlItems[1].equals("units")) {
            showDirectory(pw, 3);
            StringBuilder sb = new StringBuilder();
            int i;
            for (i = 2; i < (urlItems.length-1); i++) {
                sb.append(urlItems[i]).append("/");
            }
            sb.append(urlItems[i]);
            queryUnit(pw, sb.toString());
        } else if (urlItems[1].equals("ugroups")) {
            showDirectory(pw, 4);
            queryUnitGroup(pw, urlItems[2]);
        } else if (urlItems[1].equals("pgroups")) {
            showDirectory(pw, 2);
            queryPoolGroup(pw, urlItems[2]);
        } else if (urlItems[1].equals("links")) {
            showDirectory(pw, 5);
            queryLink(pw, urlItems[2]);
        } else if (urlItems[1].equals("linklist")) {
            showDirectory(pw, 7);
            queryLinkList(pw);
        } else if (urlItems[1].equals("match")) {
            showDirectory(pw, 6);
            showMatch(pw, request);
        } else {
            throw new HttpException(404, "Unknown key : "+urlItems[1]);
        }
    }

    private void printParameter(PrintWriter pw, String key)
    {
        try {
            printConfigurationHeader(pw);
            printPoolManagerHeader(pw, "Partition Manager");
            showDirectory(pw, 0);
            Map<String,Partition> parameterMap = _poolManager.sendAndWait("pmx get map", Map.class);
            if (key.equals("section")) {
                printParameterInSections(pw, parameterMap);
            } else if (key.equals("matrix")) {
                printParameterInMatrix(pw, parameterMap);
            }
        } catch (TimeoutCacheException e) {
            showTimeout(pw);
        } catch (CacheException | InterruptedException e) {
            showProblem(pw, e.getMessage());
        }

        pw.println("<hr><address>Created "+(new Date())+" $Id: HttpPoolMgrEngineV3.java,v 1.26 2007-08-16 20:20:56 behrmann Exp $");
        pw.println("</body></html>");
    }

    private void printParameterInMatrix(PrintWriter pw, Map<String,Partition> parameterMap)
    {
        Partition defaultParas = parameterMap.get("default");
        if (defaultParas == null) {
            return;
        }

        Map<String,Object[]>[] restMap = new Map[parameterMap.size()-1];
        String[] header  = new String[parameterMap.size()-1];
        int row = 0;

        for (Map.Entry<String,Partition> entry : parameterMap.entrySet()) {
            String mapName = entry.getKey();

            if (mapName.equals("default")) {
                continue;
            }

            header[row]  = mapName;
            restMap[row] = entry.getValue().toMap();
            row ++;
        }

        pw.println("<center><table class=\"s-table\"");

        pw.print("<tr class=\"s-table\">");
        pw.print("<th class=\"s-table\">Key</th><th class=\"s-table\">Default</th>");
        for (int m = 0; m < restMap.length; m++) {
            pw.println("<th class=\"s-table\">" + header[m] + "</th>");
        }
        pw.println("</tr>");

        Map<String, Object[]> defaultMap = new TreeMap<>(defaultParas.toMap());
        row = 0;
        String[] setColor = { "s-table-disabled", "s-table-regular"  };
        String[] rowClass = { "s-table-a", "s-table-b" };

        for (Map.Entry<String, Object[]> entry : defaultMap.entrySet()) {
            String key      =  entry.getKey();
            Object[] e      = entry.getValue();
            boolean isSet;
            String value    = e[1].toString();

            //
            // the keys
            //
            pw.print("<tr class=\""+rowClass[row%rowClass.length]+"\">");
            pw.print("<th class=\"s-table\">"); pw.print(key); pw.print("</th>");
            //
            // default values
            //
            pw.print("<td class=\"s-table-regular\">");
            pw.print("<span class=\"s-table-regular\">");
            pw.print(value); pw.println("</span></td>");
            //
            // the other partitions
            //
            for (Map<String, Object[]> aRestMap : restMap) {
                e = aRestMap.get(key);
                isSet = (Boolean) e[0];
                value = e[1].toString();
                pw.print("<td class=\"" + setColor[isSet ? 1 : 0] + "\">");
                pw.print("<span class=\"" + setColor[isSet ? 1 : 0] + "\">");
                pw.println(value);
                pw.println("</span></td>");
            }
            pw.println("</tr>");
            row++;
        }
        pw.println("</table></center>");
    }


    private void printParameterInSections(PrintWriter pw, Map<String,Partition> parameterMap)
    {
        for (Map.Entry<String,Partition> entry : parameterMap.entrySet()) {
            String name = entry.getKey();
            Partition p = entry.getValue();
            pw.println("<h2>"+name+"</h2>");
            printParameterEntry(pw, p);
        }
    }

    private void printParameterEntry(PrintWriter pw, Partition p)
    {
        Map<String, Object[]> map = p.toMap();

        int column = 0;
        int maxColumn = 2;

        pw.println("<center><table width=\"90%\" cellspacing=4 cellpadding=4 bgcolor=yellow>");

        pw.print("<tr>");
        for (int l = 0; l < (maxColumn+1); l ++) {
            pw.print("<th align=center>Key</th><th align=center>Value</th>");
        }
        pw.println("</tr>");

        for (Map.Entry<String, Object[]> entry : map.entrySet()) {
            String     name  = entry.getKey();
            Object[]  array = entry.getValue();
            boolean    isSet = (Boolean)array[0];
            Object     value = array[1].toString();

            if (column == 0) {
                pw.print("<tr>");
            }

            String col = isSet ? "black" : "gray";
            pw.print("<th bgcolor=white>"); pw.print(name); pw.print("</th>");
            pw.print("<td  bgcolor=white align=center ><font color="); pw.print(col) ; pw.print(">");
            pw.print(value); pw.println("</font></td>");

            if (column == maxColumn) {
                pw.print("</tr>");
            }

            column = (column + 1) % (maxColumn+1);
        }

        pw.println("</table></center>");

    }


    private void printLazyRestoreInfo(OutputStream out, String sorting, String grep)
    {
        HTMLWriter html = new HTMLWriter(out, _endpoint.getDomainContext());

        html.addHeader("/styles/restoreHandler.css",
                       "dCache Dataset Restore Monitor (Lazy)");

        html.beginTable("sortable",
                        "pnfs",      "PnfsId",
                        "subnet",    "Subnet",
                        "candidate", "PoolCandidate",
                        "started",   "Started",
                        "clients",   "Clients",
                        "retries",   "Retries",
                        "status",    "Status");

        List<Object[]> copy = new ArrayList<>(_lazyRestoreList);
        Collections.sort(copy, new OurComparator(sorting));
        for (Object[] a: copy) {
            if ((grep == null) || grepOk(grep, a)) {
                showRestoreInfo(html,
                        (RestoreHandlerInfo) a[0],
                        (String) a[1],
                        (StorageInfo) a[2]);
            }
        }
        html.endTable();
        html.addFooter(getClass().getName() + " [$Revision: 1.26 $]");
    }

    private void printRestoreInfo(OutputStream out, String sorting, String grep)
    {
        HTMLWriter html = new HTMLWriter(out, _endpoint.getDomainContext());

        html.addHeader("/styles/restoreHandler.css",
                       "dCache Dataset Restore Monitor");

        try {
            RestoreHandlerInfo[] list = _poolManager.sendAndWait("xrc ls", RestoreHandlerInfo[].class);
            Arrays.sort(list, new OurComparator(sorting));

            html.beginTable("sortable",
                            "pnfs",      "PnfsId",
                            "subnet",    "Subnet",
                            "candidate", "PoolCandidate",
                            "started",   "Started",
                            "clients",   "Clients",
                            "retries",   "Retries",
                            "status",    "Status");

            for (RestoreHandlerInfo info : list) {
                if ((grep == null) || grepOk(grep, info)) {
                    showRestoreInfo(html, info);
                }
            }
            html.endTable();
        } catch (TimeoutCacheException e) {
            showTimeout(html);
        } catch (InterruptedException | CacheException e) {
            showProblem(html, e.getMessage());
        }

        html.addFooter(getClass().getName() + " [$Revision: 1.26 $]");
    }

    private boolean grepOk(String grep, Object o)
    {
        RestoreHandlerInfo info;
        if (o instanceof RestoreHandlerInfo) {
            info = (RestoreHandlerInfo)o;
        } else if (o instanceof Object[]) {
            info = (RestoreHandlerInfo)((Object[])o)[0];
        } else {
            return true;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(info.getName()).append(info.getPool()).append(info.getStartTime()).append(info.getStatus());
        Object er = info.getErrorMessage();
        if (er != null) {
            sb.append(er.toString());
        }
        if (sb.indexOf(grep) > -1) {
            return true;
        }

        if (!(o instanceof Object[])) {
            return false;
        }
        Object[] a = (Object[])o;
        if ((a[1] != null) && (a[1].toString().contains(grep))) {
            return true;
        }

        StorageInfo si = (StorageInfo)a[2];
        if (si == null) {
            return false;
        }

        return si.toString().contains(grep);

    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    private class OurComparator implements Comparator<Object>
    {
        private final String _type;

        private OurComparator(String type)
        {
            _type = type;
        }

        @Override
        public int compare(Object o1, Object o2)
        {
            if (o1 instanceof RestoreHandlerInfo) {
                return compareInfo((RestoreHandlerInfo) o1, (RestoreHandlerInfo) o2);
            }
            if (o1 instanceof Object[]) {
                return compareArray((Object[]) o1, (Object[]) o2);
            }
            return 0;
        }

        private int compareInfo(RestoreHandlerInfo i1, RestoreHandlerInfo i2)
        {
            if (_type == null) {
                return i1.getName().compareTo(i2.getName());
            } else if (_type.equals("i.name")) {
                return i1.getName().compareTo(i2.getName());
            } else if (_type.equals("i.error")) {
                return (""+i1.getErrorCode()+i1.getName()).
                    compareTo(""+i2.getErrorCode()+i2.getName());
            } else if (_type.equals("i.status")) {
                return i1.getStatus().compareTo(i2.getStatus());
            } else if (_type.equals("i.pool")) {
                String a = i1.getPool();
                String b = i2.getPool();
                if ((a == null) || (b == null)) {
                    return a == null ? -1 : b == null ? 1 : 0;
                }

                return a.compareTo(b);
            } else if (_type.equals("i.start")) {
                return Long.valueOf(i1.getStartTime()).compareTo(i2
                        .getStartTime());
            } else {
                return i1.getName().compareTo(i2.getName());
            }
        }

        private int compareArray(Object[] o1, Object[] o2)
        {
            if (_type == null || _type.startsWith("i.")) {
                return compareInfo((RestoreHandlerInfo) o1[0], (RestoreHandlerInfo) o2[0]);
            }

            if (_type.equals("path")) {
                return o1[1].toString().compareTo(o2[1].toString());
            }


            return compareStorageInfo((StorageInfo) o1[2], (StorageInfo) o2[2]);
        }

        private int compareStorageInfo(StorageInfo s1, StorageInfo s2)
        {
            if (_type.equals("sclass")) {
                return s1.getStorageClass().compareTo(s2.getStorageClass());
            }

            String k1 = s1.getKey(_type);
            String k2 = s2.getKey(_type);
            if ((k1 == null) || (k2 == null)) {
                return k1 == null ? -1 : k2 == null ? 1 : 0;
            }

            return  k1.compareTo(k2);
        }

    }

    private void showRestoreInfo(HTMLWriter html, RestoreHandlerInfo info)
    {
        showRestoreInfo(html, info, null, null);
    }

    private void showRestoreInfo(HTMLWriter html,
                                 RestoreHandlerInfo info,
                                 String path,
                                 StorageInfo storageInfo)
    {
        String  name   = info.getName();
        int     pos    = name.indexOf('@');
        String  pnfsId = name.substring(0,pos);
        String  subnet = name.substring(pos+1);
        int     rc     = info.getErrorCode();
        String  msg    = info.getErrorMessage();
        String  started= _formatter.format(new Date(info.getStartTime()));

        boolean   error      = (rc != 0) || ((msg != null) && (!msg.equals("")));

        String  pool = info.getPool();
        pool = (pool == null) || (pool.equals("") || pool.equals("<unknown>")) ? "N.N." : pool;
        String status = info.getStatus();
        status = (status == null) || (status.equals("")) ? "&nbsp;" : status;
        if (error) {
            html.beginRow("error", "error odd");
        } else {
            html.beginRow(null, "odd");
        }
        html.td("pnfs",    pnfsId);
        html.td("subnet",  subnet);
        html.td("pool",    pool);
        html.td("started", started);
        html.td("clients", info.getClientCount());
        html.td("retries", info.getRetryCount());
        html.td("status",  status);
        if (path != null) {
            html.endRow(false);
            if (error) {
                html.beginRow("error", "error odd");
            } else {
                html.beginRow(null, "odd");
            }
            html.td(7, "path", path);
        }
        if (storageInfo != null) {
            html.endRow(false);
            if (error) {
                html.beginRow("error", "error odd");
            } else {
                html.beginRow(null, "odd");
            }
            if (_siDetails != null) {
                StringBuilder builder = new StringBuilder();
                for (String key : _siDetails) {
                    String value;
                    if ((value = storageInfo.getKey(key)) != null) {
                        builder.append(key).append("=").append(value)
                                .append(";");
                    }
                }
                html.td(7, "storageinfo", builder);
            } else {
                html.td(7, "storageinfo", storageInfo.toString());
            }
        }
        if (error) {
            html.endRow(false);
            if (error) {
                html.beginRow("error", "error odd");
            } else {
                html.beginRow(null, "odd");
            }
            html.td(7, "error", "Code = " + rc + "; Message = " + msg);
        }
        html.endRow();
    }

    private void printPoolManagerHeader(PrintWriter pw, String title)
    {
        pw.println("<table class=\"m-table\" border=0 cellpadding=10 cellspacing=0 width=\"90%\">");
        pw.println("<tr><td align=center valign=center width=\"1%\">");
        pw.println("<a href=\"/\"><img border=0 src=\"/images/eagleredtrans.gif\"></a>");
        pw.println("<br><font color=red>Birds Home</font>");
        pw.println("</td><td align=center>");
        if (title != null) {
            pw.println("<h1>" + title + "</h1>");
        } else {
            pw.println("<h1>Pool Manager Database V3</h1>");
        }
        pw.println("</td></tr></table>");

    }

    private void showDirectory(PrintWriter pw)
    {
        showDirectory(pw, -1);
    }

    private void showDirectory(PrintWriter pw, int position)
    {
        pw.println("<br><center><table class=\"m-table\">");
        pw.println("<tr class=\"m-table\">");

        printDirEntry(pw, "Partitions" , position == 0, "/poolInfo/parameterHandler/set/matrix/*");
        printDirEntry(pw, "Pools"      , position == 1, "/poolInfo/pools/*");
        printDirEntry(pw, "Pool Groups", position == 2, "/poolInfo/pgroups/*");
        printDirEntry(pw, "Selection"  , position == 3, "/poolInfo/units/*");

        pw.println("</tr><tr>");

        printDirEntry(pw, "Selection Groups", position == 4, "/poolInfo/ugroups/*");
        printDirEntry(pw, "Links"           , position == 5, "/poolInfo/links/*");
        printDirEntry(pw, "Link List"       , position == 7, "/poolInfo/linklist/*");
        printDirEntry(pw, "Match"           , position == 6, "/poolInfo/match/*");

        pw.println("</tr></table></center>");
        pw.println("<br><hr><br>");

    }

    private void printDirEntry(PrintWriter pw, String text, boolean inUse, String link)
    {
        String alternateClass=inUse?"class=\"m-table-active\"":"class=\"m-table\"";
        pw.print("<td width=\"25%\" class=\"m-table\"><span ");
        pw.print(alternateClass);
        pw.print("><a ");
        pw.print(alternateClass);
        pw.print(" href=\"");
        pw.print(link);
        pw.print("\">");
        pw.print(text);
        pw.println("</a></span></td>");
    }

    private void showList(PrintWriter pw, Object[] array, int rows)
    {
        showList(pw, array, rows, null);
    }

    private void showMatch(PrintWriter pw, HttpRequest request)
        throws NoRouteToCellException, InterruptedException
    {
        String type   = request.getParameter(PARAMETER_TYPE);
        String store  = request.getParameter(PARAMETER_STORE);
        String dcache = request.getParameter(PARAMETER_DCACHE);
        String net    = request.getParameter(PARAMETER_NET);
        String prot   = request.getParameter(PARAMETER_PROTOCOL);
        String linkGroup = request.getParameter(PARAMETER_LINKGROUP);

        linkGroup  = (linkGroup  == null) || (linkGroup.length() == 0) ? "none" : linkGroup;
        store  = (store  == null) || (store.length() == 0) ? "*" : store;
        dcache = (dcache == null) || (dcache.length() == 0) ? "*" : dcache;
        net    = (net    == null) || (net.length() == 0) ? "*" : net;
        prot   = (prot   == null) || (prot.length() == 0) ? "*" : prot;
        pw.println("<center>");
        if (type == null) {
            showQueryForm(pw, "none", "read", "*", "*", "*", "DCap/3");
        } else {
            showQueryForm(pw, linkGroup, type, store, dcache, net, prot);
            pw.println("<p><hr><p>");

            try {
                PoolPreferenceLevel[] result =
                        _poolManager.sendAndWait("psux match " + type + " "
                                                         + store + " " + dcache + " " + net + " " + prot
                                                         + (linkGroup.equals("none") ? "" : " -linkGroup="+linkGroup),
                                                 PoolPreferenceLevel[].class);
                for (int i = 0; i < result.length; i++) {
                    pw.print("<p><h2>Selected Pools with attraction "+i);
                    String tag = result[i].getTag();
                    if (tag != null) {
                        pw.print(" (dCache subsection=" + tag + ")");
                    }
                    pw.println("</h2>");
                    showList(pw, result[i].getPoolList().toArray(), 8, "/poolInfo/pools/");
                }
            } catch (TimeoutCacheException e) {
                showTimeout(pw);
            } catch (CacheException e) {
                showProblem(pw, e.getMessage());
            }
        }
        pw.println("</center>");
    }

    private String makeLink(String link)
    {
        StringBuilder sb = new StringBuilder();
        for (int i= 0, n = link.length(); i < n; i ++) {
            char c = link.charAt(i);
            switch(c) {
            case ':' :
                sb.append("%3A");
                break;
            default :
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private void showList(PrintWriter pw, Object[] array, int rows, String link)
    {
        pw.println("<table class=\"l-table\"");
        Arrays.sort(array);
        for (int i= 0; i < array.length; i++) {
            if ((i % rows) == 0) {
                pw.println("<tr class=\"l-table\">");
            }
            pw.print("<td class=\"l-table\" ");
            pw.print("><a class=\"l-table\"  href=\"");
            if (link != null) {
                pw.print(link);
            }
            pw.print(makeLink(array[i].toString()));
            pw.print("\"><span class=\"l-table\">");
            pw.println(array[i].toString());
            pw.println("</span>");
            pw.println("</a>");
            pw.println("</td>");
            if ((i % rows) == (rows - 1)) {
                pw.println("</tr>");
            }
        }
        int rest = rows - (array.length % rows);
        if (rest < rows) {
            for (int i = 0; i < rest; i++) {
                pw.print("<td  class=\"l-table\"><span class=\"l-table\">-</span></td>");
            }
            pw.println("</tr>");
        }
        pw.println("</table>");
    }

    private void showQueryForm(PrintWriter pw,
                               String linkGroup,
                               String type,
                               String store,
                               String dcache,
                               String net,
                               String protocol)
    {
        pw.println("<table class=\"f-table-a\">");
        pw.println("<tr class=\"f-table-a\">");
        pw.println("<td class=\"f-table-a\">");
        pw.println("<span  class=\"f-table-a\">Simulated I/O Request</span>");
        pw.println("</td></tr><tr><td class=\"f-table-a\">");
        pw.println("<form method=get action=\"/poolInfo/match/match\">");
        pw.println("<table class=\"f-table-b\">");
        pw.println("<tr class=\"f-table-b\">");
        pw.println("<th class=\"f-table-b\">LinkGroup</th>");
        pw.println("<th class=\"f-table-b\">I/O Direction</th>");
        pw.println("<th class=\"f-table-b\">Store</th>");
        pw.println("<th class=\"f-table-b\">dCache</th>");
        pw.println("<th class=\"f-table-b\">Net</th>");
        pw.println("<th class=\"f-table-b\">Protocol</th>");
        pw.println("</tr><tr>");

        pw.println("<td class=\"f-table-b\"><input name=linkGroup value=\""+linkGroup+"\"></td>");

        pw.println("<td class=\"f-table-b\">");
        pw.println("<select name=type>");
        pw.println("<option value=read  "+(type.equals("read")?"selected":"")+">Read");
        pw.println("<option value=p2p   "+(type.equals("p2p")?"selected":"")+">Pool 2 Pool");
        pw.println("<option value=cache "+(type.equals("cache")?"selected":"")+">Cache");
        pw.println("<option value=write "+(type.equals("write")?"selected":"")+">Write</select>");
        pw.println("</td>");

        pw.println("<td class=\"f-table-b\"><input name=store value=\""+store+"\"></td>");
        pw.println("<td class=\"f-table-b\"><input name=dcache value=\""+dcache+"\"></td>");
        pw.println("<td class=\"f-table-b\"><input name=net value=\""+net+"\"></td>");
        pw.println("<td class=\"f-table-b\"><input name=protocol value=\""+protocol+"\"></td>");
        pw.println("</tr></table>");
        pw.println("</td></tr><td  class=\"f-table-a\">");
        pw.println("<input type=submit value=\"Send Query\">");
        pw.println("</form>");
        pw.println("</td></tr>");
        pw.println("</table>");
    }

    private void queryAll(PrintWriter pw, String request, String type)
        throws NoRouteToCellException, InterruptedException
    {
        try {
            Object[] o = _poolManager.sendAndWait(request, Object[].class);
            pw.println("<center><h1>"+type+"</h1>");
            showList(pw, o, 8);
            pw.println("</center>");
        } catch (TimeoutCacheException e) {
            showTimeout(pw);
        } catch (CacheException e) {
            showProblem(pw, e.getMessage());
        }
    }

    private void queryAllPools(PrintWriter pw)
        throws NoRouteToCellException, InterruptedException
    {
        queryAll(pw, "psux ls pool", "Registered Pools");
    }

    private void queryAllPGroups(PrintWriter pw)
        throws NoRouteToCellException, InterruptedException
    {
        queryAll(pw, "psux ls pgroup", "Registered Pool Groups");
    }

    private void queryAllUnits(PrintWriter pw)
        throws NoRouteToCellException, InterruptedException
    {
        queryAll(pw, "psux ls unit", "Registered Units");
    }

    private void queryAllUGroups(PrintWriter pw)
        throws NoRouteToCellException, InterruptedException
    {
        queryAll(pw, "psux ls ugroup", "Registered Unit Groups");
    }

    private void queryAllLinks(PrintWriter pw)
        throws NoRouteToCellException, InterruptedException
    {
        queryAll(pw, "psux ls link", "Registered Links");
    }

    private void queryPool(PrintWriter pw, String poolName)
        throws NoRouteToCellException, InterruptedException
    {
        if ((poolName.equals("")) || (poolName.equals("*"))) {
            queryAllPools(pw);
            return;
        }
        pw.println("<center>");
        pw.println("<h1>Report for Pool <font color=red>"+poolName+"</font></h1>");
        try {
            Object[] o = _poolManager.sendAndWait("psux ls pool " + poolName, Object[].class);
            Object[] groupList = (Object[])o[1];
            Object[] linkList  = (Object[])o[2];
            boolean   enabled   = (Boolean)o[3];
            boolean   rdOnly    = (Boolean)o[5];
            long      active    = (Long)o[4];
            pw.println("<table border=0 cellspacing=4 cellpadding=4>");
            pw.print("<tr><th align=right>Enabled : </th><td align=left>");
            pw.print(enabled ? "Yes" : "No");
            pw.println("</td></tr>");
            pw.print("<tr><th align=right>Mode : </th><td align=left>");
            pw.print(rdOnly ? "Read Only" : "Read/Write");
            pw.println("</td></tr>");
            pw.print("<tr><th align=right>Active : </th><td align=left>");
            pw.print((active < (60 * 1000)) ? "Yes" : "No");
            pw.println("</td></tr></table>");
            pw.println("<h3>We are member of the following pool groups</h3>");
            showList(pw, groupList, 8, "/poolInfo/pgroups/");
            pw.println("<h3>We are pointing to the following Links</h3>");
            showList(pw, linkList, 8, "/poolInfo/links/");
        } catch (TimeoutCacheException e) {
            showTimeout(pw);
        } catch (CacheException e) {
            showProblem(pw, e.getMessage());
        }
        pw.println("</center>");
    }

    private void queryUnit(PrintWriter pw, String unitName)
        throws NoRouteToCellException, InterruptedException
    {
        if ((unitName.equals("")) || (unitName.equals("*"))) {
            queryAllUnits(pw);
            return;
        }
        pw.println("<center>");
        pw.println("<h1>Report for Unit <font color=red>"+unitName+"</font></h1>");
        try {
            Object[] o = _poolManager.sendAndWait("psux ls unit "+unitName, Object[].class);

            String   type       = o[1].toString();
            Object[] groupList = (Object[])o[2];

            pw.println("<table border=0 cellspacing=4 cellpadding=4>");
            pw.print("<tr><th align=right>Selection Unit : </th><td align=left>");
            pw.print(unitName);
            pw.println("</td></tr>");
            pw.print("<tr><th align=right>Selection Type : </th><td align=left>");
            pw.print(type);
            pw.println("</td></tr>");
            pw.print("</table>");
            pw.println("<h3>We are member of the following Selection Unit Groups</h3>");
            showList(pw, groupList, 8, "/poolInfo/ugroups/");
        } catch (TimeoutCacheException e) {
            showTimeout(pw);
        } catch (CacheException e) {
            showProblem(pw, e.getMessage());
        }
        pw.println("</center>");
    }

    private class LinkProperties implements Comparable<LinkProperties>
    {
        private String name;
        private int    readPref;
        private int    writePref;
        private int    cachePref;
        private int    p2pPref;
        private Object[] groupList;
        private Object[] poolList;
        private Object[] pGroupList;
        private String tag;
        private LinkProperties(Object[] prop)
        {
            extractLinkProperties(this, prop);
        }
        private LinkProperties extractLinkProperties(LinkProperties p, Object[] prop)
        {

            p.name      = prop[0].toString();
            p.readPref  = (Integer) prop[1];
            p.cachePref = (Integer) prop[2];
            p.writePref = (Integer) prop[3];
            p.p2pPref   = (Integer) prop[7];
            p.tag       =  prop[8] == null ? "NONE" : prop[8].toString();
            p.groupList   = (Object[])prop[4];
            p.poolList    = (Object[])prop[5];
            p.pGroupList  = (Object[])prop[6];

            return p;
        }

        @Override
        public int compareTo(LinkProperties link )
        {
            if (link.tag.equals(tag)) {
                return name.compareTo(link.name);
            }
            if (tag.equals("NONE")) {
                return -1;
            }
            return tag.compareTo(link.tag);
        }

        @Override
        public String toString() { return "["+tag+"/"+name+"]"; }
    }

    private void queryLinkList(PrintWriter pw)
        throws NoRouteToCellException, InterruptedException
    {

        try {
            List<Object[]> answer = _poolManager.sendAndWait("psux ls link -x", List.class);
            List<LinkProperties> list = new ArrayList<>();
            for (Object[] o : answer) {
                list.add(new LinkProperties(o));
            }
            Collections.sort(list);

            pw.println("<center><table class=\"s-table\">");
            pw.println("<tr  class=\"s-table\">");
            pw.print("<th rowspan=2 class=\"s-table\">Name</th>");
            pw.print("<th rowspan=2 class=\"s-table\">Partition</th>");
            pw.print("<th colspan=4 class=\"s-table\">Preferences</th>");
            pw.print("<th colspan=4 rowspan=2 class=\"s-table\">Unit Groups</th>");
            pw.print("<th rowspan=2 class=\"s-table\">Pool Groups</th>");
            pw.print("<th rowspan=2 class=\"s-table\">Pools</th>");
            pw.println("</tr>");
            pw.println("<tr class=\"s-table\">");
            pw.print("<th class=\"s-table\">Read</th><th class=\"s-table\">Write</font></th>");
            pw.print("<th class=\"s-table\">Cache</th><th class=\"s-table\">P2p</font></th>");
            pw.println("</tr>");
            int row = 0;
            String[] rowColor = { "s-table-a", "s-table-b" };

            for (LinkProperties lp : list) {
                pw.println("<tr class=\""+rowColor[row%rowColor.length]+"\">");
                printLinkPropertyRow(pw, lp);
                pw.println("</tr>");
                row++;
            }

            pw.println("</table></center>");
        } catch (TimeoutCacheException e) {
            showTimeout(pw);
        } catch (CacheException e) {
            showProblem(pw, e.getMessage());
        }
    }

    private void printLinkPropertyRow(PrintWriter pw, LinkProperties lp)
    {
        String tableData = "<td class=\"s-table\"><span class=\"s-table\">";
        String dataEnd   = "</span></td>";
        pw.print(tableData);
        pw.print("<a class=\"s-table\" href=\"/poolInfo/links/");
        pw.print(lp.name);
        pw.print("\">");
        pw.print(lp.name);
        pw.println("</a>"+dataEnd);
        pw.print(tableData); pw.print(lp.tag); pw.println(dataEnd);
        pw.print(tableData); pw.print(lp.readPref); pw.println(dataEnd);
        pw.print(tableData); pw.print(lp.writePref); pw.println(dataEnd);
        pw.print(tableData); pw.print(lp.cachePref); pw.println(dataEnd);
        if (lp.p2pPref < 0) {
            pw.print(tableData); pw.print("("+lp.readPref+")"); pw.println(dataEnd);
        } else {
            pw.print(tableData); pw.print(lp.p2pPref); pw.println(dataEnd);
        }
        int i = 0, n;
        for (n = lp.groupList == null ? 0 : lp.groupList.length; i < n; i++) {
            String unitName  = lp.groupList[i].toString();
            pw.print(tableData); pw.print(unitName); pw.println(dataEnd);
        }
        for (; i < 4; i++) {
            pw.print(tableData); pw.print("-"); pw.println(dataEnd);
        }
        StringBuilder sb = new StringBuilder();
        if (lp.pGroupList != null) {
            for (i = 0, n = lp.pGroupList.length; i < n; i++) {
                sb.append(lp.pGroupList[i]);
                if (i < (n - 1)) {
                    sb.append(",");
                }
            }
        }
        String out = sb.length() == 0 ? "-" : sb.toString();

        pw.print(tableData); pw.print(out); pw.println(dataEnd);
        sb = new StringBuilder();
        if (lp.poolList != null) {
            for (i = 0, n = lp.poolList.length; i < n; i++) {
                sb.append(lp.poolList[i]);
                if (i < (n - 1)) {
                    sb.append(",");
                }
            }
        }
        out = sb.length() == 0 ? "-" : sb.toString();
        pw.print(tableData); pw.print(out); pw.println(dataEnd);
    }

    private void queryLink(PrintWriter pw, String linkName)
        throws NoRouteToCellException, InterruptedException
    {
        if ((linkName.equals("")) || (linkName.equals("*"))) {
            queryAllLinks(pw);
            return;
        }
        try {
            Object[] answer = _poolManager.sendAndWait("psux ls link " + linkName, Object[].class);
            LinkProperties lp = new LinkProperties(answer);

            pw.println("<center>");
            pw.println("<h1>Report for Link <font color=red>"+linkName+"</font></h1>");
            pw.println("<h3>Link Properties</h3>");

            pw.println("<table class=\"s-table\" id=\"linkproperties\">");
            pw.println("<tr class=\"s-table\">");
            pw.println("<th class=\"s-table\" colspan=4>Preferences</td>");
            pw.println("<th rowspan=2 align=center class=\"s-table\">dCache Partition</td></tr>");
            pw.println("<tr class=\"s-table\">");
            pw.println("<th class=\"s-table\">Read</th>");
            pw.println("<th class=\"s-table\">Write</th>");
            pw.println("<th class=\"s-table\">Restore</th>");
            pw.println("<th class=\"s-table\">Pool to Pool</th>");
            pw.println("</tr>");
            pw.print("<tr  class=\"s-table-b\">");
            pw.print("<td class=\"s-table\"><span class=\"s-table\">");
            pw.print(lp.readPref);
            pw.println("</span></td>");
            pw.print("<td class=\"s-table\"><span class=\"s-table\">");
            pw.print(lp.writePref);
            pw.println("</span></td>");
            pw.print("<td class=\"s-table\"><span class=\"s-table\">");
            pw.print(lp.cachePref);
            pw.println("</span></td>");
            pw.print("<td class=\"s-table\"><span class=\"s-table\">");
            pw.print(lp.p2pPref);
            pw.println("</span></td>");
            pw.print("<td class=\"s-table\"><span class=\"s-table\">");
            pw.print(lp.tag);
            pw.println("</span></td>");
            pw.print("<tr>");

            pw.println("</table>");


            if (lp.poolList.length > 0) {
                pw.println("<h3>We point the following Pools</h3>");
                showList(pw, lp.poolList, 8, "/poolInfo/pools/");
            }
            pw.println("<h3>We point to the following Pool Groups</h3>");
            showList(pw, lp.pGroupList, 8, "/poolInfo/pgroups/");
            pw.println("<h3>We point to the following Selection Unit Groups</h3>");
            showList(pw, lp.groupList, 8, "/poolInfo/ugroups/");
            pw.println("</center>");
        } catch (TimeoutCacheException e) {
            showTimeout(pw);
        } catch (CacheException e) {
            showProblem(pw, e.getMessage());
        }
    }

    private void queryUnitGroup(PrintWriter pw, String groupName)
        throws NoRouteToCellException, InterruptedException
    {
        if ((groupName.equals("")) || (groupName.equals("*"))) {
            queryAllUGroups(pw);
            return;
        }
        pw.println("<center>");
        pw.println("<h1>Report for Unit Group <font color=red>"+groupName+"</font></h1>");
        try {
            Object[] o = _poolManager.sendAndWait("psux ls ugroup "+groupName, Object[].class);
            Object[] unitList   = (Object[])o[1];
            Object[] linkList   = (Object[])o[2];

            pw.println("<table border=0 cellspacing=4 cellpadding=4>");
            pw.print("<tr><th align=right>Unit Group : </th><td align=left>");
            pw.print(groupName);
            pw.println("</td></tr>");
            pw.print("</table>");

            pw.println("<h3>We have the following Members</h3>");
            showList(pw, unitList, 8, "/poolInfo/units/");

            pw.println("<h3>We are pointing to the following links</h3>");
            showList(pw, linkList, 8, "/poolInfo/links/");
        } catch (TimeoutCacheException e) {
            showTimeout(pw);
        } catch (CacheException e) {
            showProblem(pw, e.getMessage());
        }
        pw.println("</center>");
    }

    private void queryPoolGroup(PrintWriter pw, String groupName)
        throws NoRouteToCellException, InterruptedException
    {
        if ((groupName.equals("")) || (groupName.equals("*"))) {
            queryAllPGroups(pw);
            return;
        }
        pw.println("<center>");
        pw.println("<h1>Report for Pool Group <font color=red>"+groupName+"</font></h1>");
        try {
            Object[] o = _poolManager.sendAndWait("psux ls pgroup "+groupName, Object[].class);
            Object[] poolList   = (Object[])o[1];
            Object[] linkList   = (Object[])o[2];

            pw.println("<table border=0 cellspacing=4 cellpadding=4>");
            pw.print("<tr><th align=right>Pool Group : </th><td align=left>");
            pw.print(groupName);
            pw.println("</td></tr>");
            pw.print("</table>");

            pw.println("<h3>We have the following Members</h3>");
            showList(pw, poolList, 8, "/poolInfo/pools/");

            pw.println("<h3>We are pointing to the following links</h3>");
            showList(pw, linkList, 8, "/poolInfo/links/");
        } catch (TimeoutCacheException e) {
            showTimeout(pw);
        } catch (CacheException e) {
            showProblem(pw, e.getMessage());
        }
        pw.println("</center>");
    }

    private void showTimeout(PrintWriter pw)
    {
        pw.println("<font color=red><h1>Sorry, the request timed out</h1></font>");
    }

    private void showProblem(PrintWriter pw, String message)
    {
        pw.print("<font color=red><h1>");
        pw.print(message);
        pw.println("</h1></font>");
    }
}
