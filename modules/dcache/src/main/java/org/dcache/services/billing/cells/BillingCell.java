package org.dcache.services.billing.cells;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import diskCacheV111.cells.DateRenderer;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PnfsFileInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Args;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellStub;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;

/**
 * This class is responsible for the processing of messages from other
 * domains regarding transfers and pool usage.
 */
public final class BillingCell
    implements CellMessageReceiver,
               CellCommandListener,
               CellInfoProvider,
               EnvironmentAware
{
    private static final Logger _log =
        LoggerFactory.getLogger(BillingCell.class);
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final SimpleDateFormat _formatter =
        new SimpleDateFormat ("MM.dd HH:mm:ss");
    private final SimpleDateFormat _fileNameFormat =
        new SimpleDateFormat("yyyy.MM.dd");
    private final SimpleDateFormat _directoryNameFormat =
        new SimpleDateFormat("yyyy" + File.separator + "MM");

    private final STGroup _templateGroup = new STGroup('$', '$');

    private final Map<String,int[]> _map = Maps.newHashMap();
    private final Map<String,long[]> _poolStatistics = Maps.newHashMap();
    private final Map<String,Map<String,long[]>> _poolStorageMap = Maps.newHashMap();

    private int _requests;
    private int _failed;
    private File _currentDbFile;

    /*
     * Injected
     */
    private Map<String,Object> _environment;
    private CellStub _poolManagerStub;
    private File _logsDir;
    private int _printMode;
    private boolean _disableTxt;

    public BillingCell()
    {
        _templateGroup.registerRenderer(Date.class, new DateRenderer());
    }

    @Override
    public void setEnvironment(Map<String,Object> environment) {
        _environment = environment;
    }

    @Override
    public String toString() {
        return "Req=" + _requests + ";Err=" + _failed + ";";
    }

    @Override
    public CellInfo getCellInfo(CellInfo info) {
        return info;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.format("%20s : %6d / %d\n", "Requests", _requests, _failed);
        for (Map.Entry<String,int[]> entry: _map.entrySet()) {
            int[] values = entry.getValue();
            pw.format("%20s : %6d / %d\n",
                      entry.getKey(), values[0], values[1]);
        }
    }

    /**
     * The main cell routine. Depending on the type of cell message and the
     * option sets, it either processes the message for persistent storage or
     * logs the message to a text file (or both).
     *
     * @param msg
     */
    public void messageArrived(InfoMessage info) {
        /*
         * currently we have to ignore 'check'
         */
        if (info.getMessageType().equals("check")) {
            return;
        }

        updateMap(info);

        if (info.getCellType().equals("pool")) {
            doStatistics(info);
        }

        String output = getFormattedMessage(info);
        if (output.isEmpty()) {
            return;
        }
        _log.info(output);

        if (!_disableTxt) {
            String ext = getFilenameExtension(new Date(info.getTimestamp()));
            logInfo(output, ext);
            if (info.getResultCode() != 0) {
                logError(output, ext);
            }
        }
    }

    public void messageArrived(Object msg) {
        Date now = new Date();
        String output = _formatter.format(now) + " " + msg.toString();

        _log.info(output);

        if (!_disableTxt) {
            logInfo(output, getFilenameExtension(now));
        }
    }

    private String getFormattedMessage(InfoMessage msg) {
        String property = "billing.format." + msg.getClass().getSimpleName();
        Object format = _environment.get(property);
        if (format == null) {
            return msg.toString();
        } else {
            ST template = new ST(_templateGroup, format.toString());
            msg.fillTemplate(template);
            return template.render();
        }
    }

    private final static Function<Map.Entry<String,int[]>,Object[]> toPair =
        new Function<Map.Entry<String,int[]>,Object[]>()
        {
            @Override
            public Object[] apply(Map.Entry<String,int[]> entry) {
                return new Object[] { entry.getKey(),
                                      Arrays.copyOf(entry.getValue(), 2) };
            }
        };

    public Object[][] ac_get_billing_info(Args args) {
        return toArray(transform(_map.entrySet(), toPair), Object[].class);
    }

    public static final String hh_get_pool_statistics = "[<poolName>]";
    public Map<String,long[]> ac_get_pool_statistics_$_0_1(Args args) {
        if (args.argc() == 0) {
            return _poolStatistics;
        }
        Map<String,long[]> map = _poolStorageMap.get(args.argv(0));
        if (map != null) {
            return map;
        }
        return Maps.newHashMap();
    }

    public static final String hh_clear_pool_statistics = "";
    public String ac_clear_pool_statistics(Args args) {
        _poolStatistics.clear();
        _poolStorageMap.clear();
        return "";
    }

    public static final String hh_dump_pool_statistics = "[<fileName>]";
    public String ac_dump_pool_statistics_$_0_1(Args args)
        throws IOException
    {
        dumpPoolStatistics((args.argc() == 0) ? null : args.argv(0));
        return "";
    }

    public static final String hh_get_poolstatus = "[<fileName>]";
    public String ac_get_poolstatus_$_0_1(Args args) {
        String name;
        if (args.argc() == 0) {
            name = "poolStatus-" + _fileNameFormat.format(new Date());
        } else {
            name = args.argv(0);
        }
        File file = new File(_logsDir, name);
        PoolStatusCollector collector =
            new PoolStatusCollector(_poolManagerStub, file);
        collector.setName(name);
        collector.start();
        return file.toString();
    }

    private void dumpPoolStatistics(String name)
        throws IOException
    {
        if (name == null) {
            name = "poolFlow-" + _fileNameFormat.format(new Date());
        }
        File report = new File(_logsDir, name);
        try (PrintWriter pw = new PrintWriter(Files.newWriter(report, UTF8))) {
            Set<Map.Entry<String, Map<String, long[]>>> pools =
                    _poolStorageMap.entrySet();

            for (Map.Entry<String, Map<String, long[]>> poolEntry : pools) {
                String poolName = poolEntry.getKey();
                Map<String, long[]> map = poolEntry.getValue();

                for (Map.Entry<String, long[]> entry : map.entrySet()) {
                    String className = entry.getKey();
                    long[] counters = entry.getValue();
                    pw.print(poolName);
                    pw.print("  ");
                    pw.print(className);
                    for (long counter : counters) {
                        pw.print("  " + counter);
                    }
                    pw.println("");
                }
            }
        } catch (RuntimeException e) {
            _log.warn("Exception in dumpPoolStatistics : {}", e);
            if (!report.delete()) {
                _log.warn("Could not delete report: {}", report);
            }
            throw e;
        }

    }

    private void updateMap(InfoMessage info) {
        String key = info.getMessageType() + ":" + info.getCellType();
        int[] values = _map.get(key);

        if (values == null) {
            values = new int[2];
            _map.put(key, values);
        }

        values[0]++;
        _requests++;

        if (info.getResultCode() != 0) {
            _failed++;
            values[1]++;
        }
    }

    private String getFilenameExtension(Date dateOfEvent)
    {
        if (_printMode == 0) {
            _currentDbFile = _logsDir;
            return _fileNameFormat.format(dateOfEvent);
        } else {
            Date now = new Date();
            _currentDbFile =
                new File(_logsDir, _directoryNameFormat.format(now));
            if (!_currentDbFile.exists() && !_currentDbFile.mkdirs()) {
                _log.error("Failed to create directory {}", _currentDbFile);
            }
            return _fileNameFormat.format(now);
        }
    }

    private void logInfo(String output, String ext)
    {
        File outputFile = new File(_currentDbFile, "billing-" + ext);
        try {
            Files.append(output + "\n", outputFile, UTF8);
        } catch (IOException e) {
            _log.warn("Can't write billing [{}] : {}", outputFile,
                      e.toString());
        }
    }

    private void logError(String output, String ext)
    {
        File errorFile = new File(_currentDbFile, "billing-error-" + ext);
        try {
            Files.append(output + "\n", errorFile, UTF8);
        } catch (IOException e) {
            _log.warn("Can't write billing-error : {}", e.toString());
        }
    }

    private void doStatistics(InfoMessage info) {
        if (info instanceof WarningPnfsFileInfoMessage) {
            return;
        }
        String cellName = info.getCellName();
        int pos = cellName.indexOf("@");
        cellName = (pos < 1) ? cellName : cellName.substring(0, pos);
        String transactionType = info.getMessageType();
        long[] counters = _poolStatistics.get(cellName);
        if (counters == null) {
            counters = new long[4];
            _poolStatistics.put(cellName, counters);
        }

        if (info.getResultCode() != 0) {
            counters[3]++;
        } else if (transactionType.equals("transfer")) {
            counters[0]++;
        } else if (transactionType.equals("restore")) {
            counters[1]++;
        } else if (transactionType.equals("store")) {
            counters[2]++;
        }
        if (info instanceof PnfsFileInfoMessage) {
            PnfsFileInfoMessage pnfsInfo = (PnfsFileInfoMessage) info;
            StorageInfo sinfo = (pnfsInfo).getStorageInfo();
            if (sinfo != null) {
                Map<String,long[]> map = _poolStorageMap.get(cellName);
                if (map == null) {
                    map = Maps.newHashMap();
                    _poolStorageMap.put(cellName, map);
                }

                String key = sinfo.getStorageClass() + "@" + sinfo.getHsm();

                counters = map.get(key);

                if (counters == null) {
                    counters = new long[8];
                    map.put(key, counters);
                }

                if (info.getResultCode() != 0) {
                    counters[3]++;
                } else if (transactionType.equals("transfer")) {
                    counters[0]++;
                    MoverInfoMessage mim = (MoverInfoMessage) info;
                    counters[mim.isFileCreated() ? 4 : 5] +=
                        mim.getDataTransferred();
                } else if (transactionType.equals("restore")) {
                    counters[1]++;
                    counters[6] += pnfsInfo.getFileSize();
                } else if (transactionType.equals("store")) {
                    counters[2]++;
                    counters[7] += pnfsInfo.getFileSize();
                }
            }
        }
    }

    @Required
    public void setPoolManagerStub(CellStub poolManagerStub) {
        _poolManagerStub = poolManagerStub;
    }

    @Required
    public void setLogsDir(File dir) {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("No such directory: " + dir);
        }
        if (!dir.canWrite()) {
            throw new IllegalArgumentException("Directory not writable: " + dir);
        }
        _logsDir = dir;
    }

    public void setPrintMode(int printMode) {
        _printMode = printMode;
    }

    @Required
    public void setDisableTxt(boolean disableTxt) {
        _disableTxt = disableTxt;
    }
}
