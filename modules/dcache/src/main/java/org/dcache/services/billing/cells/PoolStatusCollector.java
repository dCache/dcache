package org.dcache.services.billing.cells;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Map;

import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;

/**
 * Thread run when command-line statistics call is activated. Generates a
 * statistics report file.
 */
public final class PoolStatusCollector extends Thread
{
    private static final Logger _log =
        LoggerFactory.getLogger(PoolStatusCollector.class);
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final File _report;
    private final CellStub _poolManagerStub;

    public PoolStatusCollector(CellStub poolManagerStub, File file)
    {
        _poolManagerStub = poolManagerStub;
        _report = file;
    }

    /**
     * generates report
     */
    @Override
    public void run() {
        PrintWriter pw;
        try {
            pw = new PrintWriter(Files.newWriter(_report, UTF8));
        } catch (IOException ioe) {
            _log.warn("Problem opening {} : {}", _report, ioe.getMessage());
            return;
        }

        try {
            PoolManagerCellInfo info =
                _poolManagerStub.sendAndWait("xgetcellinfo",
                                             PoolManagerCellInfo.class);
            for (Map.Entry<String, CellAddressCore> pool: info.getPoolMap().entrySet()) {
                try {
                    String s =
                        _poolManagerStub.sendAndWait(new CellPath(pool.getValue()),
                                                     "rep ls -s", String.class);
                    for (String line: s.split("\n")) {
                        pw.println(pool.getKey() + "  " + line);
                    }
                } catch (CacheException | InterruptedException t) {
                    _log.warn("CollectPoolStatus : {}: {}", pool.getValue(), t.toString());
                }
            }
        } catch (CacheException | InterruptedException t) {
            _log.warn("Exception in CollectPools status : {}", t.toString());
            if (!_report.delete()) {
                _log.warn("Could not delete report: {}", _report);
            }
        } finally {
            pw.close();
        }
    }
}
