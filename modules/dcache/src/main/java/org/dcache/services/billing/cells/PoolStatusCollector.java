package org.dcache.services.billing.cells;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
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

    private final Path _report;
    private final CellStub _poolManagerStub;

    public PoolStatusCollector(CellStub poolManagerStub, Path file)
    {
        _poolManagerStub = poolManagerStub;
        _report = file;
    }

    /**
     * generates report
     */
    @Override
    public void run() {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(_report, UTF8))) {
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
            try {
                Files.delete(_report);
            } catch (IOException e) {
                _log.warn("Could not delete report {}: {}", _report, e.toString());
            }
        } catch (IOException ioe) {
            _log.warn("Problem opening {} : {}", _report, ioe.getMessage());
        }
    }
}
