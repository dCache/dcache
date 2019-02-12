package org.dcache.services.billing.cells;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;

/**
 * Thread run when command-line statistics call is activated. Generates a
 * statistics report file.
 */
public final class PoolStatusCollector extends Thread
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(PoolStatusCollector.class);

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
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(_report, StandardCharsets.UTF_8))) {
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
                    LOGGER.warn("CollectPoolStatus : {}: {}", pool.getValue(), t.toString());
                }
            }
        } catch (CacheException | NoRouteToCellException | InterruptedException t) {
            LOGGER.warn("Exception in CollectPools status : {}", t.toString());
            try {
                Files.delete(_report);
            } catch (IOException e) {
                LOGGER.warn("Could not delete report {}: {}", _report, e.toString());
            }
        } catch (IOException ioe) {
            LOGGER.warn("Problem opening {} : {}", _report, ioe.getMessage());
        }
    }
}
