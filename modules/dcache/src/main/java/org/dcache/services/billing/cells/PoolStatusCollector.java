package org.dcache.services.billing.cells;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Date;

import diskCacheV111.poolManager.PoolManagerCellInfo;

import org.dcache.cells.CellStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

/**
 * Thread run when command-line statistics call is activated. Generates a
 * statistics report file.
 *
 * @author arossi
 */
public final class PoolStatusCollector extends Thread {
    private static final CellPath PATH_TO_POOL_MANAGER = new CellPath(
                    "PoolManager");
    private static final Logger logger = LoggerFactory
                    .getLogger(PoolStatusCollector.class);

    private final File report;
    private final CellStub poolStub;

    /**
     * @param name
     * @param parent
     */
    public PoolStatusCollector(String name, BillingCell parent) {
        name = name == null ? ("poolStatus-" + parent.getFilenameformat()
                        .format(new Date())) : name;
        setName(name);
        report = new File(parent.getLogsDir(), name);
        poolStub = parent.getPoolStub();
    }

    /**
     * @return report file
     */
    public File getReportFile() {
        return report;
    }

    /**
     * generates report
     */
    public void run() {
        PrintWriter pw;
        try {
            pw = new PrintWriter(new BufferedWriter(new FileWriter(report)));
        } catch (IOException ioe) {
            logger.warn("Problem opening {} : {}", report, ioe.getMessage());
            return;
        }

        try {
            PoolManagerCellInfo info =
                poolStub.sendAndWait("xgetcellinfo", PoolManagerCellInfo.class);
            for (String path: info.getPoolList()) {
                try {
                    String s = poolStub.sendAndWait(new CellPath(path),
                                                    "rep ls -s", String.class);
                    for (String line: s.split("\n")) {
                        pw.println(path + "  " + line);
                    }
                } catch (CacheException t) {
                    logger.warn("CollectPoolStatus : " + path + " : " + t);
                } catch (InterruptedException t) {
                    logger.warn("CollectPoolStatus : " + path + " : " + t);
                }
            }
        } catch (CacheException t) {
            logger.warn("Exception in CollectPools status : " + t);
            report.delete();
        } catch (InterruptedException t) {
            logger.warn("Exception in CollectPools status : " + t);
            report.delete();
        } finally {
            pw.close();
        }
    }
}
