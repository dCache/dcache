package org.dcache.services.billing.cells;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Date;

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
        name = name == null ? ("poolStatus-" + BillingCell.getFilenameformat()
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
            /*
             * FIXME -- there is a better way to do this using the <T extends
             * Message> APIs. -ALR
             */
            CellMessage m = new CellMessage(PATH_TO_POOL_MANAGER,
                            "xgetcellinfo");
            m = poolStub.sendAndWait(m, CellMessage.class,
                            poolStub.getTimeout());
            Object o = m.getMessageObject();
            if (!(o instanceof diskCacheV111.poolManager.PoolManagerCellInfo))
                throw new CacheException("Illegal Reply from PoolManager : "
                                + o.getClass().getName());
            diskCacheV111.poolManager.PoolManagerCellInfo info = (diskCacheV111.poolManager.PoolManagerCellInfo) o;
            String[] poolList = info.getPoolList();
            for (String path : poolList) {
                m = new CellMessage(new CellPath(path), "rep ls -s");
                try {
                    m = poolStub.sendAndWait(m, CellMessage.class,
                                    poolStub.getTimeout());
                    BufferedReader br = new BufferedReader(new StringReader(m
                                    .getMessageObject().toString()));
                    String line;
                    while ((line = br.readLine()) != null) {
                        pw.println(path + "  " + line);
                    }
                } catch (IOException t) {
                    logger.warn("CollectPoolStatus : " + path + " : " + t);
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
