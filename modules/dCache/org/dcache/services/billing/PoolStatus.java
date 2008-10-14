package org.dcache.services.billing;

import java.util.Date;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

import dmg.util.Args;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellEndpoint;

import org.dcache.cells.CellMessageSender;
import org.dcache.cells.CellCommandListener;

import diskCacheV111.poolManager.PoolManagerCellInfo;

public class PoolStatus
    extends BillingComponent
    implements CellMessageSender,
               CellCommandListener
{
    private CellEndpoint _endpoint;
    private long _timeout = 40000L;

    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
    }

    public long getTimeout()
    {
        return _timeout;
    }

    public void setTimeout(long timeout)
    {
        if (timeout < 0)
            throw new IllegalArgumentException("Timeout must not be negative");
        _timeout = timeout;
    }

    public String hh_get_poolstatus = "[<fileName>]";
    public String ac_get_poolstatus_$_0_1(Args args)
    {
        CollectPoolStatus status =
            new CollectPoolStatus(args.argc() > 0 ? args.argv(0) : null);
        return status.getReportFile().toString();
    }

    private class CollectPoolStatus implements Runnable
    {
        private File _report;

        private CollectPoolStatus(String name)
        {
            name = (name == null
                    ? "poolStatus-" + fileNameFormat.format(new Date())
                    : name);
            _report = new File(getDirectory(), name);
            Thread t = new Thread(this, "poolStatus-" + name);
            t.start();
        }

        public File getReportFile()
        {
            return _report;
        }

        private String[] rep_ls(String pool)
            throws NoRouteToCellException,
                   InterruptedException
        {
            CellMessage m = new CellMessage(new CellPath(pool), "rep ls -s");
            m = _endpoint.sendAndWait(m, _timeout);
            if (m == null) {
                error("CollectPoolStatus : pool status (rep ls -s) of " + pool + " didn't arrive in time (skipped)");
                return new String[0];
            }

            return m.getMessageObject().toString().split("\n");
        }

        private String[] getPoolList()
            throws NoRouteToCellException,
                   InterruptedException
        {
            CellMessage m = new CellMessage(new CellPath("PoolManager"),
                                            "xgetcellinfo");
            m = _endpoint.sendAndWait(m, _timeout);
            if (m == null)
                return new String[0];

            Object o = m.getMessageObject();
            if (!(o instanceof PoolManagerCellInfo))
                throw new RuntimeException("Illegal reply from PoolManager: "
                                           + o.getClass().getName());

            return ((PoolManagerCellInfo)o).getPoolList();
        }

        public void run()
        {
            try {
                PrintWriter pw =
                    new PrintWriter(new BufferedWriter(new FileWriter(_report)));
                try {
                    for (String pool: getPoolList()) {
                        try {
                            for (String line: rep_ls(pool)) {
                                pw.println(pool + "  " + line);
                            }
                        } catch (NoRouteToCellException e) {
                            error("Failed to query " + pool + ":" + e.getMessage());
                        }
                    }
                    pw.close();
                    pw = null;
                } catch (NoRouteToCellException e) {
                    error("Failed to obtain pool list: " + e);
                } finally {
                    if (pw != null) {
                        pw.close();
                        _report.delete();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                error("Problem opening " + _report + " : " + e.getMessage());
            }
       }
    }
}