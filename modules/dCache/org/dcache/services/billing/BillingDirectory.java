package org.dcache.services.billing;

import java.text.SimpleDateFormat;
import java.io.File;
import java.util.Date;

/**
 */
public class BillingDirectory
{
    protected final static SimpleDateFormat directoryNameFormat
        = new SimpleDateFormat("yyyy" + File.separator + "MM");

    protected File _billingDb;
    protected int _mode = 0;

    public BillingDirectory()
    {
    }

    /**
     * Returns the base directory for all logging output.
     */
    public File getDirectory()
    {
        return _billingDb;
    }

    /**
     * Sets the base directory for all logging output.
     */
    public void setDirectory(File dir)
    {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("No such directory: " + dir);
        }
        _billingDb = dir;
    }

    public int getMode()
    {
        return _mode;
    }

    public void setMode(int mode)
    {
        _mode = mode;
    }

    protected File getDirectory(Date date)
    {
        File dir;
        if (_mode == 0) {
            dir = _billingDb;
        } else {
            dir = new File(_billingDb, directoryNameFormat.format(date));
            if (!dir.exists())
                dir.mkdirs();
        }
        return dir;
    }
}