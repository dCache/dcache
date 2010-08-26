package org.dcache.services.billing;

import java.io.File;
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * Common base class for billing cell components containing shared
 * configuration settings.
 */
public class BillingComponent
{
    protected final static SimpleDateFormat fileNameFormat
         = new SimpleDateFormat("yyyy.MM.dd");
    protected BillingDirectory _directory;

    public BillingComponent()
    {
    }

    /**
     * Sets the base directory for all logging output.
     */
    public void setBillingDirectory(BillingDirectory directory)
    {
        _directory = directory;
    }

    protected File getDirectory()
    {
        return _directory.getDirectory();
    }

    protected File getDirectory(Date date)
    {
        return _directory.getDirectory(date);
    }

    public void info(String s)
    {

    }

    public void error(String s)
    {
        System.err.println(s);
    }
}