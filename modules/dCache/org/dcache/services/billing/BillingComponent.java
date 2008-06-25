package org.dcache.services.billing;

import java.text.SimpleDateFormat;
import java.io.File;

/**
 * Common base class for billing cell components containing shared
 * configuration settings.
 */
public class BillingComponent
{
    protected final static SimpleDateFormat formatter
         = new SimpleDateFormat ("MM.dd HH:mm:ss");
    protected final static SimpleDateFormat fileNameFormat
         = new SimpleDateFormat( "yyyy.MM.dd" ) ;
    protected final static SimpleDateFormat directoryNameFormat
        = new SimpleDateFormat( "yyyy"+File.separator+"MM" ) ;

    protected File _billingDb;

    public BillingComponent()
    {
    }

    public File getBillingDirectory()
    {
        return _billingDb;
    }

    public void setBillingDirectory(File dir)
    {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("No such directory: " + dir);
        }
        _billingDb = dir;
    }

    public void info(String s)
    {

    }

    public void error(String s)
    {
        System.err.println(s);
    }
}