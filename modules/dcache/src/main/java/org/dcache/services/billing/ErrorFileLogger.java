package org.dcache.services.billing;

import java.util.Date;
import java.io.File;

import diskCacheV111.vehicles.InfoMessage;

public class ErrorFileLogger
    extends FileLogger
{
    public void messageArrived(InfoMessage message)
    {
        if (message.getResultCode() != 0)
            super.messageArrived(message);
    }

    public void messageArrived(Object message)
    {
    }

    protected File getFile(Date date)
    {
        return new File(getDirectory(date),
                        "billing-error-" + fileNameFormat.format(date));
    }

}