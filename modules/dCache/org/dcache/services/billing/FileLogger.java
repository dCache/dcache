package org.dcache.services.billing;

import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Date;

public class FileLogger extends TextLogger
{
    protected int _printMode = 0;

    public int getPrintMode()
    {
        return _printMode;
    }

    public void setPrintMode(int mode)
    {
        _printMode = mode;
    }

    protected File getDirectory(Date date)
    {
        File dir;
        if (_printMode == 0) {
            dir = _billingDb;
        } else {
            dir = new File(_billingDb, directoryNameFormat.format(date));
            if (!dir.exists())
                dir.mkdirs();
        }
        return dir;
    }

    protected File getFile(Date date)
    {
        return new File(getDirectory(date),
                        "billing-" + fileNameFormat.format(date));
    }

    protected void log(Date date, String output)
    {
        File outputFile = getFile(new Date());

        try {
            PrintWriter pw = new PrintWriter(new FileWriter(outputFile, true));
            try {
                pw.println(output);
            } finally{
                pw.close() ;
            }
        } catch (IOException e) {
            error("Can't write billing [" + outputFile + "] : " + e.toString());
        }
    }
}
