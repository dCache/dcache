package org.dcache.services.billing;

import java.util.Date;
import java.text.SimpleDateFormat;
import diskCacheV111.vehicles.InfoMessage;
import org.dcache.cells.CellMessageReceiver;

public abstract class TextLogger
    extends BillingComponent
    implements CellMessageReceiver
{
    protected final static SimpleDateFormat formatter
         = new SimpleDateFormat ("MM.dd HH:mm:ss");

    public void messageArrived(InfoMessage message)
    {
        if (message.getMessageType().equals("check"))
            return;

        Date date = new Date(message.getTimestamp());
        String output = message.toString();
        log(date, output);
    }

    public void messageArrived(Object message)
    {
        Date date = new Date();
        String output = formatter.format(date) + " " + message;
        log(date, output);
    }

    protected abstract void log(Date date, String output);
}
