package org.dcache.services.billing.plot;

import java.util.Calendar;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.dcache.services.billing.plots.util.TimeFrame;
import org.dcache.services.billing.plots.util.TimeFrame.BinType;
import org.dcache.services.billing.plots.util.TimeFrame.Type;

/**
 * @author arossi
 */
public class TimeFrameTest extends TestCase {
    protected static final Logger logger = Logger.getLogger(TimeFrameTest.class);

    public void testTimeFrameHOUR_DAY() {
        Calendar ch = getHigh();
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(ch.getTimeInMillis());
        cl.set(Calendar.HOUR_OF_DAY, cl.get(Calendar.HOUR_OF_DAY) - 24);
        adjustForDST(ch, cl);
        Date high = ch.getTime();
        Date low = cl.getTime();
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.HOUR);
        tf.setTimeframe(Type.DAY);
        tf.configure();
        assertEquals(24, tf.getBinCount());
        assertEquals(3600.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals(low.getTime(), (long) tf.getLowTime());
    }

    public void testTimeFrameDAY_WEEK() {
        Calendar ch = getHigh();
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(ch.getTimeInMillis());
        cl.set(Calendar.DATE, cl.get(Calendar.DATE) - 7);
        adjustForDST(ch, cl);
        Date high = ch.getTime();
        Date low = cl.getTime();
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.DAY);
        tf.setTimeframe(Type.WEEK);
        tf.configure();
        assertEquals(7, tf.getBinCount());
        assertEquals(86400.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals(low.getTime(), (long) tf.getLowTime());
    }

    public void testTimeFrameDAY_MONTH() {
        Calendar ch = getHigh();
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(ch.getTimeInMillis());
        cl.set(Calendar.DAY_OF_MONTH, cl.get(Calendar.DAY_OF_MONTH) - 30);
        adjustForDST(ch, cl);
        Date high = ch.getTime();
        Date low = cl.getTime();
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.DAY);
        tf.setTimeframe(Type.MONTH);
        tf.configure();
        assertEquals(30, tf.getBinCount());
        assertEquals(86400.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals(low.getTime(), (long) tf.getLowTime());
    }

    public void testTimeFrameDAY_MONTH2() {
        Calendar ch = getHigh();
        ch.set(Calendar.MONTH, Calendar.NOVEMBER);
        ch.set(Calendar.DAY_OF_MONTH, 22);
        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.MONTH, Calendar.OCTOBER);
        cl.set(Calendar.DAY_OF_MONTH, 23);
        adjustForDST(ch, cl);
        Date high = ch.getTime();
        Date low = cl.getTime();
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.DAY);
        tf.setTimeframe(Type.MONTH);
        tf.configure();
        assertEquals(30, tf.getBinCount());
        assertEquals(86400.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals(low.getTime(), (long) tf.getLowTime());
    }

    public void testTimeFrameDAY_YEAR() {
        Calendar ch = getHigh();
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(ch.getTimeInMillis());
        cl.set(Calendar.DAY_OF_YEAR, cl.get(Calendar.DAY_OF_YEAR) - 365);
        adjustForDST(ch, cl);
        Date high = ch.getTime();
        Date low = cl.getTime();
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.DAY);
        tf.setTimeframe(Type.YEAR);
        tf.configure();
        assertEquals(365, tf.getBinCount());
        assertEquals(86400.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals(low.getTime(), (long) tf.getLowTime());
    }

    private static Calendar getHigh() {
        long t = System.currentTimeMillis();
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(t);
        return c;
    }

    private static void adjustForDST(Calendar chigh, Calendar clow) {
        boolean dstH = chigh.getTimeZone().inDaylightTime(chigh.getTime());
        boolean dstL = clow.getTimeZone().inDaylightTime(clow.getTime());
        if (dstH && !dstL) {
            clow.setTime(new Date(clow.getTime().getTime()
                            - chigh.getTimeZone().getDSTSavings()));
        } else if (dstL && !dstH) {
            chigh.setTime(new Date(chigh.getTime().getTime()
                            - clow.getTimeZone().getDSTSavings()));
        }
        chigh.set(Calendar.MILLISECOND, 0);
        clow.set(Calendar.MILLISECOND, 0);
    }
}
