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
    protected static final Logger logger = Logger
                    .getLogger(TimeFrameTest.class);

    public void testTimeFrameHOUR_DAY() {
        Date high = getHigh();
        Calendar c = Calendar.getInstance();
        c.setTime(high);
        c.set(Calendar.HOUR_OF_DAY, c.get(Calendar.HOUR_OF_DAY) - 24);
        Date low = getCSTAdjusted(c);
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.HOUR);
        tf.setTimeframe(Type.DAY);
        tf.configure();
        assertEquals(24, tf.getBinCount());
        assertEquals(3600.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals((long) low.getTime(), (long) tf.getLowTime());
    }

    public void testTimeFrameDAY_WEEK() {
        Date high = getHigh();
        Calendar c = Calendar.getInstance();
        c.setTime(high);
        c.set(Calendar.DATE, c.get(Calendar.DATE) - 7);
        Date low = getCSTAdjusted(c);
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.DAY);
        tf.setTimeframe(Type.WEEK);
        tf.configure();
        assertEquals(7, tf.getBinCount());
        assertEquals(86400.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals((long) low.getTime(), (long) tf.getLowTime());
    }

    public void testTimeFrameDAY_MONTH() {
        Date high = getHigh();
        Calendar c = Calendar.getInstance();
        c.setTime(high);
        c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) - 30);
        Date low = getCSTAdjusted(c);
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.DAY);
        tf.setTimeframe(Type.MONTH);
        tf.configure();
        assertEquals(30, tf.getBinCount());
        assertEquals(86400.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals((long) low.getTime(), (long) tf.getLowTime());
    }

    public void testTimeFrameDAY_YEAR() {
        Date high = getHigh();
        Calendar c = Calendar.getInstance();
        c.setTime(high);
        c.set(Calendar.DAY_OF_YEAR, c.get(Calendar.DAY_OF_YEAR) - 365);
        Date low = getCSTAdjusted(c);
        TimeFrame tf = new TimeFrame(high.getTime());
        tf.setTimebin(BinType.DAY);
        tf.setTimeframe(Type.YEAR);
        tf.configure();
        assertEquals(365, tf.getBinCount());
        assertEquals(86400.0, tf.getBinWidth());
        assertEquals(high, tf.getHigh());
        assertEquals(low, tf.getLow());
        assertEquals((long) low.getTime(), (long) tf.getLowTime());
    }

    private static Date getHigh() {
        long t = System.currentTimeMillis();
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(t);
        return getCSTAdjusted(c);
    }

    private static Date getCSTAdjusted(Calendar c) {
        return new Date(c.getTime().getTime() + c.get(Calendar.DST_OFFSET));
    }
}
