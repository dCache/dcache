/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.services.billing.histograms;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Data object which aids in the construction of the X-axis of a 1-D histogram.<br>
 * <br>
 * Contains convenience methods for computing and returning the number of bins
 * and the bin width based on the upper bound time value, {@link BinType} and
 * the {@link Type}.
 *
 * @author arossi
 */
public final class TimeFrame implements Serializable {

    /**
     * Unit of the time frame.
     */
    public enum BinType {
        TEN_MINUTE, HOUR, DAY, WEEK, MONTH
    }

    /**
     * Extent of the time frame.
     */
    public enum Type {
        DAY, WEEK, MONTH, YEAR, THIS_DAY, THIS_WEEK, THIS_MONTH, THIS_YEAR
    }

    private static final long serialVersionUID = 3114577177366772739L;

    /**
     * @return Current time rounded up to next 12-hour interval for hourly and
     *         down to the current day for daily data.
     */
    public static Calendar computeHighTimeFromNow(BinType type) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        int hh = 0;
        int dd = cal.get(Calendar.DAY_OF_MONTH);
        if (type == BinType.HOUR) {
            hh = cal.get(Calendar.HOUR_OF_DAY) < 12 ? 12 : 24;
        }
        cal.set(Calendar.DAY_OF_MONTH, dd);
        cal.set(Calendar.HOUR_OF_DAY, hh);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }

    private int binCount;
    private double binWidth;
    private Type timeframe;
    private BinType timebin;
    private final Calendar highDate, lowDate;

    /**
     * Defaults to current time for upper bound.
     */
    public TimeFrame() {
        this(System.currentTimeMillis());
    }

    public TimeFrame(long upperBound) {
        highDate = Calendar.getInstance();
        highDate.setTime(new Date(upperBound));
        lowDate = Calendar.getInstance();
        binCount = 1;
        timeframe = Type.DAY;
    }

    /**
     * computes boundaries, number of bins and bin width.
     */
    public void configure() {
        Calendar tmp = Calendar.getInstance();
        tmp.setTime(highDate.getTime());

        switch(timeframe) {
            case DAY:
                lowDate.setTime(new Date(highDate.getTimeInMillis()
                            - TimeUnit.DAYS.toMillis(1)));
                break;
            case WEEK:
                lowDate.setTime(new Date(highDate.getTimeInMillis()
                            - TimeUnit.DAYS.toMillis(7)));
                break;
            case MONTH:
                lowDate.setTime(new Date(highDate.getTimeInMillis()
                            - TimeUnit.DAYS.toMillis(30)));
                break;
            case YEAR:
                lowDate.setTime(new Date(highDate.getTimeInMillis()
                            - TimeUnit.DAYS.toMillis(365)));
                break;
            case THIS_DAY:
                tmp.set(Calendar.MINUTE, 0);
                tmp.set(Calendar.SECOND, 0);
                tmp.set(Calendar.HOUR_OF_DAY, 0);
                lowDate.setTime(new Date(tmp.getTimeInMillis()));
                tmp.set(Calendar.MINUTE, 59);
                tmp.set(Calendar.SECOND, 59);
                tmp.set(Calendar.HOUR_OF_DAY, 23);
                highDate.setTime(new Date(tmp.getTimeInMillis()));
                break;
            case THIS_WEEK:
                tmp.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
                tmp.set(Calendar.SECOND, 0);
                tmp.set(Calendar.MINUTE, 0);
                tmp.set(Calendar.HOUR_OF_DAY, 0);
                lowDate.setTime(new Date(tmp.getTimeInMillis()));
                tmp.set(Calendar.SECOND, 59);
                tmp.set(Calendar.MINUTE, 59);
                tmp.set(Calendar.HOUR_OF_DAY, 23);
                tmp.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
                highDate.setTime(new Date(tmp.getTimeInMillis()));
                break;
            case THIS_MONTH:
                tmp.set(Calendar.DAY_OF_MONTH, 1);
                tmp.set(Calendar.SECOND, 0);
                tmp.set(Calendar.MINUTE, 0);
                tmp.set(Calendar.HOUR_OF_DAY, 0);
                lowDate.setTime(new Date(tmp.getTimeInMillis()));
                tmp.set(Calendar.SECOND, 59);
                tmp.set(Calendar.MINUTE, 59);
                tmp.set(Calendar.HOUR_OF_DAY, 23);
                tmp.set(Calendar.DAY_OF_MONTH,
                            tmp.getActualMaximum(Calendar.DAY_OF_MONTH));
                highDate.setTime(new Date(tmp.getTimeInMillis()));
                break;
            case THIS_YEAR:
                tmp.set(Calendar.DAY_OF_MONTH, 1);
                tmp.set(Calendar.MONTH, Calendar.JANUARY);
                tmp.set(Calendar.SECOND, 0);
                tmp.set(Calendar.MINUTE, 0);
                tmp.set(Calendar.HOUR_OF_DAY, 0);
                lowDate.setTime(new Date(tmp.getTimeInMillis()));
                tmp.set(Calendar.MONTH, Calendar.DECEMBER);
                tmp.set(Calendar.SECOND, 59);
                tmp.set(Calendar.MINUTE, 59);
                tmp.set(Calendar.HOUR_OF_DAY, 23);
                tmp.set(Calendar.DAY_OF_MONTH,
                            tmp.getActualMaximum(Calendar.DAY_OF_MONTH));
                highDate.setTime(new Date(tmp.getTimeInMillis()));
                break;
            default:
                break;
        }

        switch(timebin) {
            case TEN_MINUTE:    binWidth = 10 * 60;         break;
            case HOUR:          binWidth = 3600;            break;
            case DAY:           binWidth = 3600 * 24;       break;
            case WEEK:          binWidth = 3600 * 24 * 7;   break;
            case MONTH:         binWidth = 3600 * 24 * 30;  break;
            default:            binWidth = 3600;            break;
        }

        binCount = (int) ((getHighTime().doubleValue() / 1000
                          - getLowTime().doubleValue() / 1000) / binWidth);
    }

    public int getBinCount() {
        return binCount;
    }

    public double getBinWidth() {
        return binWidth;
    }

    public Date getHigh() {
        return highDate.getTime();
    }

    public Calendar getHighDate() {
        return highDate;
    }

    public Long getHighTime() {
        return highDate.getTimeInMillis();
    }

    public Date getLow() {
        return lowDate.getTime();
    }

    public Calendar getLowDate() {
        return lowDate;
    }

    public Long getLowTime() {
        return lowDate.getTimeInMillis();
    }

    public BinType getTimebin() {
        return timebin;
    }

    public Type getTimeframe() {
        return timeframe;
    }

    public void setTimebin(BinType timebin) {
        this.timebin = timebin;
    }

    public void setTimeframe(Type timeframe) {
        this.timeframe = timeframe;
    }
}
