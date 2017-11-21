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
package org.dcache.restful.util.billing;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.dcache.restful.providers.billing.BillingDataGrid;
import org.dcache.restful.providers.billing.BillingDataGridEntry;
import org.dcache.util.histograms.TimeFrame;
import org.dcache.util.histograms.TimeFrame.BinType;
import org.dcache.util.histograms.TimeFrame.Type;
import org.dcache.vehicles.billing.BillingDataRequestMessage;
import org.dcache.vehicles.billing.BillingDataRequestMessage.SeriesDataType;
import org.dcache.vehicles.billing.BillingDataRequestMessage.SeriesType;

/**
 * <p>Provides methods for generating and transforming messages to be
 * sent to the dCache billing service.</p>
 */
public final class BillingInfoCollectionUtils {
    /**
     * Configures the boundaries and interval.
     */
    private static TimeFrame configure(Calendar high, BinType bin, Type type) {
        TimeFrame frame = new TimeFrame(high.getTimeInMillis());
        frame.setTimebin(bin);
        frame.setTimeframe(type);
        frame.configure();
        return frame;
    }

    /**
     * <p>Generates the preset grid of time-series data; the time frames
     * are computed based on the current time.</p>
     * <p>
     * <p>The grid of data is as follows:</p>
     * <p>
     * <table>
     * <tr>
     * <p>
     * <td>READ_SIZE_HOUR_DAY</td>
     * <td>READ_SIZE_DAY_WEEK</td>
     * <td>READ_SIZE_DAY_MONTH</td>
     * <td>READ_SIZE_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>WRITE_SIZE_HOUR_DAY</td>
     * <td>WRITE_SIZE_DAY_WEEK</td>
     * <td>WRITE_SIZE_DAY_MONTH</td>
     * <td>WRITE_SIZE_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>P2P_SIZE_HOUR_DAY</td>
     * <td>P2P_SIZE_DAY_WEEK</td>
     * <td>P2P_SIZE_DAY_MONTH</td>
     * <td>P2P_SIZE_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>STORE_SIZE_HOUR_DAY</td>
     * <td>STORE_SIZE_DAY_WEEK</td>
     * <td>STORE_SIZE_DAY_MONTH</td>
     * <td>STORE_SIZE_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>RESTORE_SIZE_HOUR_DAY</td>
     * <td>RESTORE_SIZE_DAY_WEEK</td>
     * <td>RESTORE_SIZE_DAY_MONTH</td>
     * <td>RESTORE_SIZE_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>READ_COUNT_HOUR_DAY</td>
     * <td>READ_COUNT_DAY_WEEK</td>
     * <td>READ_COUNT_DAY_MONTH</td>
     * <td>READ_COUNT_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>WRITE_COUNT_HOUR_DAY</td>
     * <td>WRITE_COUNT_DAY_WEEK</td>
     * <td>WRITE_COUNT_DAY_MONTH</td>
     * <td>WRITE_COUNT_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>P2P_COUNT_HOUR_DAY</td>
     * <td>P2P_COUNT_DAY_WEEK</td>
     * <td>P2P_COUNT_DAY_MONTH</td>
     * <td>P2P_COUNT_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>STORE_COUNT_HOUR_DAY</td>
     * <td>STORE_COUNT_DAY_WEEK</td>
     * <td>STORE_COUNT_DAY_MONTH</td>
     * <td>STORE_COUNT_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>RESTORE_COUNT_HOUR_DAY</td>
     * <td>RESTORE_COUNT_DAY_WEEK</td>
     * <td>RESTORE_COUNT_DAY_MONTH</td>
     * <td>RESTORE_COUNT_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>CONNECTION_MAXTIME_HOUR_DAY</td>
     * <td>CONNECTION_MAXTIME_DAY_WEEK</td>
     * <td>CONNECTION_MAXTIME_DAY_MONTH</td>
     * <td>CONNECTION_MAXTIME_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>CONNECTION_AVGTIME_HOUR_DAY</td>
     * <td>CONNECTION_AVGTIME_DAY_WEEK</td>
     * <td>CONNECTION_AVGTIME_DAY_MONTH</td>
     * <td>CONNECTION_AVGTIME_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>CONNECTION_MINTIME_HOUR_DAY</td>
     * <td>CONNECTION_MINTIME_DAY_WEEK</td>
     * <td>CONNECTION_MINTIME_DAY_MONTH</td>
     * <td>CONNECTION_MINTIME_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>CACHED_HIT_HOUR_DAY</td>
     * <td>CACHED_HIT_DAY_WEEK</td>
     * <td>CACHED_HIT_DAY_MONTH</td>
     * <td>CACHED_HIT_DAY_YEAR</td>
     * </tr>
     * <tr>
     * <td>CACHED_MISS_HOUR_DAY</td>
     * <td>CACHED_MISS_DAY_WEEK</td>
     * <td>CACHED_MISS_DAY_MONTH</td>
     * <td>CACHED_MISS_DAY_YEAR</td>
     * </tr>
     * </table>
     *
     * @return the full set of messages (60) to be sent to gather all
     * the time series data.
     */
    public static List<BillingDataRequestMessage> generateMessages() {
        List<BillingDataRequestMessage> messages = new ArrayList<>();
        TimeFrame[] timeFrames = getCurrentFrames();
        BillingDataRequestMessage msg = null;
        for (TimeFrame timeFrame : timeFrames) {
            for (SeriesType type : SeriesType.values()) {
                switch (type) {
                    case READ:
                    case WRITE:
                    case P2P:
                    case STORE:
                    case RESTORE:
                        msg = new BillingDataRequestMessage();
                        msg.setDataType(SeriesDataType.BYTES);
                        msg.setTimeFrame(timeFrame);
                        msg.setType(type);
                        messages.add(msg);
                        msg = new BillingDataRequestMessage();
                        msg.setDataType(SeriesDataType.COUNT);
                        msg.setTimeFrame(timeFrame);
                        msg.setType(type);
                        messages.add(msg);
                        break;
                    case CONNECTION:
                        msg = new BillingDataRequestMessage();
                        msg.setDataType(SeriesDataType.MAXSECS);
                        msg.setTimeFrame(timeFrame);
                        msg.setType(type);
                        messages.add(msg);
                        msg = new BillingDataRequestMessage();
                        msg.setDataType(SeriesDataType.AVGSECS);
                        msg.setTimeFrame(timeFrame);
                        msg.setType(type);
                        messages.add(msg);
                        msg = new BillingDataRequestMessage();
                        msg.setDataType(SeriesDataType.MINSECS);
                        msg.setTimeFrame(timeFrame);
                        msg.setType(type);
                        messages.add(msg);
                        break;
                    case CACHED:
                        msg = new BillingDataRequestMessage();
                        msg.setDataType(SeriesDataType.HITS);
                        msg.setTimeFrame(timeFrame);
                        msg.setType(type);
                        messages.add(msg);
                        msg = new BillingDataRequestMessage();
                        msg.setDataType(SeriesDataType.MISSES);
                        msg.setTimeFrame(timeFrame);
                        msg.setType(type);
                        messages.add(msg);
                    default:
                        break;
                }
            }
        }

        return messages;
    }

    /**
     * for daily, weekly, monthly and yearly
     */
    private static TimeFrame[] getCurrentFrames() {
        TimeFrame[] timeFrame = new TimeFrame[4];
        Calendar high = TimeFrame.computeHighTimeFromNow(BinType.HOUR);
        timeFrame[0] = configure(high, BinType.HOUR, Type.DAY);
        high = TimeFrame.computeHighTimeFromNow(BinType.DAY);
        timeFrame[1] = configure(high, BinType.DAY, Type.WEEK);
        timeFrame[2] = configure(high, BinType.DAY, Type.MONTH);
        timeFrame[3] = configure(high, BinType.DAY, Type.YEAR);
        return timeFrame;
    }

    public static BillingDataGrid getDataGrid() {
        BillingDataGrid billingDataGrid
                        = new BillingDataGrid(new HashMap<String,
                        BillingDataGridEntry>());
        generateMessages().stream()
                          .map(BillingInfoCollectionUtils::transform)
                          .forEach((e) -> billingDataGrid.getDataGrid()
                                                         .put(e.toString(), e));
        return billingDataGrid;
    }

    public static String getKey(BillingDataRequestMessage message) {
        return new BillingDataGridEntry(message.getType(),
                                        message.getDataType(),
                                        message.getTimeFrame()).toString();
    }

    private static BillingDataGridEntry transform(
                    BillingDataRequestMessage request) {
        SeriesDataType dataType = request.getDataType();
        Preconditions.checkNotNull(dataType);
        SeriesType type = request.getType();
        Preconditions.checkNotNull(type);
        TimeFrame timeFrame = request.getTimeFrame();
        Preconditions.checkNotNull(timeFrame);
        return new BillingDataGridEntry(type,
                                        dataType,
                                        timeFrame);
    }

    private BillingInfoCollectionUtils() {
    }
}
