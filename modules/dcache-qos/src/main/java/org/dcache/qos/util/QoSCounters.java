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
package org.dcache.qos.util;

import static org.dcache.util.ByteUnit.BYTES;
import static org.dcache.util.ByteUnits.jedecSymbol;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.dcache.util.ByteUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for recording monitoring information and writing out statistics to a file.
 */
public abstract class QoSCounters {

    protected static final Logger LOGGER = LoggerFactory.getLogger(QoSCounters.class);
    protected static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
          .ofPattern("yyyy/MM/dd-HH:mm:ss")
          .withZone(ZoneId.systemDefault());
    protected static final DateTimeFormatter SUFFIX_FORMATTER = DateTimeFormatter
          .ofPattern("yyyy_MM_dd_HH")
          .withZone(ZoneId.systemDefault());

    protected static final String LASTSTART = "Running since: %s\n";
    protected static final String UPTIME = "Uptime %s days, %s hours, %s minutes, %s seconds\n\n";
    protected static final String LASTSWP = "Last sweep at %s\n";
    protected static final String LASTSWPD = "Last sweep took %s seconds\n\n";

    protected static String formatWithPrefix(long count) {
        ByteUnit units = ByteUnit.Type.BINARY.unitsOf(count);
        if (units == BYTES) {
            return String.format("%s", count);
        } else {
            return String.format("%.2f %s", units.convert((double) count, BYTES),
                  jedecSymbol().of(units));
        }
    }

    protected static String getRateChangeSinceLast(double current, double last) {
        if (last == 0) {
            return "?";
        }
        double delta = 100 * (current - last) / last;
        return String.format("%.2f%%", delta);
    }

    protected final Date started = new Date();
    protected final List<String> statisticsBuffer = new ArrayList<>();

    protected long lastSweep = started.getTime();
    protected long lastSweepDuration = 0;
    protected File statisticsPath;
    protected boolean toFile = false;

    protected Map<String, QoSCounterGroup> groupMap;

    public void readStatistics(StringBuilder builder, Integer offset, Integer limit,
          boolean descending) {
        List<String> buffer = new ArrayList<>();

        File path = new File(getPath());
        if (!path.exists()) {
            return;
        }

        try (BufferedReader fr = new BufferedReader(new FileReader(getPath()))) {
            /*
             *  Title line should always be there.
             */
            buffer.add(fr.readLine());
            int end = limit == null ? Integer.MAX_VALUE : limit + 1;
            for (int pos = 0; ; ++pos) {
                String line = fr.readLine();
                if (line == null) {
                    break;
                }

                if (pos < offset) {
                    continue;
                }

                if (descending) {
                    buffer.add(1, line);
                } else if (buffer.size() < end) {
                    buffer.add(line);
                } else {
                    break;
                }

                if (buffer.size() > end) {
                    buffer.remove(end);
                }
            }
        } catch (IOException e) {
            builder.append(e.getMessage()).append("\n");
        }

        buffer.stream().forEach((l) -> builder.append(l).append("\n"));
    }

    public void recordSweep(long ended, long duration) {
        lastSweep = ended;
        lastSweepDuration = duration;
        writeStatistics();
    }

    public void setStatisticsPath(String statisticsPath) {
        this.statisticsPath = new File(statisticsPath);
    }

    public void setToFile(boolean toFile) {
        this.toFile = toFile;
    }

    public void appendRunning(StringBuilder builder) {
        long elapsed
              = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - started.getTime());
        builder.append(String.format(LASTSTART, started));
        appendDHMSElapsedTime(elapsed, UPTIME, builder);
    }

    public void appendDHMSElapsedTime(long elapsed, String format, StringBuilder builder) {
        long seconds = elapsed % 60;
        elapsed = elapsed / 60;
        long minutes = elapsed % 60;
        elapsed = elapsed / 60;
        long hours = elapsed % 24;
        long days = elapsed / 24;
        builder.append(String.format(format, days, hours, minutes, seconds));
    }

    public void appendSweep(StringBuilder builder) {
        builder.append(String.format(LASTSWP, new Date(lastSweep)));
        builder.append(String.format(LASTSWPD, TimeUnit.MILLISECONDS.toSeconds(lastSweepDuration)));
    }

    public abstract void initialize();

    public abstract void appendCounts(StringBuilder builder);

    public abstract void appendDetails(StringBuilder builder);

    protected void writeStatistics() {
        if (!toFile) {
            return;
        }

        File path = new File(getPath());

        if (!path.exists()) {
            try (FileWriter fw = new FileWriter(path, true)) {
                fw.write(String.format(getStatisticsFormat(), (Object[]) getStatisticsHeader()));
                fw.flush();
            } catch (FileNotFoundException e) {
                LOGGER.error("Unable to initialize statistics file: {}", e.getMessage());
            } catch (IOException e) {
                LOGGER.error("Unrecoverable error during initialization of statistics file: {}",
                      e.getMessage());
            }
        }

        synchronized (statisticsBuffer) {
            try (FileWriter fw = new FileWriter(path, true)) {
                for (String line : statisticsBuffer) {
                    fw.write(line);
                }
                fw.flush();
            } catch (FileNotFoundException e) {
                LOGGER.error("Unable to write to statistics file: {}", e.getMessage());
            } catch (IOException e) {
                LOGGER.error("Unrecoverable error writing to statistics file: {}", e.getMessage());
            }

            statisticsBuffer.clear();
        }
    }

    protected abstract String getStatisticsFormat();

    protected abstract String[] getStatisticsHeader();

    protected String getPath() {
        return statisticsPath.getAbsolutePath()
              + "_" + SUFFIX_FORMATTER.format(Instant.ofEpochMilli(System.currentTimeMillis()));
    }
}
