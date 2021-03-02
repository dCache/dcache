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
package org.dcache.qos.services.engine.util;

import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.dcache.qos.util.QoSCounter;
import org.dcache.qos.util.QoSCounterGroup;
import org.dcache.qos.util.QoSCounters;

import static org.dcache.qos.data.QoSMessageType.ADD_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.CLEAR_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.CORRUPT_FILE;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED_CANCELED;

public final class QoSEngineCounters extends QoSCounters implements Runnable {
  public static final String QOS_ACTION_COMPLETED = "QOS_ACTION_COMPLETED";

  private static final String MESSAGES       = "MESSAGES";
  private static final String FORMAT_CNTS    = "%-26s %12s\n";
  private static final String FORMAT_STATS   = "%-15s | %25s | %12s\n";
  private static final String[] CNTS_HEADER  = {"MSG TYPE", "RECEIVED"};
  private static final String[] STATS_HEADER = {"EPOCH", "DATETIME", "MESSAGE"};

  class QoSEngineCounterGroup extends QoSCounterGroup<QoSCounter> {

    protected QoSEngineCounterGroup(String name) {
      super(name);
    }

    @Override
    public void toFormattedString(StringBuilder builder) {
      getKeys().stream()
               .forEach(k->builder.append(String.format(FORMAT_CNTS, k, getCounter(k).getTotal())));
    }

    @Override
    protected QoSCounter createCounter(String key) {
      return new QoSCounter(key);
    }
  }

  private ScheduledExecutorService service;

  @Override
  public void appendCounts(StringBuilder builder) {
    builder.append("\n").append(String.format(FORMAT_CNTS, CNTS_HEADER));
    groupMap.values().stream().forEach(g -> g.toFormattedString(builder));
  }

  @Override
  public void appendDetails(StringBuilder builder) {
    // NOP for engine
  }

  @Override
  public void initialize() {
    groupMap = new HashMap<>();
    QoSEngineCounterGroup group = new QoSEngineCounterGroup(MESSAGES);
    group.addCounter(ADD_CACHE_LOCATION.name());
    group.addCounter(CORRUPT_FILE.name());
    group.addCounter(CLEAR_CACHE_LOCATION.name());
    group.addCounter(QOS_MODIFIED.name());
    group.addCounter(QOS_MODIFIED_CANCELED.name());
    group.addCounter(QOS_ACTION_COMPLETED);
    groupMap.put(MESSAGES, group);
  }

  public synchronized void increment(String counter) {
    QoSCounterGroup counterGroup = groupMap.get(MESSAGES);
    if (counterGroup == null) {
      LOGGER.debug("trying to increment non-existent counter group {}", MESSAGES);
      return;
    }
    counterGroup.getCounter(counter).incrementTotal();
    if (toFile) {
      Instant i = Instant.now();
      synchronized (statisticsBuffer) {
        statisticsBuffer
            .add(String.format(FORMAT_STATS, i.toEpochMilli(), DATE_FORMATTER.format(i), counter));
      }
    }
  }

  public long getCount(String counter) {
    return groupMap.get(MESSAGES).getCounter(counter).getTotal();
  }

  public void run() {
    writeStatistics();
    scheduleStatistics();
  }

  public void setService(ScheduledExecutorService service) {
    this.service = service;
  }

  public void scheduleStatistics() {
    if (toFile) {
      service.schedule(this, 1, TimeUnit.MINUTES);
    }
  }

  @Override
  protected String getStatisticsFormat() {
    return FORMAT_STATS;
  }

  @Override
  protected String[] getStatisticsHeader() {
    return STATS_HEADER;
  }
}
