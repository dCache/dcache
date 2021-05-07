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
package org.dcache.qos.services.scanner.util;

import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.util.QoSCounter;
import org.dcache.qos.util.QoSCounterGroup;
import org.dcache.qos.util.QoSCounters;

import static org.dcache.qos.data.QoSMessageType.POOL_STATUS_DOWN;
import static org.dcache.qos.data.QoSMessageType.POOL_STATUS_UP;

public final class QoSScannerCounters extends QoSCounters {
  private static final String SCANS          = "SCANS";
  private static final String POOLS          = "POOLS";
  private static final String FORMAT_SCANS = "%-25s %12s %12s\n";
  private static final String FORMAT_DETAILS = "%-25s | %12s %12s | %12s %12s %12s | %12s %20s\n";
  private static final String[] SCANS_HEADER = {"ACTION", "COMPLETED", "FAILED" };
  private static final String[] DETAILS_HEADER = {"NAME", "TOTAL", "FAILED", "UP", "DOWN", "FORCED",
                                                  "FILES", "AVGPRD (ms)"};
  private static final String FORMAT_STATS = "%-15s  | %20s | %25s %8s %5s %12s %5s\n";
  private static final String[] STATS_HEADER = {"EPOCH", "DATETIME", "POOL", "STATUS", "FORCED", "FILES", "FAILED"};

  class QoSScanCounterGroup extends QoSCounterGroup<QoSCounter> {
    protected QoSScanCounterGroup(String name) {
      super(name);
    }

    @Override
    public void format(StringBuilder builder) {
      getKeys().stream()
               .forEach(k-> {
                  QoSCounter c = getCounter(k);
                  builder.append(String.format(FORMAT_SCANS, k, c.getTotal(), c.getFailed()));
          });
    }

    @Override
    protected QoSCounter createCounter(String key) {
      return new QoSCounter(key);
    }
  }

  class QoSPoolCounterGroup extends QoSCounterGroup<QoSPoolCounter> {
    protected QoSPoolCounterGroup(String name) {
      super(name);
    }

    @Override
    public void format(StringBuilder builder) {
      getKeys().stream()
          .forEach(k -> {
              QoSPoolCounter c = getCounter(k);
              long total = c.getTotal();
              builder.append(String.format(FORMAT_DETAILS, k, c.getTotal(), c.getFailed(),
                                            c.up.get(), c.down.get(), c.forced.get(), c.files.get(),
                                            total == 0L ? 0L : c.interval.get()/total));
          });
    }

    @Override
    protected QoSPoolCounter createCounter(String key) {
      return new QoSPoolCounter(key);
    }
  }

  class QoSPoolCounter extends QoSCounter {
    final AtomicLong canceled = new AtomicLong(0L);
    final AtomicLong files = new AtomicLong(0L);
    final AtomicLong forced = new AtomicLong(0L);
    final AtomicLong down = new AtomicLong(0L);
    final AtomicLong up = new AtomicLong(0L);
    final AtomicLong interval = new AtomicLong(0L);

    protected QoSPoolCounter(String name) {
      super(name);
    }
  }

  @Override
  public void initialize() {
    groupMap = new HashMap<>();
    QoSCounterGroup group = new QoSScanCounterGroup(SCANS);
    group.addCounter(POOL_STATUS_UP.name());
    group.addCounter(POOL_STATUS_DOWN.name());
    groupMap.put(SCANS, group);

    group = new QoSPoolCounterGroup(POOLS);
    groupMap.put(POOLS, group);
  }

  @Override
  public void appendCounts(StringBuilder builder) {
    builder.append(String.format(FORMAT_SCANS, SCANS_HEADER));
    QoSCounterGroup group = groupMap.get(SCANS);
    group.format(builder);
  }

  @Override
  public void appendDetails(StringBuilder builder) {
    builder.append(String.format(FORMAT_DETAILS, DETAILS_HEADER));
    QoSPoolCounterGroup group = (QoSPoolCounterGroup)groupMap.get(POOLS);
    group.format(builder);
  }

  public void incrementCancelled(String pool, PoolQoSStatus status,
                                 long files, boolean forced, long sincePrevious) {
    checkPoolCounters(pool);
    QoSPoolCounterGroup poolCounterGroup = (QoSPoolCounterGroup)groupMap.get(POOLS);
    QoSPoolCounter counter = poolCounterGroup.getCounter(pool);
    update(counter, status == PoolQoSStatus.DOWN, files, forced, sincePrevious);
    counter.canceled.incrementAndGet();
  }

  public void increment(String pool, PoolQoSStatus status, boolean failed,
                        long files, boolean forced, long sincePrevious) {
    boolean down = status == PoolQoSStatus.DOWN;

    QoSCounterGroup group = groupMap.get(SCANS);
    QoSCounter actionCounter
        = group.getCounter(down ? POOL_STATUS_DOWN.name() : POOL_STATUS_UP.name());
    actionCounter.incrementTotal();

    checkPoolCounters(pool);
    QoSPoolCounterGroup poolCounterGroup = (QoSPoolCounterGroup)groupMap.get(POOLS);
    QoSPoolCounter counter = poolCounterGroup.getCounter(pool);

    if (failed) {
      actionCounter.incrementFailed();
      counter.incrementFailed();
    }

    update(counter, down, files, forced, sincePrevious);
    counter.incrementTotal();

    if (toFile) {
      Instant now = Instant.now();
      synchronized (statisticsBuffer) {
        statisticsBuffer.add(String.format(FORMAT_STATS, now.toEpochMilli(),
            DATE_FORMATTER.format(now), pool,
            status.name(), forced, files, failed));
      }
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

  private void checkPoolCounters(String pool) {
    QoSPoolCounterGroup group = (QoSPoolCounterGroup)groupMap.get(POOLS);
    if (!group.hasCounter(pool)) {
      group.addCounter(pool);
    }
  }

  private void update(QoSPoolCounter counter, boolean down, long files,
                      boolean forced, long sincePrevious) {
    if (down) {
      counter.down.incrementAndGet();
    } else {
      counter.up.incrementAndGet();
    }

    counter.files.addAndGet(files);

    if (forced) {
      counter.forced.incrementAndGet();
    }

    counter.interval.addAndGet(sincePrevious);
  }
}
