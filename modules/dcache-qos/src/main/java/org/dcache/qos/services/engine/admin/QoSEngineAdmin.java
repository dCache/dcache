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
package org.dcache.qos.services.engine.admin;

import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.util.concurrent.TimeUnit;
import org.dcache.qos.services.engine.util.QoSEngineCounters;
import org.dcache.qos.util.InitializerAwareCommand;
import org.dcache.qos.util.MapInitializer;
import org.dcache.qos.util.MessageGuard;

public final class QoSEngineAdmin implements CellCommandListener {
  @Command(name = "disable", hint = "turn off handling",
           description = "Prevents external messages from being processed by the qos system.")
  class DisableCommand extends InitializerAwareCommand {
    @Option(name="drop", valueSpec = "true|false",
            usage = "If true, do not store backlogged messages; false by default.")
    Boolean drop = false;

    DisableCommand() { super(initializer); }

    @Override
    protected String doCall() throws Exception {
      if (messageGuard.isEnabled()) {
        messageGuard.disable(drop);
        if (drop) {
          return "Processing of incoming messages has been disabled; "
              + "backlogged messages will be dropped.";
        }
        return "Processing of incoming messages has been disabled; "
            + "backlogged messages will be stored.";
      }

      return "Receiver already disabled.";
    }
  }

  @Command(name = "enable", hint = "turn on handling",
           description = "Allows external messages to be received by the qos system.")
  class EnableCommand extends InitializerAwareCommand {

    EnableCommand() { super(initializer); }

    @Override
    protected String doCall() {
      if (!messageGuard.isEnabled()) {
        messageGuard.enable();
        return "Processing of incoming messages has been re-enabled";
      }
      return "Receiver is already enabled.";
    }
  }

  @Command(name = "engine stats", hint = "print diagnostic statistics",
      description = "Reads in the contents of the file recording periodic statistics.")
  class EngineStatsCommand extends InitializerAwareCommand {
    @Option(name = "limit", usage = "Display up to this number of lines (default is 24 * 60).")
    Integer limit = 24 * 60;

    @Option(name = "order", valueSpec = "asc|desc",
        usage = "Display lines in ascending (default) or descending order by timestamp.")
    String order = "asc";

    @Option(name = "enable",
        usage = "Turn the recording of statistics to file on or off. Recording to file is "
            + "off by default.")
    Boolean enable = null;

    EngineStatsCommand() { super(initializer); }

    protected String doCall() throws Exception {
      if (enable != null) {
        counters.setToFile(enable);
        if (enable) {
          counters.scheduleStatistics();
        }
        return "Recording to file is now " + (enable ? "on." : "off.");
      }

      SortOrder order = SortOrder.valueOf(this.order.toUpperCase());
      StringBuilder builder = new StringBuilder();
      counters.readStatistics(builder, 0, limit, order == SortOrder.DESC);
      return builder.toString();
    }
  }

  private final MapInitializer initializer = new MapInitializer() {
    @Override
    protected long getRefreshTimeout() {
      return 0;
    }

    @Override
    protected TimeUnit getRefreshTimeoutUnit() {
      return TimeUnit.MILLISECONDS;
    }

    @Override
    public void run() {
    }

    @Override
    public boolean isInitialized() {
      return true;
    }
  };

  private MessageGuard messageGuard;
  private QoSEngineCounters counters;

  public void setCounters(QoSEngineCounters counters) {
    this.counters = counters;
  }

  public void setMessageGuard(MessageGuard messageGuard) {
    this.messageGuard = messageGuard;
  }
}
