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
package org.dcache.webadmin.model.businessobjects.rrd4j;

import java.awt.Color;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.PoolQueueInfo;

/**
 * Convenience data type for converting between pool cost info and the value set
 * to be stored. Also defined {@link PoolQueueHistogram} for round-robin archive
 * processing.
 *
 * @author arossi
 */
public class PoolQueuePlotData {
    private static final Color DARK_RED = new Color(102,0,0);
    private static final Color DARK_GREEN = new Color(0,102,0);

    public enum PoolQueueHistogram implements RoundRobinDatabaseDatasource {
        AMOVER {
            public String getLabel() {
                return "active mover";
            }

            public Color getColor() {
                return Color.CYAN;
            }
        },
        QMOVER {
            public String getLabel() {
                return "queued mover";
            }

            public Color getColor() {
                return Color.BLUE;
            }
        },
        ASTORE {
            public String getLabel() {
                return "active store";
            }

            public Color getColor() {
                return Color.GREEN;
            }
        },
        QSTORE {
            public String getLabel() {
                return "queued store";
            }

            public Color getColor() {
                return DARK_GREEN;
            }
        },
        ARESTORE {
            public String getLabel() {
                return "active restore";
            }

            public Color getColor() {
                return Color.RED;
            }
        },
        QRESTORE {
            public String getLabel() {
                return "queued restore";
            }

            public Color getColor() {
                return DARK_RED;
            }
        };

        public String getSourceName() {
            return getLabel().replaceAll(" ", "_");
        }
    }

    private String poolName;
    private Date date = new Date(System.currentTimeMillis());
    private Integer activeMovers = 0;
    private Integer queuedMovers = 0;
    private Integer activeStores = 0;
    private Integer queuedStores = 0;
    private Integer activeRestores = 0;
    private Integer queuedRestores = 0;

    public PoolQueuePlotData() {
        this("anonymous");
    }

    public PoolQueuePlotData(String poolName) {
        this.poolName = poolName;
    }

    public PoolQueuePlotData(PoolCostInfo costInfo) {
        this(costInfo.getPoolName());
        addValues(costInfo);
    }

    public void addValues(PoolCostInfo costInfo) {
        PoolQueueInfo queueInfo = costInfo.getMoverQueue();
        activeMovers += queueInfo.getActive();
        queuedMovers += queueInfo.getQueued();
        queueInfo = costInfo.getStoreQueue();
        activeStores += queueInfo.getActive();
        queuedStores += queueInfo.getQueued();
        queueInfo = costInfo.getRestoreQueue();
        activeRestores += queueInfo.getActive();
        queuedRestores += queueInfo.getQueued();
    }

    public Map<String, Double> data() {
        Map<String, Double> data = new HashMap<String, Double>();
        data.put(PoolQueueHistogram.AMOVER.getSourceName(), (double)activeMovers);
        data.put(PoolQueueHistogram.QMOVER.getSourceName(), (double)queuedMovers);
        data.put(PoolQueueHistogram.ASTORE.getSourceName(), (double)activeStores);
        data.put(PoolQueueHistogram.QSTORE.getSourceName(), (double)queuedStores);
        data.put(PoolQueueHistogram.ARESTORE.getSourceName(), (double)activeRestores);
        data.put(PoolQueueHistogram.QRESTORE.getSourceName(), (double)queuedRestores);
        return data;
    }

    public Integer getActiveMovers() {
        return activeMovers;
    }

    public Integer getActiveRestores() {
        return activeRestores;
    }

    public Integer getActiveStores() {
        return activeStores;
    }

    public Date getDate() {
        return date;
    }

    public String getPoolName() {
        return poolName;
    }

    public Integer getQueuedMovers() {
        return queuedMovers;
    }

    public Integer getQueuedRestores() {
        return queuedRestores;
    }

    public Integer getQueuedStores() {
        return queuedStores;
    }

    public void setActiveMovers(Integer activeMovers) {
        this.activeMovers = activeMovers;
    }

    public void setActiveRestores(Integer activeRestores) {
        this.activeRestores = activeRestores;
    }

    public void setActiveStores(Integer activeStores) {
        this.activeStores = activeStores;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    public void setQueuedMovers(Integer queuedMovers) {
        this.queuedMovers = queuedMovers;
    }

    public void setQueuedRestores(Integer queuedRestores) {
        this.queuedRestores = queuedRestores;
    }

    public void setQueuedStores(Integer queuedStores) {
        this.queuedStores = queuedStores;
    }
}
