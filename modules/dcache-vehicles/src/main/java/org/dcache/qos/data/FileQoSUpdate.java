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
package org.dcache.qos.data;

import diskCacheV111.util.PnfsId;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 *  A transient encapsulation of pertinent configuration data regarding a file, synthesized
 *  from a message received by the service.
 */
public final class FileQoSUpdate implements Serializable {
  private static final long serialVersionUID = 4144355720999312042L;

  private static final String FORMAT_STR = "yyyy/MM/dd HH:mm:ss";
  private static final DateTimeFormatter DATE_FORMATTER
      = DateTimeFormatter.ofPattern(FORMAT_STR).withZone(ZoneId.systemDefault());

  public static String getFormattedDateFromMillis(long millis) {
    return DATE_FORMATTER.format(Instant.ofEpochMilli(millis));
  }

  private final PnfsId pnfsId;
  private final QoSMessageType type;
  private String pool;
  private String effectivePoolGroup;
  private String storageUnit;
  private boolean forced;

  private long   size;

  public FileQoSUpdate(PnfsId pnfsId, String pool, QoSMessageType type) {
    this.pnfsId = pnfsId;
    this.pool = pool;
    this.type = type;
  }

  /**
   *  @param pnfsId of the file.
   *  @param pool   either the source of the message, or the pool being scanned.
   *  @param type   CORRUPT_FILE, CLEAR_CACHE_LOCATION, ADD_CACHE_LOCATION, QOS_MODIFIED,
   *                POOL_STATUS_DOWN, or POOL_STATUS_UP.
   *  @param storageUnit being checked for changed constraints
   *  @param forced periodic or admin scan
   */
  public FileQoSUpdate(PnfsId pnfsId, String pool, QoSMessageType type,
      String group, String storageUnit, boolean forced) {
    this(pnfsId, pool, type);
    this.effectivePoolGroup = group;
    this.storageUnit = storageUnit;
    this.forced = forced;
  }

  public String getStorageUnit() {
    return storageUnit;
  }

  public PnfsId getPnfsId() {
    return pnfsId;
  }

  public QoSMessageType getMessageType() {
    return type;
  }

  public String getPool() {
    return pool;
  }

  public boolean isForced() { return forced; }

  public void setPool(String pool) {
    this.pool = pool;
  }

  public String toString() {
    return String.format("(%s)(%s)(%s)(%s)(%s)",
        pnfsId, pool, type, effectivePoolGroup, storageUnit);
  }

  public String getEffectivePoolGroup() {
    return effectivePoolGroup;
  }

  public void setEffectivePoolGroup(String effectivePoolGroup) {
    this.effectivePoolGroup = effectivePoolGroup;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }
}
