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
package org.dcache.resilience.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.util.PnfsId;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;

/**
 * <p>An implementation of the handler which (a) writes out the pnfsids to
 *      a file and (b) sends an alarm with its file path (the alarm will
 *      report the domain, so the admin should be able to locate the host).</p>
 *
 * <p>In case of an error on the write, the pnfsid list will be dumped to
 *      the log and the alarm will contain a message to that effect.</p>
 */
public final class InaccessibleFileAlarmSender implements
                InaccessibleFileHandler {
    private static final String INACCESSIBLE_FILE_MESSAGE
                    = "Resilient pool {} is inaccessible but it contains the "
                    + "only copy of files which should have a replica elsewhere. "
                    + "Administrator intervention is required.\n\t -- {}.";

    private static final String FAILED_WRITE = "An IO error prevented writing "
                    + "the inaccessible list for %s to %s";

    private static final String ALARM_MSG = "See the list of pnfsids at %s";

    private static final String ALARM_MSG_ON_FAIL = "%s; these have been logged "
                    + "at ERROR level in the namespace log.";

    private final Map<String, String> messages = new ConcurrentHashMap<>();
    private final Map<String, String> messagesFailed = new ConcurrentHashMap<>();

    private String parentDir;

    /**
     * <p>Add pnfsId for file on the pool which has not been written
     *      to backend storage (CUSTODIAL) but for which the pool contains
     *      the unique copy.  Alarm contains path of the file listing all
     *      such pnfsids.  Multiple sends of this alarm should merely increase
     *      the count, since the key is identical for all files on a pool.</p>
     */
    @Override
    public void registerInaccessibleFile(String pool, PnfsId pnfsId) {
        File file = new File(parentDir,
                             String.format("%s-inaccessible_files", pool));
        FileWriter fw = null;
        String alarmMessage;

        try {
            fw = new FileWriter(file, true);
            fw.write(pnfsId.toString());
            fw.write("\n");
            fw.flush();
            alarmMessage = String.format(ALARM_MSG, file);
            messages.put(pool, alarmMessage);
        } catch (IOException e) {
            String errorMsg = String.format(FAILED_WRITE, pool, file);
            /*
             * Failed to write the file.  Log the error.
             */
            LOGGER.error("{}: {}\n\nFILE:\n {}.",
                            errorMsg,
                            new ExceptionMessage(e),
                            pnfsId);

            alarmMessage = String.format(ALARM_MSG_ON_FAIL, errorMsg);
            messagesFailed.put(pool, alarmMessage);
        } finally {
           if (fw != null) {
               try {
                   fw.close();
               } catch (IOException e) {
                   LOGGER.debug("{}", new ExceptionMessage(e));
               }
           }
        }
    }

    /**
     *  <p>Alarm points to either the file or the log.  Keyed to pool,
     *      so multiple files on the pool will simply increment the count
     *      on the alarm.</p>
     */
    @Override
    public void handleInaccessibleFilesIfExistOn(String pool) {
        String alarmMessage = messages.remove(pool);
        if (alarmMessage != null) {
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                            pool),
                            INACCESSIBLE_FILE_MESSAGE,
                            pool,
                            alarmMessage);
        }

        alarmMessage = messagesFailed.remove(pool);
        if (alarmMessage != null) {
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                            pool, "failed"),
                            INACCESSIBLE_FILE_MESSAGE,
                            pool,
                            alarmMessage);
        }
    }

    public void setParentDir(String parentDir) {
        this.parentDir = parentDir;
    }
}
