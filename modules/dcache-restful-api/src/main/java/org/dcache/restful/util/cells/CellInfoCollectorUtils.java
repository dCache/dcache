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
package org.dcache.restful.util.cells;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellVersion;
import org.dcache.vehicles.cells.json.CellData;

/**
 * <p>Utility class aiding in the extraction and updating of
 * information relevant to cells.</p>
 */
public class CellInfoCollectorUtils {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(CellInfoCollectorUtils.class);

    public static CellData fetch(String key, File storageDir) {
        File file = new File(storageDir, key + ".json");
        CellData data = new CellData();

        if (!file.exists()) {
            LOGGER.info("Requested file {} does not exist; creating a new "
                                        + "placeholder object for {}.",
                        file, key);
            return data;
        }

        LOGGER.info("Requested cache entry {} does not exist; "
                                    + "reading and deserializing from {}.",
                    key, file);

        try (RandomAccessFile randomAccessFile
                             = new RandomAccessFile(file,"r")) {
            randomAccessFile.getChannel()
                            .lock(0, Long.MAX_VALUE, true);
            data = new ObjectMapper().readValue(file, CellData.class);
            LOGGER.info("Deserializing {} from {} is complete.", key, file);
        } catch (IOException e) {
            LOGGER.error("Could not deserialize from file {}: {}, {}.",
                         file, e.getMessage(), e.getCause());
        }

        return data;
    }

    public static <T extends Serializable> void flushToDisk(String key,
                                                            File dir, T data) {
        File file = new File(dir, key + ".json");
        try (RandomAccessFile randomAccessFile
                             = new RandomAccessFile(file, "rw")) {
                randomAccessFile.getChannel().lock();
                new ObjectMapper().writerWithDefaultPrettyPrinter()
                                  .writeValue(file, data);
        } catch (IOException io) {
            LOGGER.error("Could not write data for {}: {} / {}.",
                         key, io.getMessage(), io.getCause());
        }
    }

    public static void update(CellData cellData, CellInfo received) {
        cellData.setCreationTime(received.getCreationTime());
        cellData.setDomainName(received.getDomainName());
        cellData.setCellType(received.getCellType());
        cellData.setCellName(received.getCellName());
        cellData.setCellClass(received.getCellClass());
        cellData.setEventQueueSize(received.getEventQueueSize());
        cellData.setExpectedQueueTime(received.getExpectedQueueTime());
        cellData.setLabel("Cell Info");
        CellVersion version = received.getCellVersion();
        cellData.setRelease(version.getRelease());
        cellData.setRevision(version.getRevision());
        cellData.setVersion(version.toString());
        cellData.setState(received.getState());
        cellData.setThreadCount(received.getThreadCount());
    }
}
