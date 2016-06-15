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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.resilience.data.FileOperation;
import org.dcache.resilience.data.FileOperationMap;
import org.dcache.resilience.data.FileUpdate;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperation;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.util.PoolSelectionUnitDecorator.SelectionAction;

/**
 * <p>Static methods for writing and reading data for checkpointing purposes.</p>
 *
 * <p>Experimentation with NIO showed that serialization using ByteBuffer
 *      is not efficient, with large writes (of 1M records or more) taking
 *      on the order of 45 minutes to an hour to complete.</p>
 *
 * <p>This implementation writes out a simple CDL to a text file.</p>
 *
 * <p>Also includes load and save methods for recording excluded pools.</p>
 *
 * <p><b>A note on why 'parent' pool information is not saved</b>:  When
 *      writing the record, the originating pool must be recorded so that
 *      a copy or remove operation may be replayed.  The information as to
 *      whether this operation was triggered by a new cache location message
 *      (no parent pool) or a scan (with parent pool), however, is discarded,
 *      because the operation will be reloaded only after a service restart,
 *      which means that all previous pool operation counters have been
 *      reset.  If these reloaded operations begin to report themselves as
 *      children of a pool scan, this may confound the counts for a new
 *      scan which could in the meantime be triggered.  It is thus best
 *      to allow these copy operations to complete by themselves without
 *      worrying about whether they originally were from a scan or not.</p>
 */
public final class CheckpointUtils {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(CheckpointUtils.class);

    /**
     * <p>Read back in from the checkpoint file operation records.
     *    These are converted to {@link FileUpdate} objects and passed
     *    to {@link FileOperationHandler#handleBrokenFileLocation(PnfsId, String)}
     *    for registration.</p>
     *
     * <p>The file to be reloaded is renamed, so that any checkpointing
     *    begun while the reload is in progress does not overwrite the file.
     *    In the case of a failed reload, the reload file should be
     *    manually merged into the current checkpoint file before restart.</p>
     *
     * @param checkpointFilePath to read
     * @param poolInfoMap for translating names to indices
     * @param handler for registering the updates.
     */
    public static void load(String checkpointFilePath,
                    PoolInfoMap poolInfoMap,
                    FileOperationMap pnfsMap,
                    FileOperationHandler handler) {
        if (!new File(checkpointFilePath).exists()) {
            return;
        }

        File current = new File(checkpointFilePath);
        File reload = new File(checkpointFilePath + "-reload");
        current.renameTo(reload);

        try (BufferedReader fr = new BufferedReader(new FileReader(reload))) {
            while (pnfsMap.isRunning()) {
                String line = fr.readLine();
                if (line == null) {
                    break;
                }
                try {
                    FileUpdate update = fromString(line, poolInfoMap);
                    if (update != null) {
                        handler.handleLocationUpdate(update);
                    }
                } catch (CacheException e) {
                    LOGGER.debug("Unable to reload operation for {}; {}",
                                    line, e.getMessage());
                }
            }
            reload.delete();
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to reload checkpoint file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during reload checkpoint file: {}",
                            e.getMessage());
        }
    }

    /**
     * <p>Read back in from the excluded pools file pool names.</p>
     *
     * <p>Deletes the file when done; NOP if there is no file.</p>
     *
     * @param excludedPoolsFile to read
     */
    public static Collection<String> load(String excludedPoolsFile) {
        File current = new File(excludedPoolsFile);
        if (!current.exists()) {
            return Collections.EMPTY_LIST;
        }

        Collection<String> excluded = new ArrayList<>();

        try (BufferedReader fr = new BufferedReader(new FileReader(current))) {
                while (true) {
                String line = fr.readLine();
                if (line == null) {
                    break;
                }
                excluded.add(line);
            }
           current.delete();
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to reload excluded pools file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during reload excluded pools file: {}",
                            e.getMessage());
        }

        return excluded;
    }

    /**
     * <p>Since we use checkpointing as an approximation,
     *      the fact that the ConcurrentMap (internal to the deque class)
     *      may be dirty and that it is not locked should not matter greatly.</p>
     *
     * @param checkpointFilePath where to write.
     * @param poolInfoMap for translation of indices to names.
     * @param iterator from a ConcurrentHashMap implementation of the index.
     * @return number of records written
     */
    public static long save(String checkpointFilePath, PoolInfoMap poolInfoMap,
                            Iterator<FileOperation> iterator) {
        File current = new File(checkpointFilePath);
        File old = new File(checkpointFilePath + "-old");
        if (current.exists()) {
            current.renameTo(old);
        }

        AtomicLong count = new AtomicLong(0);
        StringBuilder builder = new StringBuilder();

        try (PrintWriter fw = new PrintWriter(new FileWriter(checkpointFilePath, false))) {
            while (iterator.hasNext()) {
                FileOperation operation = iterator.next();
                if (toString(operation, builder, poolInfoMap)) {
                    fw.println(builder.toString());
                    count.incrementAndGet();
                    builder.setLength(0);
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to save checkpoint file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during save of checkpoint file: {}",
                            e.getMessage());
        }

        return count.get();
    }

    /**
     * <p>Save the excluded pool names to a file.</p>
     *
     * <p>If there already is such a file, it is deleted.</p>
     *
     * @param excludedPoolsFile to read
     * @param operations the pools which could potentially be
     *                   in the excluded state.
     */
    public static void save(String excludedPoolsFile,
                            Map<String, PoolOperation> operations) {
        File current = new File(excludedPoolsFile);
        if (current.exists()) {
            current.delete();
        }

        try (PrintWriter fw = new PrintWriter(new FileWriter(excludedPoolsFile, false))) {
            operations.entrySet().stream().filter((e) -> e.getValue().isExcluded())
                                 .forEach((e) -> fw.println(e.getKey()));
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to save excluded pools file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during save of excluded pools file: {}",
                            e.getMessage());
        }
    }

    /**
     *  <p>Write out the operation's relevant fields to the buffer.</p>
     *
     *  <p>See the comments to the class for explanation of why checkpointed
     *          operations are "orphaned".</p>
     */
    private static boolean toString(FileOperation operation,
                                    StringBuilder builder,
                                    PoolInfoMap map) {
        Integer parent = operation.getParent();
        Integer source = operation.getSource();
        String pool = parent == null ?
                        (source == null ? null : map.getPool(source)):
                        map.getPool(parent);
        if (pool == null) {
            /*
             *  Incomplete record.  Skip.
             */
            return false;
        }

        builder.append(operation.getPnfsId()).append(",");
        builder.append(operation.getSelectionAction()).append(",");
        builder.append(operation.getOpCount()).append(",");
        builder.append(map.getGroup(operation.getPoolGroup())).append(",");
        builder.append(pool);

        return true;
    }

    /**
     * <p>See the comments to the class for explanation of why checkpointed
     *          operations are "orphaned".</p>
     *
     * @return update object constructed from the parsed line.
     */
    private static FileUpdate fromString(String line, PoolInfoMap map) {
        String[] parts = line.split("[,]");
        if (parts.length != 5) {
            return null;
        }
        PnfsId pnfsId = new PnfsId(parts[0]);
        SelectionAction action = SelectionAction.values()[Integer.parseInt(parts[1])];
        int opCount = Integer.parseInt(parts[2]);
        Integer gindex = map.getGroupIndex(parts[3]);
        FileUpdate update = new FileUpdate(pnfsId, parts[4],
                                           MessageType.ADD_CACHE_LOCATION, action, gindex, true);
        update.setCount(opCount);
        update.setFromReload(true);
        return update;
    }

    private CheckpointUtils() {}
}
