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
package org.dcache.services.bulk.store.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Set;

import org.dcache.services.bulk.BulkRequestStorageException;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkStorageException;

import static java.util.Objects.requireNonNull;

/**
 *  Handles writing and reading from disk.
 *
 *  The contract is that this class is to be used with an in-memory map
 *  which holds the entire store.
 *
 *  This internal map is populated from disk at startup.
 *
 *  New or updated data is written out singly when the child class deems
 *  appropriate.
 *
 *  If the cell shuts down peacefully, the entire contents are written out
 *  again just before.
 *
 * @param <T> the object type to be stored.
 */
abstract class AbstractObjectFileStore<T extends Serializable>
{
    protected static final Logger LOGGER
                    = LoggerFactory.getLogger(AbstractObjectFileStore.class);

    private static final String OBJ_SUFFIX = ".obj";

    private static final FilenameFilter OBJ_FILTER
                    = (d, n) -> n.endsWith(OBJ_SUFFIX);

    protected final File storageDir;
    protected final Class<T> storageType;

    protected AbstractObjectFileStore(File storageDir, Class<T> storageType)
    {
        requireNonNull(storageDir, "Object File Store "
                        + "must be given an explicit base directory to write to.");
        requireNonNull(storageType, "Object File Store "
                        + "must be given an explicit object class.");
        this.storageDir = storageDir;
        this.storageType = storageType;
    }

    protected void deleteFromDisk(String id)
    {
        LOGGER.trace("deleteFromDisk {}.", id);

        File file = new File(storageDir, id + OBJ_SUFFIX);
        if (file.exists()) {
            file.delete();
        }
    }

    /**
     * It is assumed the storage directory points to a shallow directory
     * containing files which have the ".obj" extension and whose
     * names correspond to the key values of the map.
     */
    protected void readFromDisk()
    {
        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }

        File[] files = storageDir.listFiles(OBJ_FILTER);

        String name;
        T wrapper;

        for (File file : files) {
            try {
                wrapper = deserialize(file);
            } catch (BulkStorageException e) {
                LOGGER.warn("Deserialization failed for {}: {}; file "
                                            + "is corrupt or incomplete; "
                                            + "removing ...",
                            file, e.getMessage());
                file.delete();
                continue;
            }

            name = file.getName();
            name = name.substring(0, name.lastIndexOf(OBJ_SUFFIX));

            try {
                postProcessDeserialized(name, wrapper);
            } catch (BulkServiceException e) {
                LOGGER.warn("There was a problem post-processing object from {}: "
                                            + "{}.",
                            file, e.getMessage());
            }
        }
    }

    protected void writeToDisk()
    {
        ids().forEach(id -> writeToDisk(id));
    }

    protected synchronized void writeToDisk(String id)
    {
        LOGGER.trace("writeToDisk {}.", id);

        T wrapper;
        try {
            wrapper = newInstance(id);
        } catch (BulkServiceException e) {
            LOGGER.warn("There was a problem instantiating wrapper for {}: {}; "
                                        + "cannot write to disk.",
                        id, e.getMessage());
            return;
        }

        try {
            serializeToDisk(id, wrapper);
        } catch (BulkStorageException e) {
            LOGGER.warn("Failed to save {} to disk: "
                                        + "{}, {}.", id, e.getMessage(),
                        e.getCause());
        }
    }

    protected abstract void postProcessDeserialized(String id, T object)
                    throws BulkServiceException;

    protected abstract T newInstance(String id) throws BulkServiceException;

    protected abstract Set<String> ids();

    private T deserialize(File file) throws BulkStorageException
    {
        try (ObjectInputStream ostream
                             = new ObjectInputStream(new FileInputStream(file))) {
            return storageType.cast(ostream.readObject());
        } catch (FileNotFoundException e) {
            LOGGER.warn("File not found: {}; could not deserialize.", file);
            return null;
        } catch (ClassNotFoundException e) {
            throw new BulkRequestStorageException("Wrapper class seems not to "
                                                                  + "have been "
                                                                  + "loaded!.",
                                                  e);
        } catch (IOException e) {
            throw new BulkRequestStorageException("deserialize failed", e);
        }
    }

    private void serializeToDisk(String name,
                                 T serializable) throws BulkStorageException
    {
        File file = new File(storageDir, name + OBJ_SUFFIX);
        try (ObjectOutputStream ostream
                             = new ObjectOutputStream(new FileOutputStream(file))) {
            ostream.writeObject(serializable);
        } catch (IOException e) {
            throw new BulkRequestStorageException("problem serializing to disk "
                                                                  + "for "
                                                                  + name, e);
        }
    }
}
