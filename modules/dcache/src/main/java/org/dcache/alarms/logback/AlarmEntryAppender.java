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
package org.dcache.alarms.logback;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.dcache.alarms.dao.AlarmEntry;
import org.dcache.alarms.dao.AlarmStorageException;
import org.dcache.alarms.dao.IAlarmLoggingDAO;
import org.dcache.alarms.dao.impl.DataNucleusAlarmStore;
import org.json.JSONException;
import org.json.JSONObject;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import com.google.common.base.Preconditions;

/**
 * For server-side interception of Alarm messages. Will store them to the
 * AlarmEntry store used by the dCache installation. If the storage plugin is
 * file-based (e.g., XML), the dCache alarm display service (webadmin) must be
 * running on a shared file-system with the logging server.
 *
 * @author arossi
 */
public class AlarmEntryAppender extends AppenderBase<ILoggingEvent> {

    /**
     * Creates an entry object from event message, which is assumed to be
     * formatted using JSON. Will fail if message format is faulty.
     *
     * @param event
     *            containing the alarm representation
     * @return data storage object
     */
    private static AlarmEntry createEntryFromJSONMessage(ILoggingEvent event)
                    throws JSONException {
        String alarmInfo = event.getMessage();
        Preconditions.checkNotNull(alarmInfo);
        JSONObject json = new JSONObject(alarmInfo);
        return new AlarmEntry(json);
    }
    private final Properties properties = new Properties();
    private String path;
    private String propertiesPath;

    private IAlarmLoggingDAO store;

    public void setDriver(String driver) {
        properties.setProperty("datanucleus.ConnectionDriverName", driver);
    }

    public void setPass(String pass) {
        properties.setProperty("datanucleus.ConnectionPassword", pass);
    }

    public void setPropertiesPath(String propertiesPath) {
       this.propertiesPath = propertiesPath;
    }

    public void setStorePath(String path) {
        this.path = path;
    }

    public void setUrl(String url) {
        properties.setProperty("datanucleus.ConnectionURL", url);
    }

    public void setUser(String user) {
        properties.setProperty("datanucleus.ConnectionUserName", user);
    }

    @Override
    public void start() {
        try {
            if (store == null) {
                if (propertiesPath != null
                                && propertiesPath.trim().length() > 0) {
                    File file = new File(propertiesPath);
                    if (!file.exists()) {
                        throw new FileNotFoundException(
                                   "Cannot find properties file: " + file);
                    }
                    try (InputStream stream = new FileInputStream(file)) {
                        properties.load(stream);
                    }
                }
                store = new DataNucleusAlarmStore(path, properties);
            }
            super.start();
        } catch (AlarmStorageException t) {
            addError(t.getMessage() + "; " + t.getCause());
            // do not set started to true
        } catch (IOException t) {
            addError(t.getMessage() + "; " + t.getCause());
            // do not set started to true
        }
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        try {
            if (isStarted()) {
                store.put(createEntryFromJSONMessage(eventObject));
            }
        } catch (Exception t) {
            addError(t.getMessage() + "; " + t.getCause());
        }
    }

    /**
     * Largely a convenience for internal testing.
     */
    void setStore(IAlarmLoggingDAO store) {
        this.store = store;
    }
}
