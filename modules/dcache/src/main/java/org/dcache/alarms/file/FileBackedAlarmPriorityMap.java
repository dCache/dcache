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
package org.dcache.alarms.file;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.alarms.Alarm;
import org.dcache.alarms.AlarmDefinitionsMap;
import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.AlarmPriorityMap;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.vehicles.alarms.AlarmPriorityMapRequestMessage;

/**
 * Uses a simple properties file to maintain current settings.
 * Priority mappings can be changed either directly in that file
 * (and then by reloading), or through the setPriority command.
 * Also responds to message requests for a copy of the priority map.
 *
 * @author arossi
 */
public final class FileBackedAlarmPriorityMap
        implements AlarmPriorityMap, CellMessageReceiver {
    private final Map<String, AlarmPriority> internalMap = new ConcurrentHashMap<>();

    private AlarmDefinitionsMap definitions;
    private AlarmPriority defaultPriority = AlarmPriority.CRITICAL;
    private String propertiesPath;

    public AlarmPriority getDefaultPriority() {
        return defaultPriority;
    }

    @Override
    public AlarmPriority getPriority(String type) throws NoSuchElementException {
        AlarmPriority priority = internalMap.get(type);
        if (priority == null) {
            throw new NoSuchElementException("No alarm of type "
                            + type + " yet exists.");
        }
        return priority;
    }

    @Override
    public Map<String, AlarmPriority> getPriorityMap() {
        refreshDefinitions();
        return ImmutableMap.copyOf(internalMap);
    }

    @Override
    public String getSortedList() {
        String[] keys
            = internalMap.keySet().toArray(new String[internalMap.size()]);
        Arrays.sort(keys);
        StringBuilder list = new StringBuilder();
        for (String key: keys) {
            list.append(key)
                .append(" : ")
                .append(internalMap.get(key))
                .append("\n");
        }
        return list.toString();
    }

    public void initialize() throws Exception {
        load(new Properties());
    }

    @Override
    public void load(Properties env) throws Exception {
        internalMap.clear();

        for (Alarm alarm: PredefinedAlarm.values()) {
            internalMap.put(alarm.getType(), defaultPriority);
        }

        refreshDefinitions();
        overrideFromSavedMappings(env);
    }

    public AlarmPriorityMapRequestMessage messageArrived(AlarmPriorityMapRequestMessage request) {
        request.clearReply();
        request.setMap(getPriorityMap());
        request.setSucceeded();
        return request;
    }

    public void restoreAllToDefaultPriority() {
        for (String type: internalMap.keySet()) {
            internalMap.put(type, defaultPriority);
        }
    }

    @Override
    public void save(Properties env) throws Exception {
        File saved = new File(env.getProperty(AlarmPriorityMap.PATH,
                                              propertiesPath));
        if (saved.exists()) {
            saved.delete();
        }

        Properties properties = new Properties();
        for (Map.Entry<String, AlarmPriority> entry : internalMap.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue().toString());
        }

        try (BufferedWriter br = new BufferedWriter(new FileWriter(saved))) {
            properties.store(br, "Last Updated "
                            + new Date(System.currentTimeMillis()));
        }
    }

    public void setDefaultPriority(String priority) {
        this.defaultPriority = AlarmPriority.valueOf(priority.toUpperCase());
    }

    public void setDefinitions(AlarmDefinitionsMap definitions) {
        this.definitions = definitions;
    }

    @Override
    public void setPriority(String alarm, AlarmPriority priority) {
        if (!internalMap.containsKey(alarm)) {
            throw new NoSuchElementException("Alarm type " + alarm
                            + " is currently undefined.");
        }
        internalMap.put(alarm, priority);
    }

    public void setPropertiesPath(String propertiesPath) {
        this.propertiesPath = Preconditions.checkNotNull(propertiesPath);
    }

    private void overrideFromSavedMappings(Properties env) throws IOException {
        File saved = new File(env.getProperty(AlarmPriorityMap.PATH,
                                              propertiesPath));
        if (!saved.exists()) {
            return;
        }

        Properties properties = new Properties();
        try (BufferedReader br = new BufferedReader(new FileReader(saved))) {
            properties.load(br);
        }
        for (Object property: properties.keySet()) {
            String key = property.toString();
            String value = properties.getProperty(key);
            internalMap.put(key, AlarmPriority.valueOf(value));
        }
    }

    private void refreshDefinitions() {
        Set<String> customtypes = definitions.getTypes();
        Set<String> alarmtypes = new HashSet(internalMap.keySet());

        for (PredefinedAlarm predefined: PredefinedAlarm.values()) {
            alarmtypes.remove(predefined.getType());
        }

        for (String type: alarmtypes) {
            if (customtypes.contains(type)) {
                /*
                 * current map already has this defined
                 */
                customtypes.remove(type);
            } else{
                /*
                 * current map contains stale definition
                 */
                internalMap.remove(type);
            }
        }

        /*
         * leftover definitions are new, so they get the default priority
         */
        for (String type: customtypes) {
            internalMap.put(type, defaultPriority);
        }
    }
}
