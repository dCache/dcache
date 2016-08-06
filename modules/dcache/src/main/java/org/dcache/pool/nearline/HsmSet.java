/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.nearline;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import javax.annotation.PreDestroy;

import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.Formats;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;
import org.dcache.util.Args;
import org.dcache.util.ColumnWriter;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static java.util.Collections.unmodifiableSet;

/**
 * An HsmSet encapsulates information about attached HSMs. The HsmSet
 * also acts as a cell command interpreter, allowing the user to
 * add, remove or alter the information.
 *
 * Each HSM has a case sensitive instance name which uniquely
 * identifies this particular tape system throughout dCache. Notice
 * that multiple pools can be attached to the same HSM. In that case
 * the instance name must be the same at each pool.
 *
 * An HSM also has a type, e.g. OSM or Enstore. The type is not case
 * sensitive.  Traditionally, the type was called the HSM name. It is
 * important not to confuse the type with the instance name.
 *
 * Earlier versions of dCache did not specify an instance name. For
 * compatibility, the type may serve as an instance name.
 */
public class HsmSet
    implements CellCommandListener, CellSetupProvider, CellLifeCycleAware
{
    private static final ServiceLoader<NearlineStorageProvider> PROVIDERS =
            ServiceLoader.load(NearlineStorageProvider.class);
    private static final String DEFAULT_PROVIDER = "script";

    private final ConcurrentMap<String, HsmInfo> _newConfig = Maps.newConcurrentMap();
    private final ConcurrentMap<String, HsmInfo> _hsm = Maps.newConcurrentMap();
    private boolean _isReadingSetup;

    private NearlineStorageProvider findProvider(String name)
    {
        for (NearlineStorageProvider provider : PROVIDERS) {
            if (provider.getName().equals(name)) {
                return provider;
            }
        }
        throw new IllegalArgumentException("No such nearline storage provider: " + name);
    }

    /**
     * Information about a particular HSM instance.
     */
    public class HsmInfo
    {
        private final String _type;
        private final String _instance;
        private final Map<String,String> _currentAttributes = new LinkedHashMap<>();
        private final Map<String,String> _newAttributes = new LinkedHashMap<>();
        private final NearlineStorageProvider _provider;
        private final NearlineStorage _nearlineStorage;

        /**
         * Constructs an HsmInfo object.
         *
         * @param instance A unique instance name.
         * @param type     The HSM type, e.g. OSM or enstore.
         */
        public HsmInfo(String instance, String type, String provider)
        {
            _instance = instance;
            _type = type.toLowerCase();
            _provider = findProvider(provider);
            _nearlineStorage = _provider.createNearlineStorage(_type, _instance);
        }

        /**
         * Returns the instance name.
         */
        public String getInstance()
        {
            return _instance;
        }

        /**
         * Returns the HSM type.
         */
        public String getType()
        {
            return _type;
        }

        /**
         * Returns the HSM provider name.
         */
        public String getProvider()
        {
            return _provider.getName();
        }

        /**
         * Returns the value of an attribute. Returns null if the
         * attribute has not been defined.
         *
         * @param attribute An attribute name
         */
        public synchronized String getAttribute(String attribute)
        {
           return _currentAttributes.get(attribute);
        }

        /**
         * Removes an attribute.
         *
         * @param attribute An attribute name
         */
        public synchronized void unsetAttribute(String attribute)
        {
            _newAttributes.remove(attribute);
        }

        /**
         * Sets an attribute to a value.
         *
         * @param attribute An attribute name
         * @param value     A value string
         */
        public synchronized void setAttribute(String attribute, String value)
        {
           _newAttributes.put(attribute, value);
        }

        /**
         * Returns the set of attributes.
         */
        public synchronized Iterable<Map.Entry<String, String>> attributes()
        {
            return new ArrayList<>(_currentAttributes.entrySet());
        }

        /**
         * Applies the current configuration to the nearline storage. Roles back
         * to the previous configuration if the new configuration is rejected.
         */
        public synchronized void refresh()
        {
            try {
                _nearlineStorage.configure(_newAttributes);
                _currentAttributes.clear();
                _currentAttributes.putAll(_newAttributes);
            } catch (Exception e) {
                _newAttributes.clear();
                _newAttributes.putAll(_currentAttributes);
                throw Throwables.propagate(e);
            }
        }

        /**
         * Scans an argument set for options and applies those as
         * attributes to an HsmInfo object.
         */
        public synchronized void scanOptions(Args args)
        {
            for (Map.Entry<String,String> e: args.options().entries()) {
                String optName  = e.getKey();
                String optValue = e.getValue();
                setAttribute(optName, optValue == null ? "" : optValue);
            }
            if (!_isReadingSetup) {
                refresh();
            }
        }

        /**
         * Scans an argument set for options and removes and unsets those
         * attributes in the given HsmInfo object.
         */
        public synchronized void scanOptionsUnset(Args args)
        {
            for (String optName: args.options().keySet()) {
                unsetAttribute(optName);
            }
            if (!_isReadingSetup) {
                refresh();
            }
        }

        public NearlineStorage getNearlineStorage()
        {
            return _nearlineStorage;
        }

        public void shutdown()
        {
            _nearlineStorage.shutdown();
        }
    }

    /**
     * Returns an unmodifiable view of the HSM instance names.
     *
     * Notice that the set is not Serializable.
     */
    public Set<String> getHsmInstances()
    {
        return unmodifiableSet(_hsm.keySet());
    }


    public NearlineStorage getNearlineStorageByName(String name)
    {
        HsmInfo info = _hsm.get(name);
        return (info != null) ? info.getNearlineStorage() : null;
    }

    public NearlineStorage getNearlineStorageByType(String type)
    {
        Collection<HsmInfo> infos = _hsm.values();
        return infos.stream()
                .filter(hsm -> hsm.getType().equals(type))
                .map(HsmInfo::getNearlineStorage)
                .findFirst().orElse(null);
    }

    /**
     * Returns the name of an HSM accessible for this pool and which
     * contains the given file.
     */
    public String getInstanceName(FileAttributes fileAttributes) throws FileNotInCacheException
    {
        StorageInfo file = fileAttributes.getStorageInfo();
        if (file.locations().isEmpty() && _hsm.containsKey(fileAttributes.getHsm())) {
            // This is for backwards compatibility.
            return fileAttributes.getHsm();
        }
        for (URI location : file.locations()) {
            if (_hsm.containsKey(location.getAuthority())) {
                return location.getAuthority();
            }
        }
        throw new FileNotInCacheException("Pool does not have access to any of the HSM locations " + file.locations());
    }

    @AffectsSetup
    @Command(name = "hsm create", hint = "create nearline storage",
            description = "Creates a nearline storage. A nearline storage is dCache's interface to external " +
                          "storage providers such as tape systems. Files are copied to (flush) and from (stage) " +
                          "nearline storages.")
    public class CreateCommand implements Callable<String>
    {
        @Argument(index = 0,
                usage = "The nearline storage type is usually determined by the storage-info-extractor " +
                        "used by the pnfs manager. This matches the string after the @ in a storage unit and " +
                        "is typically either 'osm' or 'enstore'.")
        String type;

        @Argument(index = 1, required = false,
                usage = "Uniquely identifies the nearline storage. Defaults to the HSM type, " +
                        "but should be set if interfaces with multiple nearline storages. If " +
                        "multiple pools interact with the same nearline storage, these should " +
                        "use the same instance name.")
        String instance;

        @Argument(index = 2, required = false,
                usage = "Nearline storage providers are pluggable drivers for the nearline storage interface. " +
                        "The provider handles flushing to and staging from the nearline storage as well " +
                        "deleting files.")
        String provider = DEFAULT_PROVIDER;

        @CommandLine(allowAnyOption = true,
                usage = "Provider specific options.")
        Args options;

        @Override
        public String call() throws Exception
        {
            String instance = (this.instance == null) ? type : this.instance;
            if (_isReadingSetup) {
                if (_newConfig.containsKey(instance)) {
                    throw new IllegalArgumentException("Nearline storage already exists: " + instance);
                }
                HsmInfo info = _hsm.get(instance);
                if (info == null) {
                    info = new HsmInfo(instance, type, provider);
                }
                info.scanOptions(options);
                _newConfig.put(instance, info);
            } else {
                if (_hsm.containsKey(instance)) {
                    throw new IllegalArgumentException("Nearline storage already exists: " + instance);
                }
                HsmInfo info = new HsmInfo(instance, type, provider);
                info.scanOptions(options);
                _hsm.put(instance, info);
            }
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "hsm set", hint = "set nearline storage options",
            description = "Sets options of a nearline storage. See the nearline storage provider " +
                          "documentation for information on supported options.")
    public class SetCommand implements Callable<String>
    {
        @Argument(usage = "Nearline storage instance name.")
        String instance;

        @CommandLine(allowAnyOption = true,
                usage = "Provider specific options.")
        Args options;

        @Override
        public String call() throws Exception
        {
            HsmInfo info = _isReadingSetup ? _newConfig.get(instance) : _hsm.get(instance);
            if (info == null) {
                throw new IllegalArgumentException(instance + ": No such nearline storage. You may need to run 'hsm create'.");
            }
            info.scanOptions(options);
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "hsm unset", hint = "unset nearline storage options",
            description = "Unsets options of a nearline storage.")
    public class UnsetCommand implements Callable<String>
    {
        @Argument(usage = "Nearline storage instance name.")
        String instance;

        @CommandLine(allowAnyOption = true, valueSpec = "-KEY ...",
                usage = "Provider specific options.")
        Args options;

        @Override
        public String call() throws Exception
        {
            HsmInfo info = _isReadingSetup ? _newConfig.get(instance) : _hsm.get(instance);
            if (info == null) {
                throw new IllegalArgumentException(instance + ": No such nearline storage. You may need to run 'hsm create'.");
            }
            info.scanOptionsUnset(options);
            return "";
        }

    }

    @Command(name = "hsm ls", hint = "list nearline storages",
            description = "Lists all nearline storages defined on this pool.")
    public class LsCommand implements Callable<String>
    {
        @Argument(usage = "Limit output to these instances.")
        String[] instances;

        @Override
        public String call() throws Exception
        {
            StringBuilder sb = new StringBuilder();
            if (instances.length > 0) {
                for (String instance : instances) {
                    printInfos(sb, instance);
                }
            } else {
                for (String name : _hsm.keySet()) {
                    printInfos(sb, name);
                }
            }
            return sb.toString();
        }
    }

    @AffectsSetup
    @Command(name = "hsm remove", hint = "remove nearlinestorage definition",
            description = "Deletes the nearline storage definition from this pool.")
    public class RemoveCommand implements Callable<String>
    {
        @Argument(usage = "Nearline storage instance name.")
        String instance;

        @Override
        public String call() throws Exception
        {
            HsmInfo info = (_isReadingSetup ? _newConfig : _hsm).remove(instance);
            if (info != null) {
                info.shutdown();
            }
            return "";
        }
    }

    @Command(name = "hsm show providers", hint = "list available providers",
            description = "Nearline storage providers are pluggable. Third party plugins may " +
                          "use the nearline storage SPI to implement custom nearline storage " +
                          "drivers.")
    public class ShowProvidersCommand implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            ColumnWriter writer = new ColumnWriter();
            writer.header("PROVIDER").left("provider").space();
            writer.header("DESCRIPTION").left("description");
            for (NearlineStorageProvider provider : PROVIDERS) {
                writer.row()
                        .value("provider", provider.getName())
                        .value("description", provider.getDescription());
            }
            return writer.toString();
        }
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        if (!_hsm.isEmpty()) {
            pw.println("#\n# Nearline storage\n#");
            for (HsmInfo info : _hsm.values()) {
                pw.print("hsm create ");
                pw.print(info.getType());
                pw.print(" ");
                pw.print(info.getInstance());
                pw.print(" ");
                pw.print(info.getProvider());
                for (Map.Entry<String, String> entry : info.attributes()) {
                    pw.print(" -");
                    pw.print(entry.getKey());
                    if (entry.getValue() != null) {
                        pw.print("=");
                        pw.print(entry.getValue());
                    }
                }
                pw.println();
            }
        }
    }

    @Override
    public synchronized void beforeSetup()
    {
        _isReadingSetup = true;
    }

    @Override
    public synchronized void afterSetup()
    {
        _isReadingSetup = false;

        /* Remove the stores that are not in the new configuration.
         */
        Iterator<HsmInfo> iterator =
                Maps.filterKeys(_hsm, not(in(_newConfig.keySet()))).values().iterator();
        while (iterator.hasNext()) {
            iterator.next().shutdown();
            iterator.remove();
        }

        /* Apply configuration changes
         */
        for (HsmInfo hsm : _newConfig.values()) {
            hsm.refresh();
        }

        _hsm.putAll(_newConfig);
        _newConfig.clear();
    }

    @PreDestroy
    public void shutdown()
    {
        for (HsmInfo info : _hsm.values()) {
            info.shutdown();
        }
    }

    private void printInfos(StringBuilder sb, String instance)
    {
        assert instance != null;

        HsmInfo info = _hsm.get(instance);
        if (info == null) {
            sb.append(instance).append(" not found\n");
        } else {
            sb.append(instance).append("(").append(info.getType())
                    .append("):").append(info.getProvider()).append('\n');
            for (Map.Entry<String,String> entry : info.attributes()) {
                String attrName  = entry.getKey();
                String attrValue = entry.getValue();
                sb.append("   ").
                    append(Formats.field(attrName,20,Formats.LEFT)).
                    append(attrValue == null ? "<set>" : attrValue).
                    append('\n');
            }
        }
    }
}
