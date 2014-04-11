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

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.PreDestroy;

import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.Formats;

import org.dcache.pool.nearline.script.ScriptNearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;
import org.dcache.util.Args;
import org.dcache.util.ColumnWriter;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableCollection;
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
    implements CellCommandListener,
               CellSetupProvider
{
    private static final ServiceLoader<NearlineStorageProvider> PROVIDERS =
            ServiceLoader.load(NearlineStorageProvider.class);
    private static final String DEFAULT_PROVIDER = "script";

    private final ConcurrentMap<String, HsmInfo> _hsm = Maps.newConcurrentMap();
    private boolean _isReadingSetup;
    private Integer _legacyRemoveConcurrency;
    private Integer _legacyStoreConcurrency;
    private Integer _legacyRestoreConcurrency;

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
        private final Map<String,String> _attr = new HashMap<>();
        private final NearlineStorageProvider _provider;
        private NearlineStorage _nearlineStorage;

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
           return _attr.get(attribute);
        }

        /**
         * Removes an attribute.
         *
         * @param attribute An attribute name
         */
        public synchronized void unsetAttribute(String attribute)
        {
            _attr.remove(attribute);
            if (_nearlineStorage != null) {
                _nearlineStorage.configure(_attr);
            }
        }

        /**
         * Sets an attribute to a value.
         *
         * @param attribute An attribute name
         * @param value     A value string
         */
        public synchronized void setAttribute(String attribute, String value)
        {
           _attr.put(attribute, value);
            if (_nearlineStorage != null) {
                _nearlineStorage.configure(_attr);
            }
        }

        /**
         * Returns the set of attributes.
         */
        public synchronized Iterable<Map.Entry<String, String>> attributes()
        {
            return new ArrayList<>(_attr.entrySet());
        }

        public synchronized NearlineStorage getNearlineStorage()
        {
            if (_nearlineStorage == null) {
                _nearlineStorage = _provider.createNearlineStorage(_type, _instance);
                _nearlineStorage.configure(_attr);
            }
            return _nearlineStorage;
        }

        public synchronized void shutdown()
        {
            if (_nearlineStorage != null) {
                _nearlineStorage.shutdown();
            }
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

    /**
     * Returns information about the named HSM. Returns null if no HSM
     * with this instance name was defined.
     *
     * @param instance an HSM instance name.
     */
    public HsmInfo getHsmInfoByName(String instance)
    {
       return _hsm.get(instance);
    }


    /**
     * Returns an unmodifiable view of the HSMs of a given type.
     *
     * @param type an HSM type name.
     */
    public Collection<HsmInfo> getHsmInfoByType(final String type)
    {
        return unmodifiableCollection(
                Collections2.filter(_hsm.values(),
                                    new Predicate<HsmInfo>()
                                    {
                                        @Override
                                        public boolean apply(HsmInfo hsm)
                                        {
                                            return hsm.getType().equals(type);
                                        }
                                    }));
    }

    public NearlineStorage getNearlineStorageByName(String name)
    {
        HsmInfo info = getHsmInfoByName(name);
        return (info != null) ? info.getNearlineStorage() : null;
    }

    public NearlineStorage getNearlineStorageByType(String type)
    {
        HsmInfo info = Iterables.getFirst(getHsmInfoByType(type), null);
        return (info != null) ? info.getNearlineStorage() : null;
    }

    /**
     * Returns the name of an HSM accessible for this pool and which
     * contains the given file. Returns null if no such HSM exists.
     */
    public String getInstanceName(FileAttributes fileAttributes)
    {
        StorageInfo file = fileAttributes.getStorageInfo();
        if (file.locations().isEmpty() && _hsm.containsKey(file.getHsm())) {
            // This is for backwards compatibility.
            return file.getHsm();
        }
        for (URI location : file.locations()) {
            if (_hsm.containsKey(location.getAuthority())) {
                return location.getAuthority();
            }
        }
        return null;
    }

    /**
     * Removes any information about the named HSM.
     *
     * @param instance An HSM instance name.
     */
    private void removeInfo(String instance)
    {
        HsmInfo info = _hsm.remove(instance);
        if (info != null) {
            info.shutdown();
        }
    }

    /**
     * Scans an argument set for options and applies those as
     * attributes to an HsmInfo object.
     */
    private void scanOptions(HsmInfo info, Args args)
    {
        for (Map.Entry<String,String> e: args.options().entries()) {
            String optName  = e.getKey();
            String optValue = e.getValue();
            info.setAttribute(optName, optValue == null ? "" : optValue);
        }
    }

    /**
     * Scans an argument set for options and removes and unsets those
     * attributes in the given HsmInfo object.
     */
    private void scanOptionsUnset(HsmInfo info, Args args)
    {
        for (String optName: args.options().keySet()) {
            info.unsetAttribute(optName);
        }
    }

    public static final String hh_hsm_create = "<type> [<name> [<provider>]] [-<key>=<value>] ...";
    public String ac_hsm_create_$_1_3(Args args)
    {
        String type = args.argv(0);
        String instance = (args.argc() == 1) ? type : args.argv(1);
        String provider = (args.argc() == 3) ? args.argv(2) : DEFAULT_PROVIDER;
        HsmInfo info = new HsmInfo(instance, type, provider);
        scanOptions(info, args);
        if (_hsm.putIfAbsent(instance, info) != null) {
            throw new IllegalArgumentException("Nearline storage already exists: " + instance);
        }
        return "";
    }

    public static final String hh_hsm_set = "<name> [-<key>=<value>] ...";
    public String ac_hsm_set_$_1_2(Args args)
    {
        if (_isReadingSetup) {
            /* For backwards compatibility with old pool setup files.
             */
            String type = args.argv(0);
            String instance = (args.argc() == 1) ? type : args.argv(1);
            HsmInfo info = new HsmInfo(instance, type, DEFAULT_PROVIDER);
            HsmInfo existing = _hsm.putIfAbsent(instance, info);
            if (existing != null) {
                info = existing;
            }
            scanOptions(info, args);
        } else {
            String instance = args.argv(0);
            HsmInfo info = getHsmInfoByName(instance);
            if (info == null) {
                throw new IllegalArgumentException("No such nearline storage: " + instance);
            }
            scanOptions(info, args);
        }
        return "";
    }

    public static final String hh_hsm_unset = "<name> [-<key>] ...";
    public String ac_hsm_unset_$_1(Args args)
    {
       String instance = args.argv(0);
       HsmInfo info = getHsmInfoByName(instance);
       if (info == null) {
           throw new IllegalArgumentException("No such nearline storage: " + instance);
       }
       scanOptionsUnset(info, args);
       return "";
    }

    public static final String hh_hsm_ls = "[<name>] ...";
    public String ac_hsm_ls_$_0_99(Args args)
    {
       StringBuilder sb = new StringBuilder();
       if (args.argc() > 0) {
          for (int i = 0; i < args.argc(); i++) {
             printInfos(sb, args.argv(i));
          }
       } else {
           for (String name : _hsm.keySet()) {
               printInfos(sb, name);
           }
       }
       return sb.toString();
    }

    public static final String hh_hsm_remove = "<name>";
    public String ac_hsm_remove_$_1(Args args)
    {
       removeInfo(args.argv(0));
       return "";
    }

    public static final String hh_rh_set_max_active = "# Deprecated";
    public String ac_rh_set_max_active_$_1(Args args)
    {
        checkState(_isReadingSetup, "Legacy command only supported in pool setup file.");
        _legacyRestoreConcurrency = Integer.parseInt(args.argv(0));
        return "";
    }

    public static final String hh_st_set_max_active = "# Deprecated";
    public String ac_st_set_max_active_$_1(Args args)
    {
        checkState(_isReadingSetup, "Legacy command only supported in pool setup file.");
        _legacyStoreConcurrency = Integer.parseInt(args.argv(0));
        return "";
    }

    public static final String hh_rm_set_max_active = "# Deprecated";
    public String ac_rm_set_max_active_$_1(Args args)
    {
        checkState(_isReadingSetup, "Legacy command only supported in pool setup file.");
        _legacyRemoveConcurrency = Integer.parseInt(args.argv(0));
        return "";
    }

    public static final String hh_hsm_show_providers = "# show available nearline storage providers";
    public String ac_hsm_show_providers(Args args)
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

    @Override
    public void printSetup(PrintWriter pw)
    {
        for (HsmInfo info : _hsm.values()) {
            pw.print("hsm create ");
            pw.print(info.getType());
            pw.print(" ");
            pw.print(info.getInstance());
            pw.print(" ");
            pw.println(info.getProvider());
            for (Map.Entry<String,String> entry : info.attributes()) {
                pw.print("hsm set ");
                pw.print(info.getInstance());
                pw.print(" -");
                pw.print(entry.getKey());
                pw.print("=");
                pw.println(entry.getValue() == null ? "-" : entry.getValue());
            }
        }
    }

    @Override
    public void beforeSetup()
    {
        _isReadingSetup = true;
    }

    @Override
    public void afterSetup()
    {
        _isReadingSetup = false;
        for (HsmInfo info : _hsm.values()) {
            if (info.getProvider().equals(DEFAULT_PROVIDER)) {
                if (_legacyRestoreConcurrency != null) {
                    info.setAttribute(ScriptNearlineStorage.CONCURRENT_GETS, String.valueOf(_legacyRestoreConcurrency));
                }
                if (_legacyStoreConcurrency != null) {
                    info.setAttribute(ScriptNearlineStorage.CONCURRENT_PUTS, String.valueOf(_legacyStoreConcurrency));
                }
                if (_legacyRemoveConcurrency != null) {
                    info.setAttribute(ScriptNearlineStorage.CONCURRENT_REMOVES, String.valueOf(_legacyRemoveConcurrency));
                }
            }
        }
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

        HsmInfo info = getHsmInfoByName(instance);
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
