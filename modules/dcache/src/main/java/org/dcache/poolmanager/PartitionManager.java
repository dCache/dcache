package org.dcache.poolmanager;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Formatter;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.Set;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.CommandException;

import org.dcache.util.Args;

import static com.google.common.base.Predicates.*;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Maps.*;

/**
 * Manages one or more pool manager partitions.
 *
 * A partition encapsulates configuration properties and pool
 * selection logic. Pool manager maintains a default partition, but
 * additional named partitions may be defined. Pool manager links
 * identify a partition to use for transfers in that link.
 *
 * A set of shared properties is maintained by PartitionManager. These
 * properties are inherited by all partitions, including the default
 * partition.
 */
public class PartitionManager
    implements Serializable,
               CellCommandListener,
               CellSetupProvider
{
    private static final long serialVersionUID = 3245564135066081407L;

    private static final String DEFAULT = "default";

    private static final ServiceLoader<PartitionFactory> _factories =
        ServiceLoader.load(PartitionFactory.class);

    private static final Function<PartitionFactory,String> getType =
        new Function<PartitionFactory,String>()
        {
            @Override
            public String apply(PartitionFactory factory)
            {
                return factory.getType();
            }
        };

    /**
     * Properties inherited by all partitions. Each partition may
     * override any of the properties.
     */
    private ImmutableMap<String,String> _inheritedProperties =
        ImmutableMap.of();

    /**
     * When true the default partition's properties and the set of
     * inherited properties are the same. Ie. the default partition
     * cannot override any of the inherited properties.
     *
     * Once an explicit default partition is created, this field is set
     * to false.
     *
     * This feature provides backwards compatibility with dCache
     * 1.9.12: That version wrote PoolManager.conf files with the
     * assumption that a property on the default partition would be
     * inherited by other partitions. Once the PoolManager.conf file
     * is regenerated with a newer version of dCache, an explicit
     * default partition is created. Support for this feature can be
     * removed in dCache 2.3.
     */
    private volatile boolean _hasImplicitDefaultPartition;

    /**
     * Partition manager manages a set of named partitions.
     *
     * Like Partitions themself the map over partitions uses copy on
     * write semantics. Any property update requires that the complete
     * map and affected Partitions are recreated.
     */
    private volatile ImmutableMap<String,Partition> _partitions =
        ImmutableMap.of();

    public PartitionManager()
    {
        clear();
    }

    public synchronized void clear()
    {
        _hasImplicitDefaultPartition = true;
        _inheritedProperties = ImmutableMap.of();
        _partitions =
            ImmutableMap.of(DEFAULT, (Partition) new WassPartition());
    }

    private PartitionFactory getFactory(String type)
        throws NoSuchElementException
    {
        return find(_factories, compose(equalTo(type), getType));
    }

    private Function<Partition,Partition>
        updateInherited(final Map<String,String> properties)
    {
        return new Function<Partition,Partition>() {
            @Override
            public Partition apply(Partition partition) {
                return partition.updateInherited(properties);
            }
        };
    }

    public synchronized void setProperties(String name,
                                           Map<String,String> properties)
        throws IllegalArgumentException
    {
        if (name == null || (_hasImplicitDefaultPartition && name.equals(DEFAULT))) {
            _inheritedProperties =
                ImmutableMap.<String,String>builder()
                .putAll(filterKeys(_inheritedProperties,
                                   not(in(properties.keySet()))))
                .putAll(filterValues(properties, notNull()))
                .build();
            _partitions =
                ImmutableMap.copyOf(transformValues(_partitions, updateInherited(_inheritedProperties)));
        } else if (_partitions.containsKey(name)) {
            _partitions =
                ImmutableMap.<String,Partition>builder()
                .putAll(filterKeys(_partitions, not(equalTo(name))))
                .put(name, _partitions.get(name).updateProperties(properties))
                .build();
        } else {
            throw new IllegalArgumentException("No such partition: " + name);
        }
    }

    public synchronized void
        createPartition(PartitionFactory factory, String name)
        throws IllegalArgumentException, NoSuchElementException
    {
        Partition partition = factory.createPartition(_inheritedProperties);
        _partitions =
            ImmutableMap.<String,Partition>builder()
            .putAll(filterKeys(_partitions, not(equalTo(name))))
            .put(name, partition)
            .build();
        if (name.equals(DEFAULT)) {
            _hasImplicitDefaultPartition = false;
        }
    }

    public synchronized void destroyPartition(String name)
        throws IllegalArgumentException
    {
        if (name.equals(DEFAULT)) {
            throw new IllegalArgumentException("Cannot destroy default partition");
        }

        if (!_partitions.containsKey(name)) {
            throw new IllegalArgumentException("No such partition: " + name);
        }

        _partitions =
            ImmutableMap.<String,Partition>builder()
            .putAll(filterKeys(_partitions, not(equalTo(name))))
            .build();
    }

    public Partition getDefaultPartition()
    {
        return _partitions.get(DEFAULT);
    }

    public Partition getPartition(String name)
    {
        if (name == null) {
            return getDefaultPartition();
        }
        Partition partition = _partitions.get(name);
        if (partition == null) {
            return getDefaultPartition();
        }
        return partition;
    }

    public final static String fh_pmx_get_map =
        "Name:\n" +
        "    pmx get map - get partition map\n" +
        "\n" +
        "Synopsis:\n" +
        "    pmx get map\n" +
        "\n" +
        "Description:\n" +
        "    Internal command to query the internal representation of all\n" +
        "    partitions.\n";
    public final static String hh_pmx_get_map = "# get partition map";
    public Object ac_pmx_get_map(Args args)
    {
       return _partitions;
    }

    public ImmutableMap<String,Partition> getPartitions()
    {
        return _partitions;
    }

    public final static String fh_pm_set =
        "Name:\n" +
        "    pm set - set partition parameters\n" +
        "\n" +
        "Synopsis:\n" +
        "    pm set [<partition>] OPTION...\n" +
        "\n" +
        "Description:\n" +
        "    Set one or more parameters on a partition.\n" +
        "\n" +
        "    If no partition name is provided, then the set of inherited\n" +
        "    parameters is updated. These parameters are inherited by all\n" +
        "    partitions except for those that explicitly redefine the\n" +
        "    parameters.\n" +
        "\n" +
        "    Setting a parameter to the value 'off' resets it back to\n" +
        "    inherited value or back to the default parameter value.\n" +
        "\n" +
        "    Available parameters depend on the partition type. Use\n" +
        "    'pm ls -a' to see available parameters and their current\n" +
        "    value. A small set of parameters are common to all partition\n" +
        "    types. These are:\n" +
        "\n" +
        "       -p2p-allowed=yes|no|off\n"+
        "       -p2p-oncost=yes|no|off\n"+
        "       -p2p-fortransfer=yes|no|off\n"+
        "       -stage-allowed=yes|no|off\n"+
        "       -stage-oncost=yes|no|off\n";
    public final static String hh_pm_set =
        "[<partition>] OPTION... # set partition parameters";
    public String ac_pm_set_$_0_1(Args args)
        throws IllegalArgumentException
    {
        setProperties(args.argv(0), scanProperties(args));
        return "";
    }

    public final static String fh_pm_types =
        "Name:\n" +
        "    pm types - list available partition types\n" +
        "\n" +
        "Synopsis:\n" +
        "    pm types\n" +
        "\n" +
        "Description:\n" +
        "    List partition types that can be used when creating new\n" +
        "    partitions. The pool selection algorithm, the configuration\n" +
        "    parameters, and the default values are defined by the\n" +
        "    partition type.\n" +
        "\n" +
        "    Partition types are pluggable and new partition types can\n" +
        "    be added through a plugin mechanism.";
    public final static String hh_pm_types =
        "# list available partition types";
    public String ac_pm_types_$_0(Args args)
    {
        final String format = "%-16s %s\n";
        Formatter s = new Formatter(new StringBuilder());
        s.format(format, "Partition type", "Description");
        s.format(format, "--------------", "-----------");
        for (PartitionFactory factory: _factories) {
            s.format(format, factory.getType(), factory.getDescription());
        }
        return s.toString();
    }

    public final static String hh_pm_create =
        "[-type=<type>] <partition> # create new partition";
    public final static String fh_pm_create =
        "Name:\n" +
        "    pm create - create new partition\n" +
        "\n" +
        "Synopsis:\n" +
        "    pm create [-type=<type>] <partition>\n" +
        "\n" +
        "Description:\n" +
        "    Creates a pool manager partition named <partition> of <type>.\n" +
        "    If no <type> is specified then 'wass' is used as a default\n" +
        "    type.\n" +
        "\n" +
        "    A partition encapsulates configuration parameters and pool\n" +
        "    selection logic. Each pool manager link identifies a partition\n" +
        "    to use for transfers in that link.\n" +
        "\n" +
        "    The partition type defines the pool selection logic and the\n" +
        "    available configuration parameters. 'pm types' lists available\n" +
        "    partition types." +
        "\n" +
        "    A default partition named 'default' exists and is used for\n" +
        "    links that do not explicitly define which partition to use.\n" +
        "    Creating a partition reusing an existing name overwrites that\n" +
        "    partition. This allows the type of the default partition to be\n" +
        "    redefined. Any parameter values on the partition are lost.";
    public String ac_pm_create_$_1(Args args) throws CommandException
    {
        String type = args.getOption("type", "wass");
        PartitionFactory factory;
        try {
            factory = getFactory(type);
        } catch (NoSuchElementException e) {
            throw new CommandException(1, "Unknown partition type \"" + type + "\"");
        }
        createPartition(factory,args.argv(0));
        return "";
    }

    public final static String fh_pm_destroy =
        "Name:\n" +
        "    pm destroy - remove partition\n" +
        "\n" +
        "Synopsis:\n" +
        "    pm destroy <partition>\n" +
        "\n" +
        "Description:\n" +
        "    Remove the specified pool manager partition. Links using the\n" +
        "    partition will fall back to the default partition. Any\n" +
        "    parameter values associated with the partition are lost.";
    public final static String hh_pm_destroy =
        "<partition> # remove partition";
    public String ac_pm_destroy_$_1(Args args)
    {
        destroyPartition(args.argv(0));
        return "";
    }

    public final static String fh_pm_ls =
        "Name:\n" +
        "    pm ls - list partitions\n" +
        "\n" +
        "Synopsis:\n" +
        "    pm ls [-l] [-a] [<partition>]\n" +
        "\n" +
        "Description:\n" +
        "    List information about the <partition>. If no <partition> is\n" +
        "    specified then information about all partitions is given.\n" +
        "\n" +
        "    -l  List parameters (default when <partition> is specified).\n" +
        "    -a  List all parameters. The default is not to list inherited\n" +
        "        and default parameters.\n";
    public final static String hh_pm_ls =
        "[-l] [-a] [<partition>] # list partitions";
    public String ac_pm_ls_$_0_1(Args args)
        throws IllegalArgumentException
    {
        boolean showAll = args.hasOption("a");
        StringBuilder sb = new StringBuilder();
        if (args.argc() != 0) {
            String name = args.argv(0);
            Partition partition = _partitions.get(name);
            if (partition == null) {
                throw new IllegalArgumentException("Partition not found: " + name);
            }

            sb.append(name).append(" (").append(partition.getType()).append(")\n");
            printProperties(sb, partition, showAll);
        } else {
            boolean showProperties = args.hasOption("l") || showAll;
            if (showProperties) {
                printInheritedProperties(sb);
            }
            for (Map.Entry<String,Partition> entry: _partitions.entrySet()) {
                sb.append(entry.getKey())
                    .append(" (")
                    .append(entry.getValue().getType())
                    .append(")\n");
                if (showProperties) {
                    printProperties(sb, entry.getValue(), showAll);
                }
            }
        }
        return sb.toString();
    }

    private void printInheritedProperties(StringBuilder sb)
    {
        Set<String> keys = _inheritedProperties.keySet();
        sb.append("Inherited by all partitions\n");
        for (String key: Ordering.<String>natural().sortedCopy(keys)) {
            sb.append("    -").append(key)
                .append("=").append(_inheritedProperties.get(key)).append("\n");
        }
    }

    private void printProperties(StringBuilder sb, Partition partition,
                                 boolean showAll)
    {
        Set<String> all = partition.getAllProperties().keySet();
        Set<String> defined = partition.getProperties().keySet();

        for (String key: Ordering.<String>natural().sortedCopy(all)) {
            if (defined.contains(key) || showAll) {
                sb.append("    -").append(key)
                    .append("=").append(partition.getProperty(key));
                if (defined.contains(key)) {
                    sb.append("\n");
                } else if (_inheritedProperties.containsKey(key)) {
                    sb.append(" [inherited]\n");
                } else {
                    sb.append(" [default]\n");
                }
            }
        }
    }

    private Map<String,String> scanProperties(Args args)
    {
        Map<String,String> map = newHashMap();
        for (Map.Entry<String,String> entry: args.optionsAsMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value.equals("off")) {
                map.put(key, null);
            } else {
                map.put(key, value);
            }
        }
        return map;
    }

    @Override
    public void beforeSetup()
    {
        clear();
    }

    @Override
    public void afterSetup()
    {
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        dumpInfo(pw, "", _inheritedProperties);
        for (Map.Entry<String,Partition> entry: _partitions.entrySet()) {
            pw.format("pm create -type=%s %s\n",
                      entry.getValue().getType(), entry.getKey());
            dumpInfo(pw, entry.getKey(), entry.getValue().getProperties());
        }
    }

    private void dumpInfo(PrintWriter pw, String name,
                          Map<String,String> properties)
    {
        if (!properties.isEmpty()) {
            pw.append("pm set");
            if (!name.isEmpty()) {
                pw.append(" ").append(name);
            }
            for (Map.Entry<String,String> entry: properties.entrySet()) {
                pw.append(" -").append(entry.getKey()).append("=").append(entry.getValue());
            }
            pw.println();
        }
    }
}
