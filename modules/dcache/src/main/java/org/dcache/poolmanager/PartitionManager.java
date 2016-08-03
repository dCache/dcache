package org.dcache.poolmanager;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Formatter;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Callable;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;
import dmg.util.command.Option;

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
    implements Serializable, CellCommandListener, CellSetupProvider, CellLifeCycleAware
{
    private static final long serialVersionUID = 3245564135066081407L;

    private static final String DEFAULT = "default";

    private static final ServiceLoader<PartitionFactory> _factories =
        ServiceLoader.load(PartitionFactory.class);

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
            ImmutableMap.of(DEFAULT, new WassPartition());
    }

    private PartitionFactory getFactory(String type)
        throws NoSuchElementException
    {
        return find(_factories, compose(equalTo(type), PartitionFactory::getType));
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
                ImmutableMap.copyOf(transformValues(_partitions,
                                                    partition -> partition.updateInherited(_inheritedProperties)));
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

    @Command(name = "pmx get map",
            hint = "get partition map",
            description = "Internal command to query the internal representation of " +
                    "all partitions.")
    public class PmxGetMapCommand implements Callable<ImmutableMap<String,Partition>>
    {
        @Override
        public ImmutableMap<String,Partition> call()
        {
            return _partitions;
        }
    }

    public ImmutableMap<String,Partition> getPartitions()
    {
        return _partitions;
    }

    @AffectsSetup
    @Command(name = "pm set",
            hint = "set partition parameters",
            description = "Set one or more parameters on a partition. If no partition " +
                          "name is provided, then the set of inherited parameters is updated. " +
                          "These parameters are inherited by all partitions except for those " +
                          "that explicitly redefine the parameters.\n\n" +

                          "Setting a parameter to the value 'off' resets it back to inherited " +
                          "value or back to the default parameter value.")
    public class PmSetCommand implements Callable<String>
    {
        @CommandLine(allowAnyOption = true,
                usage = "Partition type specific options. Use 'pm ls -a' to discover available options.")
        Args args;

        @Argument(required = false,
                usage = "The name of the partition to set its corresponding parameter and " +
                        "values.")
        String partition;

        @Option(name = "p2p-allowed", category = "Common options", values = {"yes", "no", "off"})
        String p2pAllowed;

        @Option(name = "p2p-oncost", category = "Common options", values = {"yes", "no", "off"})
        String p2pOncost;

        @Option(name = "p2p-fortransfer", category = "Common options", values = {"yes", "no", "off"})
        String p2pFortransfer;

        @Option(name = "stage-allowed", category = "Common options", values = {"yes", "no", "off"})
        String stageAllowed;

        @Option(name = "stage-oncost", category = "Common options", values = {"yes", "no", "off"})
        String stageOncost;

        @Override
        public String call() throws IllegalArgumentException
        {
            setProperties(partition, scanProperties(args));
            return "";
        }
    }

    @Command(name = "pm types",
            hint = "list available partition types",
            description = "List partition types that can be used when creating new " +
                    "partitions. The pool selection algorithm, the configuration " +
                    "parameters, and the default values are defined by the partition " +
                    "type.\n\n" +
                    "Partition types are pluggable and new partition types can " +
                    "be added through a plugin mechanism.")
    public class PmTypesCommand implements Callable<String>
    {
        @Override
        public String call()
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
    }

    @AffectsSetup
    @Command(name = "pm create",
            hint = "create a new partition",
            description = "Creates a pool manager partition named <partition> of <type>. " +
                    "If no <type> is specified then 'wass' is used as a default type.\n\n" +

                    "A partition encapsulates configuration parameters and pool " +
                    "selection logic. Each pool manager link identifies a partition " +
                    "to use for transfers in that link.\n\n" +

                    "The partition type defines the pool selection logic and the " +
                    "available configuration parameters.\n\n" +

                    "A default partition named 'default' exists and is used for " +
                    "links that do not explicitly define which partition to use. " +
                    "Creating a partition reusing an existing name overwrites that " +
                    "partition. This allows the type of the default partition to be " +
                    "redefined. Any parameter values on the partition are lost.")
    public class PmCreateCommand implements Callable<String>
    {
        @Argument(usage = "The name of the partition to be created.")
        String partition;

        @Option(name = "type",
                usage = "The partition type to create. Use 'pm types' to see list of " +
                        "available partition types.")
        String type = "wass";

        @Override
        public String call() throws CommandException
        {
            PartitionFactory factory;
            try {
                factory = getFactory(type);
            } catch (NoSuchElementException e) {
                throw new CommandException(1, "Unknown partition type \"" + type + "\"");
            }
            createPartition(factory, partition);
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "pm destroy",
            hint = "remove a partition",
            description = "Remove the specified pool manager partition. Links using the " +
                    "partition will fall back to the default partition. Any parameter " +
                    "values associated with the partition are lost.")
    public class PmDestroyCommand implements Callable<String>
    {
        @Argument(usage = "The name of the partition to remove.")
        String partition;

        @Override
        public String call() throws IllegalArgumentException
        {
            destroyPartition(partition);
            return "";
        }
    }

    @Command(name = "pm ls",
            hint = "list all partitions",
            description = "List information about the <partition>. If no <partition> is " +
                    "specified then information about all partitions is given.")
    public class PmLsCommand implements Callable<String>
    {
        @Argument(required = false, metaVar = "",
                usage = "The name of the partition to list the info.")
        String name;

        @Option(name = "a",
                usage = "List all parameters. The default is not to list inherited " +
                        "and default parameters.")
        boolean showAll;

        @Option(name = "l", usage = "List parameters (default when <partition> is specified).")
        boolean showMore;

        @Override
        public String call() throws IllegalArgumentException
        {
            StringBuilder sb = new StringBuilder();
            if ( name != null ) {
                Partition partition = _partitions.get(name);
                if (partition == null) {
                    throw new IllegalArgumentException("Partition not found: " + name);
                }

                sb.append(name).append(" (").append(partition.getType()).append(")\n");
                printProperties(sb, partition, showAll);
            } else {
                boolean showProperties = showMore || showAll;
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

    private synchronized void writeObject(ObjectOutputStream stream) throws IOException
    {
        stream.defaultWriteObject();
    }
}
