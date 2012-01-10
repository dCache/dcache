package org.dcache.poolmanager;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.ServiceLoader;
import java.util.NoSuchElementException;
import java.util.Formatter;

import org.dcache.cells.CellSetupProvider;
import org.dcache.cells.CellCommandListener;

import dmg.util.Args;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.base.Function;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.compose;

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
    static final long serialVersionUID = 3245564135066081407L;

    private final static String DEFAULT = "default";

    private final static ServiceLoader<PartitionFactory> _factories =
        ServiceLoader.load(PartitionFactory.class);

    private final static Function<PartitionFactory,String> getType =
        new Function<PartitionFactory,String>()
        {
            public String apply(PartitionFactory factory)
            {
                return factory.getType();
            }
        };

    /**
     * Properties inherited by all partitions. Each partition may
     * override any of the properties.
     */
    private ImmutableMap<String,String> _properties =
        ImmutableMap.of();

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
        _properties = ImmutableMap.of();
        _partitions =
            ImmutableMap.of(DEFAULT, (Partition) new ClassicPartition());
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
            public Partition apply(Partition partition) {
                return partition.updateInherited(properties);
            }
        };
    }

    public synchronized void setProperties(String name,
                                           Map<String,String> properties)
        throws IllegalArgumentException
    {
        if (name == null || name.equals(DEFAULT)) {
            _properties =
                ImmutableMap.<String,String>builder()
                .putAll(filterKeys(_properties, not(in(properties.keySet()))))
                .putAll(filterValues(properties, notNull()))
                .build();
            _partitions =
                ImmutableMap.copyOf(transformValues(_partitions, updateInherited(_properties)));
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
        if (_partitions.containsKey(name)) {
            throw new IllegalArgumentException("Partition " + name + " already exists.");
        }

        Partition partition = factory.createPartition(_properties);
        _partitions =
            ImmutableMap.<String,Partition>builder()
            .putAll(_partitions)
            .put(name, partition)
            .build();
    }

    public synchronized void destroyPartition(String name)
        throws IllegalArgumentException
    {
        if (name.equals(DEFAULT)) {
            throw new IllegalArgumentException("Can't destroy default parameter partition");
        }

        if (!_partitions.containsKey(name)) {
            throw new IllegalArgumentException("No such parameter partition " + name);
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

    public final static String hh_pmx_get_map = "";
    public Object ac_pmx_get_map(Args args)
    {
       return _partitions;
    }

    public ImmutableMap<String,Partition> getPartitions()
    {
        return _partitions;
    }

    public final static String fh_pm_set =
       "pm set [<partitionName>] OPTIONS\n"+
       "    OPTIONS\n"+
       "       -spacecostfactor=<scf>|off\n"+
       "       -cpucostfactor=<ccf>|off\n"+
       "       -max-copies=<max-replicas>|off\n"+
       "       -idle=<value>|off\n"+
       "       -p2p=<value>|off\n"+
       "       -alert=<value>|off\n"+
       "       -panic=<value>|off\n"+
       "       -fallback=<value>|off\n"+
       "       -slope=<value>|off\n"+
       "       -p2p-allowed=yes|no|off\n"+
       "       -p2p-oncost=yes|no|off\n"+
       "       -p2p-fortransfer=yes|no|off\n"+
       "       -stage-allowed=yes|no|off\n"+
       "       -stage-oncost=yes|no|off\n"+
       "";
    public final static String hh_pm_set =
        "[<partitionName>] OPTIONS #  help pm set" ;
    public String ac_pm_set_$_0_1(Args args)
        throws IllegalArgumentException
    {
        setProperties(args.argv(0), scanParameter(args));
        return "";
    }

    public String ac_pm_types_$_0(Args args)
    {
        final String format = "%-16s %s\n";
        Formatter s = new Formatter(new StringBuilder());
        s.format(format, "Partition type", "Description");
        s.format(format, "--------------", "-----------");
        StringBuilder sb = new StringBuilder();
        for (PartitionFactory factory: _factories) {
            s.format(format, factory.getType(), factory.getDescription());
        }
        return s.toString();
    }

    public final static String hh_pm_create =
        "[-type=<partitionType>] <partitionName>";
    public String ac_pm_create_$_1(Args args)
    {
        String type = args.getOption("type");
        PartitionFactory factory =
            getFactory((type == null) ? "classic" : type);
        createPartition(factory,args.argv(0));
        return "";
    }

    public final static String hh_pm_destroy =
        "<partitionName> # destroys parameter partition";
    public String ac_pm_destroy_$_1(Args args)
    {
        destroyPartition(args.argv(0));
        return "";
    }

    public final static String hh_pm_ls = "[-l] [<partitionName>]";
    public String ac_pm_ls_$_0_1(Args args)
        throws IllegalArgumentException
    {
        StringBuilder sb = new StringBuilder();
        if (args.argc() != 0) {
            String name = args.argv(0);
            Partition partition = _partitions.get(name);
            if (partition == null) {
                throw new IllegalArgumentException("Section not found: " + name);
            }

            sb.append(name).append(" (").append(partition.getType()).append(")\n");
            printProperties(sb, partition);
        } else {
            for (Map.Entry<String,Partition> entry: _partitions.entrySet()) {
                sb.append(entry.getKey())
                    .append(" (")
                    .append(entry.getValue().getType())
                    .append(")\n");
                if (args.hasOption("l")) {
                    printProperties(sb, entry.getValue());
                }
            }
        }
        return sb.toString();
    }

    private void printProperties(StringBuilder sb, Partition partition)
    {
        Set<String> all = partition.getAllProperties().keySet();
        Set<String> defined = partition.getProperties().keySet();

        for (String key: Ordering.<String>natural().sortedCopy(all)) {
            sb.append("    -").append(key)
                .append("=").append(partition.getProperty(key));
            if (defined.contains(key)) {
                sb.append("\n");
            } else if (_properties.containsKey(key)) {
                sb.append(" [inherited]\n");
            } else {
                sb.append(" [default]\n");
            }
        }
    }

    private Map<String,String> scanParameter(Args args)
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
        dumpInfo(pw, "", _properties);
        for (Map.Entry<String,Partition> entry: _partitions.entrySet()) {
            if (!entry.getKey().equals(DEFAULT)) {
                pw.format("pm create -type=%s %s\n",
                          entry.getValue().getType(), entry.getKey());
            }
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
