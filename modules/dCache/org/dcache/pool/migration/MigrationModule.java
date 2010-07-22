package org.dcache.pool.migration;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.Collection;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Comparator;
import java.io.StringWriter;
import java.io.PrintWriter;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheFileAvailable;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellStub;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.util.Interval;
import org.dcache.util.Glob;

import dmg.util.Args;
import dmg.cells.nucleus.CellEndpoint;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Module for migrating files between pools.
 *
 * This module provides services for copying replicas from a source
 * pool to a set of target pools.  The repository state and sticky
 * list of both the source replica and the target replica can be
 * defined, supporting several use cases, including migration,
 * replication and caching.
 *
 * The module consists of two components: The MigrationModule class
 * provides the user interface and must run on the source pool. The
 * MigrationModuleServer must run on any pool that is to be used as a
 * transfer destination.
 *
 * Most of the functionality is implemented on the source pool. The
 * user executes commands to define jobs. A job consists of rules for
 * selecting replicas on the source pool, for selecting target pools,
 * defines the state of the target replica, and how the state of the
 * source replica must be updated.
 *
 * A job is idempotent, that is, it can be repeated without ill
 * effect. This is achieved by querying the set of target pools for
 * existing copies of the replica. If found, the transfer may be
 * skipped. Care is taken to check the state of the replica on the
 * target pool - and updating it if necessary.
 *
 * Jobs monitor the local repository for changes. If a replica changes
 * state before it is transfered, and the replica no longer passes the
 * selection criteria of the job, then it will not be transferred. If
 * it is in the process of being transferred, then the transfer is
 * cancelled. If the transfer has already completed, then nothing
 * happens.
 *
 * Jobs can be defined as permanent. A permanent job will monitor the
 * repository for state changes. Should a replica be added or change
 * state in such a way that is passes the selection criteria of the
 * job, then it is added to the transfer queue of the job. A permanent
 * job does not terminate, even if its transfer queue becomes
 * empty. Permanent jobs are saved to the pool setup file and restored
 * on pool start.
 *
 * Each job schedules transfer tasks. Whereas a job defines a bulk
 * operation, a task encapsulates a transfer of a single replica.
 *
 * Most classes in this package are thread safe. Non of the classes
 * create threads themselves. Instead they rely on an injected
 * ScheduledExecutorService. Most cell communication is implemented
 * asynchronously.
 */
public class MigrationModule
    extends AbstractCellComponent
    implements CellCommandListener,
               CellMessageReceiver
{
    private final static Logger _log =
        LoggerFactory.getLogger(MigrationModule.class);

    private final static PoolManagerPoolInformation DUMMY_POOL =
        new PoolManagerPoolInformation("pool", 0.0, 0.0);

    private final List<Job> _alive = new ArrayList();
    private final Map<Integer,Job> _jobs = new HashMap();
    private final Map<Job,String> _commands = new HashMap();
    private final MigrationContext _context = new MigrationContext();
    private final JexlEngine _jexl = new JexlEngine() {
            @Override
            protected Object doCreateInstance(Object clazz, Object...args)
            {
                throw new JexlException(debugInfo(),
                                        "Object creation is not supported");
            }
        };

    private final Expression TRUE_EXPRESSION =
        _jexl.createExpression("true");
    private final Expression FALSE_EXPRESSION =
        _jexl.createExpression("false");

    private int _counter = 1;

    public void setCellEndpoint(CellEndpoint endpoint)
    {
        super.setCellEndpoint(endpoint);
        _context.setPoolName(getCellName());
    }

    public void setRepository(Repository repository)
    {
        _context.setRepository(repository);
    }

    public void setExecutor(ScheduledExecutorService executor)
    {
        _context.setExecutor(executor);
    }

    public void setPnfsStub(CellStub stub)
    {
        _context.setPnfsStub(stub);
    }

    public void setPoolManagerStub(CellStub stub)
    {
        _context.setPoolManagerStub(stub);
    }

    public void setPoolStub(CellStub stub)
    {
        _context.setPoolStub(stub);
    }

    public void setPinManagerStub(CellStub stub)
    {
        _context.setPinManagerStub(stub);
    }

    /** Returns the job with the given id. */
    private synchronized Job getJob(int id)
        throws NoSuchElementException
    {
        Job job = _jobs.get(id);
        if (job == null) {
            throw new NoSuchElementException("Job not found");
        }
        return job;
    }

    /** Returns a one line description of a job. */
    private synchronized String getJobSummary(int id)
    {
        Job job = getJob(id);
        return String.format("[%d] %-12s %s", id, job.getState(),
                             _commands.get(job));
    }

    private List<CacheEntryFilter> createFilters(Args args)
        throws IllegalArgumentException, NumberFormatException
    {
        String state = args.getOpt("state");
        String sticky = args.getOpt("sticky");
        String sc = args.getOpt("storage");
        String pnfsid = args.getOpt("pnfsid");
        String accessed = args.getOpt("accessed");
        String size = args.getOpt("size");
        String al = args.getOpt("al");
        String rp = args.getOpt("rp");

        List<CacheEntryFilter> filters = new ArrayList();

        if (sc != null) {
            filters.add(new StorageClassFilter(sc));
        }

        if (pnfsid != null) {
            Collection<PnfsId> ids = new HashSet<PnfsId>();
            for (String id: pnfsid.split(",")) {
                ids.add(new PnfsId(id));
            }
            filters.add(new PnfsIdFilter(ids));
        }

        if (state == null) {
            filters.add(new StateFilter(EntryState.CACHED, EntryState.PRECIOUS));
        } else if (state.equals("cached")) {
            filters.add(new StateFilter(EntryState.CACHED));
        } else if (state.equals("precious")) {
            filters.add(new StateFilter(EntryState.PRECIOUS));
        } else {
            throw new IllegalArgumentException(state + ": Invalid state");
        }

        if (sticky != null) {
            if (sticky.equals("")) {
                filters.add(new StickyFilter());
            } else {
                for (String owner: sticky.split(",")) {
                    filters.add(new StickyOwnerFilter(owner));
                }
            }
        }

        if (size != null) {
            filters.add(new SizeFilter(Interval.parseInterval(size)));
        }

        if (accessed != null) {
            filters.add(new AccessedFilter(Interval.parseInterval(accessed)));
        }

        if (al != null) {
            filters.add(new AccessLatencyFilter(AccessLatency.getAccessLatency(al)));
        }

        if (rp != null) {
            filters.add(new RetentionPolicyFilter(RetentionPolicy.getRetentionPolicy(rp)));
        }

        return filters;
    }

    private RefreshablePoolList
        createPoolList(double spaceCost,
                       double cpuCost,
                       String type,
                       List<String> targets,
                       Collection<Pattern> exclude,
                       Expression excludeWhen,
                       Collection<Pattern> include,
                       Expression includeWhen)
    {
        CellStub poolManager = _context.getPoolManagerStub();

        if (type.equals("pool")) {
            return new FixedPoolList(targets);
        } else if (type.equals("pgroup")) {
            if (targets.size() != 1) {
                throw new IllegalArgumentException(targets.toString() +
                                                   ": Only one target supported for -type=pgroup");
            }
            return new PoolListByPoolGroup(poolManager,
                                           exclude,
                                           excludeWhen,
                                           include,
                                           includeWhen,
                                           spaceCost,
                                           cpuCost,
                                           targets.get(0));
        } else if (type.equals("link")) {
            if (targets.size() != 1) {
                throw new IllegalArgumentException(targets.toString() +
                                                   ": Only one target supported for -type=link");
            }
            return new PoolListByLink(poolManager,
                                      exclude,
                                      excludeWhen,
                                      include,
                                      includeWhen,
                                      spaceCost,
                                      cpuCost,
                                      targets.get(0));
        } else {
            throw new IllegalArgumentException(type + ": Invalid value");
        }
    }

    private PoolSelectionStrategy
        createPoolSelectionStrategy(String type)
    {
        if (type.equals("proportional")) {
            return new ProportionalPoolSelectionStrategy();
        } else if (type.equals("best")) {
            return new BestPoolSelectionStrategy();
        } else if (type.equals("random")) {
            return new RandomPoolSelectionStrategy();
        } else {
            throw new IllegalArgumentException(type + ": Invalid value");
        }
    }

    private final static Pattern STICKY_PATTERN =
        Pattern.compile("(\\w+)(\\((-?\\d+)\\))?");

    private StickyRecord parseStickyRecord(String s)
        throws IllegalArgumentException
    {
        Matcher matcher = STICKY_PATTERN.matcher(s);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(s + ": Syntax error");
        }
        String owner = matcher.group(1);
        String lifetime = matcher.group(3);
        try {
            long expire = (lifetime == null) ? -1 : Integer.valueOf(lifetime);
            if (expire < -1) {
                throw new IllegalArgumentException(lifetime + ": Invalid lifetime");
            } else if (expire > 0) {
                expire = System.currentTimeMillis() + expire * 1000;
            }
            return new StickyRecord(owner, expire);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(lifetime + ": Invalid lifetime");
        }
    }

    private CacheEntryMode
        createCacheEntryMode(String type)
    {
        String[] s = type.split("\\+");
        List<StickyRecord> records = new ArrayList();

        for (int i = 1; i < s.length; i++) {
            records.add(parseStickyRecord(s[i]));
        }

        if (s[0].equals("same")) {
            return new CacheEntryMode(CacheEntryMode.State.SAME, records);
        } else if (s[0].equals("cached")) {
            return new CacheEntryMode(CacheEntryMode.State.CACHED, records);
        } else if (s[0].equals("delete")) {
            return new CacheEntryMode(CacheEntryMode.State.DELETE, records);
        } else if (s[0].equals("removable")) {
            return new CacheEntryMode(CacheEntryMode.State.REMOVABLE, records);
        } else if (s[0].equals("precious")) {
            return new CacheEntryMode(CacheEntryMode.State.PRECIOUS, records);
        } else {
            throw new IllegalArgumentException(type + ": Invalid value");
        }
    }

    private Comparator<CacheEntry> createComparator(String order)
    {
        if (order == null) {
            return null;
        } else if (order.equals("size")) {
            return new SizeOrder();
        } else if (order.equals("-size")) {
            return new ReverseOrder(new SizeOrder());
        } else if (order.equals("lru")) {
            return new LruOrder();
        } else if (order.equals("-lru")) {
            return new ReverseOrder(new LruOrder());
        } else {
            throw new IllegalArgumentException(order + ": Invalid value for option -order");
        }
    }

    /**
     * Checks that a JEXL expression evaluates to a boolean within a
     * given context. If the expression has side effects, then the
     * context will be updated.
     *
     * @throw JexlException if the expression cannot be evaluated
     * @throw IllegalArgumentException if the expression does not
     *                                 evaluate to a boolean
     */
    private void assertExpressionEvaluatesToBoolean(Expression expression,
                                                    JexlContext context)
        throws JexlException
    {
        Object result = expression.evaluate(context);
        if (!(result instanceof Boolean)) {
            throw new IllegalArgumentException(expression.getExpression() +
                                               ": The expression does not evaluate to a boolean");
        }
    }

    private Expression createPoolExpression(String s, Expression ifNull)
    {
        try {
            if (s == null) {
                return ifNull;
            }

            Expression expression = _jexl.createExpression(s);

            MapContextWithConstants context = new MapContextWithConstants();
            context.addConstant("target", new PoolValues(DUMMY_POOL));
            assertExpressionEvaluatesToBoolean(expression, context);

            return expression;
        } catch (JexlException e) {
            throw new IllegalArgumentException(s + ": " + e.getMessage(), e);
        }
    }

    private Set<Pattern> createPatterns(String globs)
    {
        Set<Pattern> patterns = new HashSet<Pattern>();
        if (globs != null) {
            for (String s: globs.split(",")) {
                patterns.add(Glob.parseGlobToPattern(s));
            }
        }
        return patterns;
    }

    private synchronized int copy(Args args,
                                  String defaultSelect,
                                  String defaultTarget,
                                  String defaultSourceMode,
                                  String defaultTargetMode,
                                  String defaultRefresh,
                                  String defaultPins,
                                  boolean defaultVerify)
        throws IllegalArgumentException, NumberFormatException
    {
        String exclude = args.getOpt("exclude");
        String excludeWhen = args.getOpt("exclude-when");
        String include = args.getOpt("include");
        String includeWhen = args.getOpt("include-when");
        boolean permanent = (args.getOpt("permanent") != null);
        boolean eager = (args.getOpt("eager") != null);
        boolean verify = (args.getOpt("verify") != null) || defaultVerify;
        String sourceMode = args.getOpt("smode");
        String targetMode = args.getOpt("tmode");
        String select = args.getOpt("select");
        String target = args.getOpt("target");
        String refresh = args.getOpt("refresh");
        String concurrency = args.getOpt("concurrency");
        String pins = args.getOpt("pins");
        String order = args.getOpt("order");

        if (permanent && order != null) {
            throw new IllegalArgumentException("Permanent jobs cannot be ordered");
        }

        if (select == null) {
            select = defaultSelect;
        }
        if (target == null) {
            target = defaultTarget;
        }
        if (sourceMode == null) {
            sourceMode = defaultSourceMode;
        }
        if (targetMode == null) {
            targetMode = defaultTargetMode;
        }
        if (refresh == null) {
            refresh = defaultRefresh;
        }
        if (concurrency == null) {
            concurrency = "1";
        }
        if (pins == null) {
            pins = defaultPins;
        }

        List<String> targets = new ArrayList();
        for (int i = 0; i < args.argc(); i++) {
            targets.add(args.argv(i));
        }

        Collection<Pattern> excluded = createPatterns(exclude);
        excluded.add(Pattern.compile(Pattern.quote(_context.getPoolName())));
        Collection<Pattern> included = createPatterns(include);

        boolean mustMovePins;
        if (pins.equals("keep")) {
            mustMovePins = false;
        } else if (pins.equals("move")) {
            mustMovePins = true;
        } else {
            throw new IllegalArgumentException(pins + ": Invalid value for option -pins");
        }

        JobDefinition definition =
            new JobDefinition(createFilters(args),
                              createCacheEntryMode(sourceMode),
                              createCacheEntryMode(targetMode),
                              createPoolSelectionStrategy(select),
                              createComparator(order),
                              createPoolList(1.0, 0.0, target, targets,
                                             excluded,
                                             createPoolExpression(excludeWhen, FALSE_EXPRESSION),
                                             included,
                                             createPoolExpression(includeWhen, TRUE_EXPRESSION)),
                              Integer.valueOf(refresh) * 1000,
                              permanent,
                              eager,
                              mustMovePins,
                              verify);

        if (definition.targetMode.state == CacheEntryMode.State.DELETE
            || definition.targetMode.state == CacheEntryMode.State.REMOVABLE) {
            throw new IllegalArgumentException(targetMode + ": Invalid value");
        }

        int n = Integer.valueOf(concurrency);
        int id = _counter++;
        Job job = new Job(_context, definition);
        job.setConcurrency(n);
        _jobs.put(id, job);
        _alive.add(job);
        return id;
    }

    /**
     * Immediately cancels all jobs.
     */
    public synchronized void cancelAll()
    {
        for (Job job: _jobs.values()) {
            try {
                job.cancel(true);
            } catch (IllegalStateException e) {
                // Jobs cannot always be cancelled. This should be
                // fixed in the Job. For now we silently ignore this
                // error.
            }
        }
    }

    public final static String hh_migration_concurrency = "<job> <n>";
    public final static String fh_migration_concurrency =
        "Adjusts the concurrency of a job.";
    public String ac_migration_concurrency_$_2(Args args)
    {
        int id = Integer.valueOf(args.argv(0));
        int concurrency = Integer.valueOf(args.argv(1));
        Job job = getJob(id);
        job.setConcurrency(concurrency);
        return String.format("[%d] Concurrency set to %d", id, concurrency);
    }

    public final static String hh_migration_copy = "[options] <target> ...";
    public final static String fh_migration_copy =
        "Copies files to other pools. Unless filter options are specified,\n" +
        "all files on the source pool are copied.\n\n" +

        "The operation is idempotent, that is, it can safely be repeated\n" +
        "without creating extra copies of the files. If the replica exists\n" +
        "on any of the target pools, then it is not copied again. If the\n" +
        "target pool with the existing replica fails to respond, then the\n" +
        "operation is retried indefinitely, unless the job is marked as\n" +
        "eager.\n\n" +

        "Both the state of the local replica and that of the target replica\n" +
        "can be specified. If the target replica already exists, the state\n" +
        "is updated to be at least as strong as the specified target state,\n" +
        "that is, the lifetime of sticky bits is extended, but never reduced,\n" +
        "and cached can be changed to precious, but never the opposite.\n\n" +

        "Transfers are subject to the checksum computiton policy of the\n" +
        "target pool. Thus checksums are verified if and only if the target\n" +
        "pool is configured to do so. For existing replicas, the checksum is\n" +
        "only verified if the verify option was specified on the migration job.\n\n" +

        "Jobs can be marked permanent. Permanent jobs never terminate and\n" +
        "are stored in the pool setup file with the 'save' command. Permanent\n" +
        "jobs watch the repository for state changes and copy any replicas\n" +
        "that match the selection criteria, even replicas added after the\n" +
        "job was created. Notice that any state change will cause a replica\n" +
        "to be reconsidered and enqueued if it matches the selection\n" +
        "criteria - also replicas that have been copied before.\n\n" +

        "Several options allow an expression to be specified. The following\n" +
        "operators are recognized: <, <=, ==, !=, >=, >, lt, le, eq, ne, ge,\n" +
        "gt, ~=, !~, +, -, *, /, %, div, mod, |, &, ^, ~, &&, ||, !, and, or,\n" +
        "not, ?:, =. Literals may be integer literals, floating point literals,\n" +
        "single or double quoted string literals, and boolean true and false.\n" +
        "Depending on the context, the expression may refer to constants.\n\n" +

        "Syntax:\n" +
        "  copy [options] <target> ...\n\n" +
        "Filter options:\n" +
        "  -accessed=<n>|[<n>]..[<m>]\n"+
        "          Only copy replicas accessed n seconds ago, or accessed\n" +
        "          within the given, possibly open-ended, interval. E.g.\n" +
        "          -accessed=0..60 matches files accessed within the last\n" +
        "          minute; -accesed=60.. matches files accessed one minute\n" +
        "          or more ago.\n" +
        "  -al=ONLINE|NEARLINE\n" +
        "          Only copy replicas with the given access latency.\n" +
        "  -pnfsid=<pnfsid>[,<pnfsid>] ...\n" +
        "          Only copy replicas with one of the given PNFS IDs.\n" +
        "  -rp=CUSTODIAL|REPLICA|OUTPUT\n" +
        "          Only copy replicas with the given retention policy.\n" +
        "  -size=<n>|[<n>]..[<m>]\n"+
        "          Only copy replicas with size n, or a size within the\n" +
        "          given, possibly open-ended, interval.\n" +
        "  -state=cached|precious\n" +
        "          Only copy replicas in the given state.\n"+
        "  -sticky[=<owner>[,<owner>...]]\n" +
        "          Only copy sticky replicas. Can optionally be limited to\n" +
        "          the list of owners. A sticky flag for each owner must be\n" +
        "          present for the replica to be selected.\n" +
        "  -storage=<class>\n" +
        "          Only copy replicas with the given storage class.\n\n" +
        "Transfer options:\n" +
        "  -concurrency=<concurrency>\n" +
        "          Specifies how many concurrent transfers to perform.\n" +
        "          Defaults to 1.\n" +
        "  -order=[-]size|[-]lru\n" +
        "          Sort transfer queue. By default transfers are placed in\n" +
        "          ascending order, that is, smallest and least recently used\n" +
        "          first. Transfers are placed in descending order if the key\n" +
        "          is prefixed by a minus sign. Failed transfers are placed at\n" +
        "          the end of the queue for retry regardless of the order. This\n" +
        "          option cannot be used for permanent jobs. Notice that for\n" +
        "          pools with a large number of files, sorting significantly\n" +
        "          increases the initialization time of the migration job.\n" +
        "          size:\n" +
        "              Sort according to file size.\n" +
        "          lru:\n" +
        "              Sort according to last access time.\n" +
        "  -pins=move|keep\n" +
        "          Controls how sticky flags owned by the pin manager are handled:\n" +
        "          move:\n" +
        "              Ask pin manager to move pins to the target pool.\n" +
        "          keep:\n" +
        "              Keep pin on the source pool.\n" +
        "  -smode=same|cached|precious|removable|delete[+<owner>[(<lifetime>)] ...]\n" +
        "          Update the local replica to the given mode after transfer:\n" +
        "          same:\n" +
        "              does not change the local state (this is the default).\n" +
        "          cached:\n" +
        "              marks it cached.\n" +
        "          precious:\n" +
        "              marks it precious.\n" +
        "          removable:\n" +
        "              marks it cached and strips all existing sticky flags\n" +
        "              exluding pins.\n" +
        "          delete:\n" +
        "              deletes the replica unless it is pinned.\n" +
        "          An optional list of sticky flags can be specified. The\n" +
        "          lifetime is in seconds. A lifetime of 0 causes the flag\n" +
        "          to immediately expire. Notice that existing sticky flags\n" +
        "          of the same owner are overwritten.\n" +
        "  -tmode=same|cached|precious[+<owner>[(<lifetime>)]...]\n" +
        "          Set the mode of the target replica:\n" +
        "          same:\n" +
        "              applies the state and sticky bits excluding pins\n" +
        "              of the local replica (this is the default).\n" +
        "          cached:\n" +
        "              marks it cached.\n" +
        "          precious:\n" +
        "              marks it precious.\n" +
        "          An optional list of sticky flags can be specified. The\n" +
        "          lifetime is in seconds.\n" +
        "  -verify\n" +
        "          Force checksum computation when an existing target is updated.\n\n" +
        "Target options:\n" +
        "  -eager\n" +
        "          Copy replicas rather than retrying when pools with\n" +
        "          existing replicas fail to respond.\n" +
        "  -exclude=<pattern>[,<pattern>...]\n" +
        "          Exclude target pools matching any of the patterns. Single\n" +
        "          character (?) and multi character (*) wildcards may be used.\n" +
        "  -exclude-when=<expr>\n" +
        "          Exclude target pools for which the expression evaluates to\n" +
        "          true. The expression may refer to the following constants:\n" +
        "          target.name:\n" +
        "              pool name\n" +
        "          target.spaceCost:\n" +
        "              space cost\n" +
        "          target.cpuCost:\n" +
        "              cpu cost\n" +
        "          target.free:\n" +
        "              free space in bytes\n" +
        "          target.total:\n" +
        "              total space in bytes\n" +
        "          target.removable:\n" +
        "              removable space in bytes\n" +
        "          target.used:\n" +
        "              used space in bytes\n" +
        "  -include=<pattern>[,<pattern>...]\n" +
        "          Only include target pools matching any of the patterns. Single\n" +
        "          character (?) and multi character (*) wildcards may be used.\n" +
        "  -include-when=<expr>\n" +
        "          Only include target pools for which the expression evaluates\n" +
        "          to true. See the description of -exclude-when for the list\n" +
        "          of allowed constants.\n" +
        "  -refresh=<time>\n" +
        "          Sets the period in seconds of when target pool information\n" +
        "          is queried from the pool manager. Inclusion and exclusion\n" +
        "          expressions are evaluated whenever the information is\n" +
        "          refreshed. The default is 300 seconds.\n" +
        "  -select=proportional|best|random\n" +
        "          Determines how a pool is selected from the set of target pools:\n" +
        "          proportional:\n" +
        "              selects a pool with a probability inversely proportional\n" +
        "              to the cost of the pool.\n" +
        "          best:\n" +
        "              selects the pool with the lowest cost.\n" +
        "          random:\n" +
        "              selects a pool randomly.\n" +
        "          The default is 'proportional'.\n" +
        "  -target=pool|pgroup|link\n" +
        "          Determines the interpretation of the target names. The\n" +
        "          default is 'pool'.\n\n" +
        "Lifetime options:\n" +
        "  -permanent\n" +
        "          Mark job as permanent.\n";
//         "  -dry-run\n" +
//         "          Perform all the steps without actually copying anything\n" +
//         "          or updating the state.";
    public synchronized String ac_migration_copy_$_1_99(Args args)
        throws IllegalArgumentException, NumberFormatException
    {
        int id = copy(args, "proportional", "pool", "same", "same", "300", "keep", false);
        String command = "migration copy " + args.toString();
        _commands.put(_jobs.get(id), command);
        return getJobSummary(id);
    }

    public final static String hh_migration_move = "[options] <target> ...";
    public final static String fh_migration_move =
        "Moves replicas to other pools. The source replica is deleted.\n" +
        "Accepts the same options as 'migration copy'. Corresponds to\n\n" +
        "     migration copy -smode=delete -tmode=same -pins=move -verify\n";
    public String ac_migration_move_$_1_99(Args args)
        throws IllegalArgumentException, NumberFormatException
    {
        int id = copy(args, "proportional", "pool", "delete", "same", "300", "move", true);
        String command = "migration move " + args.toString();
        _commands.put(_jobs.get(id), command);
        return getJobSummary(id);
    }

    public final static String hh_migration_cache =
        "[options] <target> ...";
    public final static String fh_migration_cache =
        "Caches replicas on other pools. Accepts the same options as\n" +
        "'migration copy'. Corresponds to\n\n" +
        "     migration copy -smode=same -tmode=cached\n";
    public String ac_migration_cache_$_1_99(Args args)
        throws IllegalArgumentException, NumberFormatException
    {
        int id = copy(args, "proportional", "pool", "same", "cached", "300", "keep", false);
        String command = "migration cache " + args.toString();
        _commands.put(_jobs.get(id), command);
        return getJobSummary(id);
    }

    public final static String hh_migration_suspend =
        "<job>";
    public final static String fh_migration_suspend =
        "Suspends a migration job. A suspended job finishes ongoing\n" +
        "transfers, but is does not start any new transfer.";
    public String ac_migration_suspend_$_1(Args args)
    {
        int id = Integer.valueOf(args.argv(0));
        Job job = getJob(id);
        job.suspend();
        return getJobSummary(id);
    }

    public final static String hh_migration_resume =
        "<job>";
    public final static String fh_migration_resume =
        "Resumes a suspended migration job.";
    public String ac_migration_resume_$_1(Args args)
    {
        int id = Integer.valueOf(args.argv(0));
        Job job = getJob(id);
        job.resume();
        return getJobSummary(id);
    }

    public final static String hh_migration_cancel =
        "[-force] <job>";
    public final static String fh_migration_cancel =
        "Cancels a migration job.\n\n" +
        "Options:\n" +
        "  -force\n" +
        "     Kill ongoing transfers.\n";
    public String ac_migration_cancel_$_1(Args args)
    {
        int id = Integer.valueOf(args.argv(0));
        boolean force = (args.getOpt("force") != null);
        Job job = getJob(id);
        job.cancel(force);
        return getJobSummary(id);
    }

    public final static String fh_migration_clear =
        "Removes completed migration jobs. For reference, information about\n" +
        "migration jobs are kept until explicitly cleared.\n";
    public synchronized String ac_migration_clear(Args args)
    {
        Iterator<Job> i = _jobs.values().iterator();
        while (i.hasNext()) {
            Job job = i.next();
            switch (job.getState()) {
            case CANCELLED:
            case FINISHED:
                i.remove();
                _commands.remove(job);
                break;
            default:
                break;
            }
        }
        return "";
    }

    public final static String fh_migration_ls =
        "Lists all migration jobs.";
    public synchronized String ac_migration_ls(Args args)
    {
        StringBuilder s = new StringBuilder();
        for (int id: _jobs.keySet()) {
            s.append(getJobSummary(id)).append('\n');
        }
        return s.toString();
    }

    public final static String hh_migration_info =
        "<job>";
    public final static String fh_migration_info =
        "Shows detailed information about a migration job. Possible\n" +
        "job states are:\n\n" +
        "   INITIALIZING   Initial scan of repository\n" +
        "   RUNNING        Job runs (schedules new tasks)\n" +
        "   SLEEPING       A task failed; no tasks are scheduled for 10 seconds\n" +
        "   SUSPENDED      Job suspended by user; no tasks are scheduled\n" +
        "   CANCELLING     Job cancelled by user; waiting for tasks to stop\n" +
        "   CANCELLED      Job cancelled by user; no tasks are running\n" +
        "   FINISHED       Job completed\n\n" +
        "   FAILED         Job failed (check log file for details)\n" +
        "Job tasks may be in any of the following states:\n\n" +
        "   Queued               Queued for execution\n" +
        "   GettingLocations     Querying PnfsManager for file locations\n" +
        "   UpdatingExistingFile Updating the state of existing target file\n" +
        "   CancellingUpdate     Task cancelled, waiting for update to complete\n" +
        "   InitiatingCopy       Request send to target, waiting for confirmation\n" +
        "   Copying              Waiting for target to complete the transfer\n" +
        "   Pinging              Ping send to target, waiting for reply\n" +
        "   NoResponse           Cell connection to target lost\n" +
        "   Waiting              Waiting for final confirmation from target\n" +
        "   MovingPin            Waiting for pin manager to move pin\n" +
        "   Cancelling           Attempting to cancel transfer\n" +
        "   Cancelled            Task cancelled, file was not copied\n" +
        "   Failed               The task failed\n" +
        "   Done                 The task completed successfully";
    public synchronized String ac_migration_info_$_1(Args args)
        throws NoSuchElementException
    {
        int id = Integer.valueOf(args.argv(0));
        Job job = getJob(id);
        String command = _commands.get(job);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println("Command    : " + command);
        job.getInfo(pw);
        return sw.toString();
    }

    public synchronized
        void messageArrived(PoolMigrationCopyFinishedMessage message)
    {
        if (!message.getPool().equals(_context.getPoolName())) {
            return;
        }

        /* To avoid callbacks or an extra update thread, we lazily
         * move jobs out of _alive.
         */
        Iterator<Job> i = _alive.iterator();
        while (i.hasNext()) {
            Job job = i.next();
            switch (job.getState()) {
            case CANCELLED:
            case FINISHED:
                i.remove();
                break;
            default:
                job.messageArrived(message);
            }
        }
    }

    public synchronized void getInfo(PrintWriter pw)
    {
        for (int id: _jobs.keySet()) {
            pw.println(getJobSummary(id));
        }
    }

    public synchronized void printSetup(PrintWriter pw)
    {
        pw.println("#\n# MigrationModule\n#");
        for (Job job: _jobs.values()) {
            if (job.getDefinition().isPermanent) {
                pw.println(_commands.get(job));
            }
        }
    }

    public synchronized boolean isActive(PnfsId id)
    {
        for (Job job: _jobs.values()) {
            if (job.isRunning(id)) {
                return true;
            }
        }
        return false;
    }
}