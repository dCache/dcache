package org.dcache.pool.migration;

import com.google.common.collect.Range;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.ReportingParseRunner;
import org.parboiled.support.ParsingResult;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import dmg.cells.nucleus.CellEndpoint;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;
import dmg.util.command.Option;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.cells.CellStub;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Glob;
import org.dcache.util.expression.Expression;
import org.dcache.util.expression.ExpressionParser;
import org.dcache.util.expression.Token;
import org.dcache.util.expression.Type;
import org.dcache.util.expression.TypeMismatchException;
import org.dcache.util.expression.UnknownIdentifierException;

import static java.util.Arrays.asList;
import static org.parboiled.errors.ErrorUtils.printParseErrors;

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
 * target pool - and updating it if necessary. Idempotence may however
 * be affected by exclude and include expressions: If those rely on
 * values that change during the runtime of the job, then the job will
 * no longer be idempotent.
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
    private final static PoolManagerPoolInformation DUMMY_POOL =
        new PoolManagerPoolInformation("pool", new PoolCostInfo("pool"));

    public final static String CONSTANT_TARGET = "target";
    public final static String CONSTANT_SOURCE = "source";
    public final static String CONSTANT_TARGETS = "targets";
    public final static String CONSTANT_QUEUE_FILES = "queue.files";
    public final static String CONSTANT_QUEUE_BYTES = "queue.bytes";

    public final static int NON_EMPTY_QUEUE = 1;
    public final static int NO_TARGETS = 0;

    private final static Pattern STICKY_PATTERN =
            Pattern.compile("(\\w+)(\\((-?\\d+)\\))?");

    private final Map<String,Job> _jobs = new HashMap<>();
    private final Map<Job,String> _commands = new HashMap<>();
    private final MigrationContext _context = new MigrationContext();

    private final static Expression TRUE_EXPRESSION =
        new Expression(Token.TRUE);
    private final static Expression FALSE_EXPRESSION =
        new Expression(Token.FALSE);

    static {
        TRUE_EXPRESSION.setType(Type.BOOLEAN);
        FALSE_EXPRESSION.setType(Type.BOOLEAN);
    }

    private int _counter = 1;

    @Override
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
    private synchronized Job getJob(String id)
        throws NoSuchElementException
    {
        Job job = _jobs.get(id);
        if (job == null) {
            throw new NoSuchElementException("Job not found");
        }
        return job;
    }

    /** Returns a one line description of a job. */
    private synchronized String getJobSummary(String id)
    {
        Job job = getJob(id);
        return String.format("[%s] %-12s %s", id, job.getState(),
                             _commands.get(job));
    }

    /**
     * Parse a range in the format N..M. If N or M is omitted, then
     * the range will be unbounded in that direction.
     *
     * @throws IllegalArgumentException in case of syntax errors.
     */
    private static Range<Long> parseRange(String s)
        throws IllegalArgumentException
    {
        String[] bounds = s.split("\\.\\.", 2);
        switch (bounds.length) {
        case 1:
            return Range.singleton(Long.parseLong(bounds[0]));

        case 2:
            if (bounds[0].length() == 0 && bounds[1].length() == 0) {
                return Range.all();
            } else if (bounds[0].length() == 0) {
                return Range.atMost(Long.parseLong(bounds[1]));
            } else if (bounds[1].length() == 0) {
                return Range.atLeast(Long.parseLong(bounds[0]));
            } else {
                return Range.closed(Long.parseLong(bounds[0]),
                                    Long.parseLong(bounds[1]));
            }

        default:
            throw new IllegalArgumentException(s + ": Invalid interval");
        }
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

    @Command(name="migration concurrency",
            description ="Adjust the concurrency of a job.")
    public class MigrationConcurrencyCommand implements Callable<String>
    {
        @Argument(index=0)
        String id;

        @Argument(index=1)
        int concurrency;

        @Override
        public String call() throws NoSuchElementException
        {
            Job job = getJob(id);
            job.setConcurrency(concurrency);
            return String.format("[%s] Concurrency set to %d", id, concurrency);
        }
    }

    @Command(name="migration copy",
            description = "Copies files to other pools. Unless filter options are specified, " +
                    "all files on the source pool are copied.\n\n" +

                    "The operation is idempotent, that is, it can safely be repeated " +
                    "without creating extra copies of the files. If the replica exists " +
                    "on any of the target pools, then it is not copied again. If the " +
                    "target pool with the existing replica fails to respond, then the " +
                    "operation is retried indefinitely, unless the job is marked as " +
                    "eager.\n\n" +

                    "Please notice that a job is only idempotent as long as the set of " +
                    "target pools do not change. If pools go offline or are excluded as " +
                    "a result of a an exclude or include expression, then the idempotent " +
                    "nature of a job may be lost.\n\n" +

                    "Both the state of the local replica and that of the target replica " +
                    "can be specified. If the target replica already exists, the state " +
                    "is updated to be at least as strong as the specified target state, " +
                    "that is, the lifetime of sticky bits is extended, but never reduced, " +
                    "and cached can be changed to precious, but never the opposite.\n\n" +

                    "Transfers are subject to the checksum computiton policy of the " +
                    "target pool. Thus checksums are verified if and only if the target " +
                    "pool is configured to do so. For existing replicas, the checksum is " +
                    "only verified if the verify option was specified on the migration job.\n\n" +

                    "Jobs can be marked permanent. Permanent jobs never terminate and " +
                    "are stored in the pool setup file with the 'save' command. Permanent " +
                    "jobs watch the repository for state changes and copy any replicas " +
                    "that match the selection criteria, even replicas added after the " +
                    "job was created. Notice that any state change will cause a replica " +
                    "to be reconsidered and enqueued if it matches the selection " +
                    "criteria - also replicas that have been copied before.\n\n" +

                    "Several options allow an expression to be specified. The following " +
                    "operators are recognized: <, <=, ==, !=, >=, >, ~=, !~, +, -, *, /, " +
                    "**, %, and, or, not, ?:. Literals may be floating point literals, " +
                    "single or double quoted string literals, and boolean true and false. " +
                    "Depending on the context, the expression may refer to constants.")
    public class MigrationCopyCommand implements Callable<String>
    {
        @Option(name="id")
        String id;

        @Option(name="accessed",
                category="Filter options",
                usage = "Only copy replicas accessed n seconds ago, or accessed " +
                        "within the given, possibly open-ended, interval. E.g. " +
                        "-accessed=0..60 matches files accessed within the last " +
                        "minute; -accesed=60.. matches files accessed one minute " +
                        "or more ago.")
        String accessed;

        @Option(name="al", values={"online", "nearline"},
                category="Filter options",
                usage="Only copy replicas with the given access latency.")
        String accessLatency;

        @Option(name="pnfsid", separator=",",
                category="Filter options",
                usage="Only copy replicas with one of the given PNFS IDs.")
        PnfsId[] pnfsid;

        @Option(name="rp", values={"custodial", "replica", "output"},
                category="Filter options",
                usage="Only copy replicas with the given retention policy.")
        String retentionPolicy;

        @Option(name="size",
                category="Filter options",
                usage="Only copy replicas with size n, or a size within the given, possibly open-ended, interval")
        String size;

        @Option(name="state", values={"cached", "precious"},
                category="Filter options",
                usage="Only copy replicas in the given state.")
        String state;

        @Option(name="sticky", metaVar="owner", separator=",",
                category="Filter options",
                usage = "Only copy sticky replicas. Can optionally be limited to " +
                        "the list of owners. A sticky flag for each owner must be " +
                        "present for the replica to be selected.")
        String[] sticky;

        @Option(name="storage", metaVar="class",
                category="Filter options",
                usage="Only copy replicas with the given access latency.")
        String storage;

        @Option(name="concurrency",
                category="Transfer options",
                usage="Specifies how many concurrent transfers to perform.")
        int concurrency = 1;

        @Option(name="order", valueSpec="[-]size|[-]lru",
                category="Transfer options",
                usage = "Sort transfer queue. By default transfers are placed in " +
                        "ascending order, that is, smallest and least recently used " +
                        "first. Transfers are placed in descending order if the key " +
                        "is prefixed by a minus sign. Failed transfers are placed at " +
                        "the end of the queue for retry regardless of the order. This " +
                        "option cannot be used for permanent jobs. Notice that for " +
                        "pools with a large number of files, sorting significantly " +
                        "increases the initialization time of the migration job.\n" +
                        "size:\n" +
                        "    Sort according to file size.\n" +
                        "lru:\n" +
                        "    Sort according to last access time.")
        String order;

        @Option(name="pins", values={"keep", "move"},
                category="Transfer options",
                usage = "Controls how sticky flags owned by the pin manager are handled:\n" +
                        "move:\n" +
                        "    Ask pin manager to move pins to the target pool.\n" +
                        "keep:\n" +
                        "    Keep pin on the source pool.")
        String pins = "keep";

        @Option(name="smode", valueSpec="same|cached|precious|removable|delete[+OWNER[(LIFETIME)]]...",
                category="Transfer options",
                usage = "Update the local replica to the given mode after transfer:\n" +
                        "same:\n" +
                        "    does not change the local state.\n" +
                        "cached:\n" +
                        "    marks it cached.\n" +
                        "precious:\n" +
                        "    marks it precious.\n" +
                        "removable:\n" +
                        "    marks it cached and strips all existing sticky flags\n" +
                        "    exluding pins.\n" +
                        "delete:\n" +
                        "    deletes the replica unless it is pinned.\n" +
                        "An optional list of sticky flags can be specified. The " +
                        "lifetime is in seconds. A lifetime of 0 causes the flag " +
                        "to immediately expire. Notice that existing sticky flags " +
                        "of the same owner are overwritten.")
        String sourceMode = "same";

        @Option(name="tmode", valueSpec="same|cached|precious[+OWNER[(LIFETIME)]]...",
                category="Transfer options",
                usage = "Set the mode of the target replica:\n" +
                        "same:\n" +
                        "    applies the state and sticky bits excluding pins\n" +
                        "    of the local replica.\n" +
                        "cached:\n" +
                        "    marks it cached.\n" +
                        "precious:\n" +
                        "    marks it precious.\n" +
                        "An optional list of sticky flags can be specified. The " +
                        "lifetime is in seconds.")
        String targetMode = "same";

        @Option(name="verify",
                category="Transfer options",
                usage="Force checksum computation when an existing target is updated.")
        boolean verify;

        @Option(name="eager",
                category="Target options",
                usage = "Copy replicas rather than retrying when pools with " +
                        "existing replicas fail to respond.")
        boolean eager;

        @Option(name="exclude", metaVar="glob", separator=",",
                category="Target options",
                usage = "Exclude target pools matching any of the patterns. Single " +
                        "character (?) and multi character (*) wildcards may be used.")
        String[] exclude;

        @Option(name="exclude-when", metaVar="expr",
                category="Target options",
                usage = "Exclude target pools for which the expression evaluates to " +
                        "true. The expression may refer to the following constants:\n" +
                        "source.name/target.name:\n" +
                        "    pool name\n" +
                        "source.cpuCost/target.cpuCost:\n" +
                        "    cpu cost\n" +
                        "source.free/target.free:\n" +
                        "    free space in bytes\n" +
                        "source.total/target.total:\n" +
                        "    total space in bytes\n" +
                        "source.removable/target.removable:\n" +
                        "    removable space in bytes\n" +
                        "source.used/target.used:\n" +
                        "     used space in bytes")
        String excludeWhen;

        @Option(name="include", metaVar="glob", separator=",",
                category="Target options",
                usage = "Only include target pools matching any of the patterns. Single " +
                        "character (?) and multi character (*) wildcards may be used.")
        String[] include;

        @Option(name="include-when", metaVar="expr",
                category="Target options",
                usage = "Only include target pools for which the expression evaluates " +
                        "to true. See the description of -exclude-when for the list " +
                        "of allowed constants.")
        String includeWhen;

        @Option(name="refresh", metaVar="seconds",
                category="Target options",
                usage = "Sets the period in seconds of when target pool information " +
                        "is queried from the pool manager. Inclusion and exclusion " +
                        "expressions are evaluated whenever the information is " +
                        "refreshed.")
        int refresh = 300;

        @Option(name="select", values={"proportional", "random"},
                category="Target options",
                usage = "Determines how a pool is selected from the set of target pools:\n" +
                        "proportional:\n" +
                        "    selects a pool with a probability proportional\n" +
                        "    to the free space.\n" +
                        "random:\n" +
                        "    selects a pool randomly.\n")
        String select = "proportional";

        @Option(name="target", values={"pool", "pgroup", "link"},
                category="Target options",
                usage = "Determines the interpretation of the target names.")
        String target = "pool";

        @Option(name="pause-when", metaVar="expr",
                category="Lifetime options",
                usage = "Pauses the job when the expression becomes true. The job " +
                        "continues when the expression once again evaluates to false. " +
                        "The following constants are defined for this pool:\n" +
                        "queue.files:\n" +
                        "    the number of files remaining to be transferred.\n" +
                        "queue.bytes:\n" +
                        "    the number of bytes remaining to be transferred.\n" +
                        "source.name:\n" +
                        "    pool name\n" +
                        "source.cpuCost:\n" +
                        "    cpu cost\n" +
                        "source.free:\n" +
                        "    free space in bytes\n" +
                        "source.total:\n" +
                        "    total space in bytes\n" +
                        "source.removable:\n" +
                        "    removable space in bytes\n" +
                        "source.used:\n" +
                        "    used space in bytes\n" +
                        "targets:\n" +
                        "    the number of target pools.")
        String pauseWhen;

        @Option(name="permanent", usage="Mark job as permanent.",
                category="Lifetime options")
        boolean permanent;

        @Option(name="stop-when", metaVar="expr",
                category="Lifetime options",
                usage = "Terminates the job when the expression becomes true. This option " +
                        "cannot be used for permanent jobs. See the description of " +
                        "-pause-when for the list of constants allowed in the expression.")
        String stopWhen;

        @Option(name="force-source-mode",
                category="Transfer options",
                usage = "Enables the transfer of files from a disabled pool.")
        boolean forceSourceMode;

        @Argument(metaVar="target")
        String[] targets;

        @CommandLine
        String commandLine;

        private RefreshablePoolList createPoolList(String type, List<String> targets)
        {
            CellStub poolManager = _context.getPoolManagerStub();

            switch (type) {
            case "pool":
                return new PoolListByNames(poolManager, targets);
            case "pgroup":
                return new PoolListByPoolGroup(poolManager, targets);
            case "link":
                if (targets.size() != 1) {
                    throw new IllegalArgumentException(targets.toString() +
                            ": Only one target supported for -type=link");
                }
                return new PoolListByLink(poolManager, targets.get(0));
            default:
                throw new IllegalArgumentException(type + ": Invalid value");
            }
        }

        private PoolSelectionStrategy createPoolSelectionStrategy(String type)
        {
            switch (type) {
            case "proportional":
                return new ProportionalPoolSelectionStrategy();
            case "random":
                return new RandomPoolSelectionStrategy();
            default:
                throw new IllegalArgumentException(type + ": Invalid value");
            }
        }

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
            List<StickyRecord> records = new ArrayList<>();

            for (int i = 1; i < s.length; i++) {
                records.add(parseStickyRecord(s[i]));
            }

            switch (s[0]) {
            case "same":
                return new CacheEntryMode(CacheEntryMode.State.SAME, records);
            case "cached":
                return new CacheEntryMode(CacheEntryMode.State.CACHED, records);
            case "delete":
                return new CacheEntryMode(CacheEntryMode.State.DELETE, records);
            case "removable":
                return new CacheEntryMode(CacheEntryMode.State.REMOVABLE, records);
            case "precious":
                return new CacheEntryMode(CacheEntryMode.State.PRECIOUS, records);
            default:
                throw new IllegalArgumentException(type + ": Invalid value");
            }
        }

        private Comparator<CacheEntry> createComparator(String order)
        {
            if (order == null) {
                return null;
            }

            switch (order) {
            case "size":
                return new SizeOrder();
            case "-size":
                return new ReverseOrder<>(new SizeOrder());
            case "lru":
                return new LruOrder();
            case "-lru":
                return new ReverseOrder<>(new LruOrder());
            default:
                throw new IllegalArgumentException(order + ": Invalid value for option -order");
            }
        }

        private Expression createPredicate(String s, Expression ifNull,
                                           SymbolTable symbols)
        {
            try {
                if (s == null) {
                    return ifNull;
                }

                ExpressionParser parser =
                        Parboiled.createParser(ExpressionParser.class);
                ParsingResult<Expression> result =
                        new ReportingParseRunner<Expression>(parser.Top()).run(s);

                if (result.hasErrors()) {
                    throw new IllegalArgumentException("Invalid expression: " +
                            printParseErrors(result));
                }

                Expression expression = result.resultValue;
                if (expression.check(symbols) != Type.BOOLEAN) {
                    throw new IllegalArgumentException("Expression does not evaluate to a boolean");
                }

                return expression;
            } catch (UnknownIdentifierException | TypeMismatchException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        private Expression createPoolPredicate(String s, Expression ifNull)
        {
            SymbolTable symbols = new SymbolTable();
            symbols.put(CONSTANT_SOURCE, DUMMY_POOL);
            symbols.put(CONSTANT_TARGET, DUMMY_POOL);
            return createPredicate(s, ifNull, symbols);
        }

        private Expression createLifetimePredicate(String s)
        {
            SymbolTable symbols = new SymbolTable();
            symbols.put(CONSTANT_SOURCE, DUMMY_POOL);
            symbols.put(CONSTANT_QUEUE_FILES, NON_EMPTY_QUEUE);
            symbols.put(CONSTANT_QUEUE_BYTES, NON_EMPTY_QUEUE);
            symbols.put(CONSTANT_TARGETS, NO_TARGETS);
            return createPredicate(s, null, symbols);
        }

        private Set<Pattern> createPatterns(String[] globs)
        {
            Set<Pattern> patterns = new HashSet<>();
            if (globs != null) {
                for (String s: globs) {
                    patterns.add(Glob.parseGlobToPattern(s));
                }
            }
            return patterns;
        }

        private List<CacheEntryFilter> createFilters()
                throws IllegalArgumentException
        {
            List<CacheEntryFilter> filters = new ArrayList<>();

            if (storage != null) {
                filters.add(new StorageClassFilter(storage));
            }

            if (pnfsid != null) {
                filters.add(new PnfsIdFilter(new HashSet<>(asList(pnfsid))));
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
                if (sticky.length == 0) {
                    filters.add(new StickyFilter());
                } else {
                    for (String owner: sticky) {
                        filters.add(new StickyOwnerFilter(owner));
                    }
                }
            }

            if (size != null) {
                filters.add(new SizeFilter(parseRange(size)));
            }

            if (accessed != null) {
                filters.add(new AccessedFilter(parseRange(accessed)));
            }

            if (accessLatency != null) {
                filters.add(new AccessLatencyFilter(AccessLatency.getAccessLatency(accessLatency)));
            }

            if (retentionPolicy != null) {
                filters.add(new RetentionPolicyFilter(RetentionPolicy.getRetentionPolicy(retentionPolicy)));
            }

            return filters;
        }

        @Override
        public String call() throws IllegalArgumentException
        {
            if (permanent) {
                if (order != null) {
                    throw new IllegalArgumentException("Permanent jobs cannot be ordered");
                }
                if (stopWhen != null) {
                    throw new IllegalArgumentException("Permanent jobs cannot have a stop condition.");
                }
            }

            Collection<Pattern> excluded = createPatterns(exclude);
            excluded.add(Pattern.compile(Pattern.quote(_context.getPoolName())));
            Collection<Pattern> included = createPatterns(include);

            boolean mustMovePins;
            switch (pins) {
            case "keep":
                mustMovePins = false;
                break;
            case "move":
                mustMovePins = true;
                break;
            default:
                throw new IllegalArgumentException(pins + ": Invalid value for option -pins");
            }

            /* The source list is used to fetch pool information about this pool.
            */
            RefreshablePoolList sourceList =
                    new PoolListByNames(_context.getPoolManagerStub(),
                            Collections.singletonList(_context.getPoolName()));

            Expression excludeExpression =
                    createPoolPredicate(excludeWhen, FALSE_EXPRESSION);
            Expression includeExpression =
                    createPoolPredicate(includeWhen, TRUE_EXPRESSION);

            RefreshablePoolList poolList =
                    new PoolListFilter(createPoolList(target, asList(targets)),
                            excluded, excludeExpression,
                            included, includeExpression,
                            sourceList);

            JobDefinition definition =
                    new JobDefinition(createFilters(),
                            createCacheEntryMode(sourceMode),
                            createCacheEntryMode(targetMode),
                            createPoolSelectionStrategy(select),
                            createComparator(order),
                            sourceList,
                            poolList,
                            refresh * 1000,
                            permanent,
                            eager,
                            mustMovePins,
                            verify,
                            createLifetimePredicate(pauseWhen),
                            createLifetimePredicate(stopWhen),
                            forceSourceMode);

            if (definition.targetMode.state == CacheEntryMode.State.DELETE
                    || definition.targetMode.state == CacheEntryMode.State.REMOVABLE) {
                throw new IllegalArgumentException(targetMode + ": Invalid value");
            }

            if (id == null) {
                do {
                    id = String.valueOf(_counter++);
                } while (_jobs.containsKey(id));
            } else {
                Job job = _jobs.get(id);
                if (job != null) {
                    switch (job.getState()) {
                    case FAILED:
                    case CANCELLED:
                    case FINISHED:
                        break;
                    default:
                        throw new IllegalArgumentException("Job id is already in use: " + id);
                    }
                }
            }

            Job job = new Job(_context, definition);
            job.setConcurrency(concurrency);
            _jobs.put(id, job);

            _commands.put(job, commandLine);
            return getJobSummary(id);
        }
    }

    @Command(name="migration move",
            description = "Moves replicas to other pools. The source replica is deleted. " +
                    "Accepts the same options as 'migration copy'. Corresponds to\n\n" +
                    "     migration copy -smode=delete -tmode=same -pins=move -verify")
    public class MigrationMoveCommand extends MigrationCopyCommand
    {
        public MigrationMoveCommand()
        {
            select = "proportional";
            target = "pool";
            sourceMode = "delete";
            targetMode = "same";
            refresh = 300;
            pins = "move";
            verify = true;
        }
    }

    @Command(name="migration cache",
            description = "Caches replicas on other pools. Accepts the same options as " +
                    "'migration copy'. Corresponds to\n\n" +
                    "     migration copy -smode=same -tmode=cached")
    public class MigrationCacheCommand extends MigrationCopyCommand
    {
        public MigrationCacheCommand()
        {
            select = "proportional";
            target = "pool";
            sourceMode = "same";
            targetMode = "cached";
            refresh = 300;
            pins = "keep";
            verify = false;
        }
    }

    @Command(name="migration suspend",
            description = "Suspends a migration job. A suspended job finishes ongoing " +
                    "transfers, but is does not start any new transfer.")
    public class MigrationSuspendCommand implements Callable<String>
    {
        @Argument(metaVar="job")
        String id;

        @Override
        public String call()
        {
            Job job = getJob(id);
            job.suspend();
            return getJobSummary(id);
        }
    }

    @Command(name="migration resume",
            description = "Resumes a suspended migration job.")
    public class MigrationResumeCommand implements Callable<String>
    {
        @Argument(metaVar="job")
        String id;

        @Override
        public String call()
        {
            Job job = getJob(id);
            job.resume();
            return getJobSummary(id);
        }
    }

    @Command(name="migration cancel",
            description ="Cancels a migration job.")
    public class MigrationCancelCommand implements Callable<String>
    {
        @Option(name="force", usage="Kill ongoing transfers.")
        boolean force;

        @Argument(metaVar="job")
        String id;

        @Override
        public String call()
        {
            Job job = getJob(id);
            job.cancel(force);
            return getJobSummary(id);
        }
    }

    @Command(name="migration clear",
            description ="Removes completed migration jobs. For reference, information about " +
                    "migration jobs are kept until explicitly cleared.")
    public class MigrationClearCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            Iterator<Job> i = _jobs.values().iterator();
            while (i.hasNext()) {
                Job job = i.next();
                switch (job.getState()) {
                case CANCELLED:
                case FAILED:
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
    }

    @Command(name="migration ls",
            description = "Lists all migration jobs")
    public class MigrationListCommand implements Callable<String>
    {
        @Override
        public String call() throws Exception
        {
            StringBuilder s = new StringBuilder();
            for (String id: _jobs.keySet()) {
                s.append(getJobSummary(id)).append('\n');
            }
            return s.toString();
        }
    }

    @Command(name="migration info",
    description = "Shows detailed information about a migration job. Possible " +
            "job states are:\n\n" +
            "   INITIALIZING   Initial scan of repository\n" +
            "   RUNNING        Job runs (schedules new tasks)\n" +
            "   SLEEPING       A task failed; no tasks are scheduled for 10 seconds\n" +
            "   PAUSED         Pause expression evaluates to true; no tasks for 10 seconds\n" +
            "   STOPPING       Stop expression evaluated to true; waiting for tasks to stop\n" +
            "   SUSPENDED      Job suspended by user; no tasks are scheduled\n" +
            "   CANCELLING     Job cancelled by user; waiting for tasks to stop\n" +
            "   CANCELLED      Job cancelled by user; no tasks are running\n" +
            "   FINISHED       Job completed\n" +
            "   FAILED         Job failed (check log file for details)\n\n" +
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
            "   Done                 The task completed successfully")
    public class MigrationInfoCommand implements Callable<String>
    {
        @Argument(metaVar="job")
        String id;

        @Override
        public String call()
                throws NoSuchElementException
        {
            Job job = getJob(id);
            String command = _commands.get(job);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println("Command    : " + command);
            job.getInfo(pw);
            return sw.toString();
        }
    }

    public synchronized
        void messageArrived(PoolMigrationCopyFinishedMessage message)
    {
        if (!message.getPool().equals(_context.getPoolName())) {
            return;
        }

        for (Job job: _jobs.values()) {
            job.messageArrived(message);
        }
    }

    public Object messageArrived(PoolMigrationJobCancelMessage message)
    {
        try {
            return getJob(message.getJobId()).messageArrived(message);
        } catch (NoSuchElementException e) {
            message.setSucceeded();
            return message;
        }
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        for (String id: _jobs.keySet()) {
            pw.println(getJobSummary(id));
        }
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        pw.println("#\n# MigrationModule\n#");
        for (Job job: _jobs.values()) {
            if (job.getDefinition().isPermanent) {
                switch (job.getState()) {
                case CANCELLED:
                case CANCELLING:
                case STOPPING:
                case FAILED:
                case FINISHED:
                    break;
                default:
                    pw.println(_commands.get(job));
                    break;
                }
            }
        }
    }

    public synchronized boolean isActive(PnfsId id)
    {
        return _context.isActive(id);
    }
}
