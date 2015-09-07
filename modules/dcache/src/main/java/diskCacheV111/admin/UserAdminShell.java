package diskCacheV111.admin;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.GetAllDomainsReply;
import dmg.cells.services.GetAllDomainsRequest;
import dmg.util.AclException;
import dmg.util.AuthorizedString;
import dmg.util.CommandAclException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandThrowableException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;
import dmg.util.command.HelpFormat;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.Args;
import org.dcache.util.Glob;
import org.dcache.util.Version;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.util.concurrent.Futures.*;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static org.dcache.util.Glob.parseGlobToPattern;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.RED;

public class UserAdminShell
        extends CommandInterpreter
        implements Completer
{
    private static final Logger _log =
            LoggerFactory.getLogger(UserAdminShell.class);

    private static final String ADMIN_COMMAND_NOOP = "xyzzy";
    private static final int CD_PROBE_MESSAGE_TIMEOUT_MS = 1000;
    public static final StringsCompleter SHELL_COMMAND_COMPLETER =
            new StringsCompleter("\\c", "\\exception", "\\l", "\\s", "\\sl", "\\sn",
                                 "\\sp", "\\timeout", "\\q", "\\h", "\\?");
    private final Completer POOL_MANAGER_COMPLETER = createRemoteCompleter("PoolManager");
    private final Completer PNFS_MANAGER_COMPLETER = createRemoteCompleter("PnfsManager");

    private CellEndpoint _cellEndpoint;
    private CellStub _acmStub;
    private CellStub _poolManager;
    private CellStub _pnfsManager;
    private CellStub _cellStub;
    private ListDirectoryHandler _list;
    private String _user;
    private String _authUser;
    private long _timeout = TimeUnit.MINUTES.toMillis(5);
    private boolean _fullException;
    private final String _instance;
    private Position _currentPosition = null;
    private Completer _completer;

    public UserAdminShell(String prompt)
    {
        _instance = prompt;
    }

    public void setUser(String user)
    {
        _user = _authUser = user;
    }

    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _cellEndpoint = endpoint;
        _cellStub = new CellStub(_cellEndpoint);
    }

    public void setAcm(CellStub stub)
    {
        _acmStub = stub;
    }

    public void setPoolManager(CellStub stub)
    {
        _poolManager = stub;
    }

    public void setPnfsManager(CellStub stub)
    {
        _pnfsManager = stub;
    }

    public void setListHandler(ListDirectoryHandler list)
    {
        _list = list;
    }

    @Override
    protected Serializable doExecute(CommandEntry entry, Args args, String[] acls)
            throws CommandException
    {
        try {
            checkPermission(acls);
            return super.doExecute(entry, args, acls);
        } catch (AclException e) {
            throw new CommandAclException(e.getPrincipal(), e.getAcl());
        }
    }

    protected String getUser()
    {
        return _user;
    }

    protected void checkPermission(String[] acls) throws AclException
    {
        if (acls.length > 0) {
            AclException e = null;
            for (String acl : acls) {
                try {
                    checkPermission(acl);
                    return;
                } catch (AclException ce) {
                    e = ce;
                }
            }
            throw e;
        }
    }

    public void checkPermission(String aclName)
            throws AclException
    {
        Object[] request = new Object[5];
        request[0] = "request";
        request[1] = "<nobody>";
        request[2] = "check-permission";
        request[3] = getUser();
        request[4] = aclName;
        Object[] r;
        try {
            r = _acmStub.sendAndWait(request, Object[].class);
        } catch (TimeoutCacheException e) {
            throw new AclException(e.getMessage());
        } catch (CacheException | InterruptedException e) {
            throw new AclException("Problem: " + e.getMessage());
        }
        if (r.length < 6 | !(r[5] instanceof Boolean)) {
            throw new AclException("Protocol violation 4456");
        }

        if (!((Boolean) r[5])) {
            throw new AclException(getUser(), aclName);
        }
    }

    /**
     * Asynchronously queries the cells of {@code domain} matching {@code cellPredicate}.
     * <p>
     * The resulting list is sorted and fully qualified. Errors are logged and otherwise ignored.
     */
    private ListenableFuture<List<String>> getCells(String domain, Predicate<String> cellPredicate)
    {
        /* Query System cell and split, filter, sort and expand the answer. */
        ListenableFuture<List<String>> future = transform(
                _cellStub.send(new CellPath("System", domain), "ps", String.class),
                (String s) ->
                        Arrays.stream(s.split("\n"))
                                .filter(cellPredicate)
                                .sorted(CASE_INSENSITIVE_ORDER)
                                .map(cell -> cell + "@" + domain)
                                .collect(toList()));
        /* Log and ignore any errors. */
        return withFallback(future,
                            t -> {
                                _log.debug("Failed to query the System cell of domain {}: {}", domain, t);
                                return immediateFuture(emptyList());
                            });
    }

    /**
     * Asynchronously fetch the list of pools matching the given predicate.
     */
    private ListenableFuture<List<String>> getPools(Predicate<String> predicate)
    {
        return transform(
                _poolManager.send("psu ls pool", String.class),
                (String s) -> Stream.of(s.split("\n"))
                                .filter(predicate)
                                .collect(toList()));
    }

    /**
     * Asynchronously fetch the list of pools of a pool group.
     */
    private ListenableFuture<Stream<String>> getPools(String poolGroup)
    {
        return transform(
                _poolManager.send(new PoolManagerGetPoolsByPoolGroupMessage(singletonList(poolGroup))),
                (PoolManagerGetPoolsByPoolGroupMessage m) ->
                        m.getPools().stream().map(PoolManagerPoolInformation::getName));
    }

    /**
     * Asynchronously fetch the list of pool groups.
     */
    private ListenableFuture<List<String>> getPoolGroups()
    {
        return transform(
                _poolManager.send("psu ls pgroup", String.class),
                (String s) -> asList(s.split("\n")));
    }

    private ListenableFuture<List<String>> getPoolsInGroups(Predicate<String> predicate)
    {
        ListenableFuture<List<String>> poolGroups = getPoolGroups();

        /* Query the pools of each pool group so we have a list of list of pools. */
        ListenableFuture<List<Stream<String>>> pools = transform(
                poolGroups,
                (List<String> groups) ->
                        allAsList(groups.stream().filter(predicate).map(this::getPools).collect(toList())));

        /* Flatten these to form a list of pools. */
        return transform(pools,
                         (List<Stream<String>> l) -> l.stream().flatMap(s -> s).distinct().collect(toList()));
    }

    private List<String> expandCellPatterns(List<String> patterns)
            throws CacheException, InterruptedException, ExecutionException
    {
        /* Query domains and well-known cells on demand. */
        Supplier<Future<Map<String, Collection<String>>>> domains =
                Suppliers.memoize(() -> transform(_cellStub.send(new CellPath("RoutingMgr"),
                                                                 new GetAllDomainsRequest(), GetAllDomainsReply.class),
                                                  GetAllDomainsReply::getDomains));

        List<ListenableFuture<List<String>>> futures = new ArrayList<>();
        for (String pattern : patterns) {
            int i = pattern.indexOf('@');
            if (i >= 0) {
                /* Find the cells of each matching domain.
                 */
                Predicate<String> matchesCellName = toGlobPredicate(pattern.substring(0, i));
                Predicate<String> matchesDomainName = toGlobPredicate(pattern.substring(i + 1));
                CellStub.get(domains.get()).keySet().stream()
                        .filter(matchesDomainName)
                        .sorted(CASE_INSENSITIVE_ORDER)
                        .map(domain -> getCells(domain, matchesCellName))
                        .forEach(futures::add);
                continue;
            }

            i = pattern.indexOf('/');
            if (i >= 0) {
                Predicate<String> matchesPool = toGlobPredicate(pattern.substring(0, i));

                if (i + 1 == pattern.length()) {
                    /* Special case when no pool group is specified - matches over all pools, even those
                     * not in a pool group.
                     */
                    futures.add(transform(getPools(matchesPool),
                                          (List<String> pools) ->
                                                  pools.stream()
                                                          .sorted(CASE_INSENSITIVE_ORDER)
                                                          .collect(toList())));
                } else {
                    /* Find the pools of each matching pool group.
                     */
                    Predicate<String> matchesPoolGroup = toGlobPredicate(pattern.substring(i + 1));
                    futures.add(
                            transform(getPoolsInGroups(matchesPoolGroup),
                                      (List<String> pools) ->
                                              pools.stream()
                                                      .filter(matchesPool)
                                                      .sorted(CASE_INSENSITIVE_ORDER)
                                                      .collect(toList())));
                }
                continue;
            }

            Predicate<String> matchesCellName = toGlobPredicate(pattern);
            /* Add matching well-known cells. */
            CellStub.get(domains.get()).values().stream()
                    .flatMap(Collection::stream)
                    .filter(matchesCellName)
                    .sorted(CASE_INSENSITIVE_ORDER)
                    .map(Collections::singletonList)
                    .map(Futures::immediateFuture)
                    .forEach(futures::add);
        }

        /* Collect and flatten the result. */
        return allAsList(futures).get().stream().flatMap(Collection::stream).collect(toList());
    }

    private Predicate<String> toGlobPredicate(String s)
    {
        return s.isEmpty() ? (String) -> true : parseGlobToPattern(s).asPredicate();
    }


    public String getHello()
    {
        return "dCache (" + Version.of(UserAdminShell.class).getVersion() + ")\n" + "Type \"\\?\" for help.\n";
    }

    public String getPrompt()
    {
        return (_instance == null ? "" : ("[" + _instance + "] ")) +
               (_currentPosition == null ? "(local) " : ("(" + _currentPosition.remoteName + ") ")) +
               getUser() + " > ";
    }

    @Command(name = "\\exception", hint = "controls display of stack traces",
            description = "When enabled, full Java stack traces are displayed on errors.")
    class SetExceptionCommand implements Callable<String>
    {
        @Argument(required = false)
        Boolean trace;

        @Override
        public String call() throws Exception
        {
            if (trace != null) {
                _fullException = trace;
            }
            return "Stack traces on errors are " + (_fullException ? "enabled" : "disabled") + ".";
        }
    }

    @Command(name = "\\timeout", hint = "sets the command timeout",
            description = "Sets the timeout after which command execution is cancelled. " +
                          "Commands can always be cancelled interactively by pressing Ctrl-C.")
    class TimeoutCommand implements Callable<String>
    {
        @Argument(required = false)
        Integer seconds;

        @Override
        public String call() throws Exception
        {
            if (seconds != null) {
                checkArgument(seconds >= 1, "Timeout must be positive.");
                _timeout = TimeUnit.SECONDS.toMillis(seconds);
            }
            return "Timeout is " + (_timeout / 1000) + " seconds.";
        }
    }

    @Command(name = "\\l", hint = "list cells",
            description = "Lists all matching cells. The argument is interpreted as a glob. If no " +
                          "domain suffix is provided, only well known cells are listed. Otherwise " +
                          "all matching cells in all matching domains are listed.")
    class ListCommand implements Callable<String>
    {
        @Argument(required = false, valueSpec = "CELL[@DOMAIN]",
                usage = "A glob pattern. An empty CELL or DOMAIN string matches any name.")
        String[] pattern = {"*"};

        @Override
        public String call() throws Exception
        {
            return String.join("\n", expandCellPatterns(asList(pattern)));
        }
    }

    @Command(name = "\\c", hint = "connect to cell",
            description = "Connect to new cell. May optionally switch to another user.")
    class ConnectCommand implements Callable<String>
    {
        @Argument(index = 0, valueSpec = "CELL[@DOMAIN]",
                usage = "Well known or fully qualified cell name.")
        String name;

        @Argument(required = false, index = 1,
                usage = "Account to connect with.")
        String user;

        @Override
        public String call() throws Exception
        {
            String oldUser = _user;
            try {
                if (user != null) {
                    if (!user.equals(_authUser) && !user.equals(_user)) {
                        try {
                            checkPermission("system.*.newuser");
                        } catch (AclException acle) {
                            checkPermission("system." + user + ".newuser");
                        }
                    }
                    _user = user;
                }
                checkCdPermission(name);
                _currentPosition = resolve(name);
                _completer = null;
            } catch (Throwable e) {
                _user = oldUser;
                throw e;
            }
            return "";
        }

        private Position resolve(String cell) throws InterruptedException
        {
            CellPath address = new CellPath(cell);
            try {
                SettableFuture<CellPath> future = SettableFuture.create();
                _cellEndpoint.sendMessage(new CellMessage(address, ADMIN_COMMAND_NOOP),
                                          new CellMessageAnswerable()
                                          {
                                              @Override
                                              public void answerArrived(CellMessage request, CellMessage answer)
                                              {
                                                  future.set(answer.getSourcePath());
                                              }

                                              @Override
                                              public void exceptionArrived(CellMessage request, Exception exception)
                                              {
                                                  future.setException(exception);
                                              }

                                              @Override
                                              public void answerTimedOut(CellMessage request)
                                              {
                                                  future.setException(new NoRouteToCellException(request, "No reply"));
                                              }
                                          }, MoreExecutors.directExecutor(), CD_PROBE_MESSAGE_TIMEOUT_MS);
                CellPath returnPath = future.get();
                if (address.hops() == 1 && address.getCellDomainName().equals("local")) {
                    return new Position(returnPath.getSourceAddress().toString(), returnPath.revert());
                } else {
                    return new Position(cell, returnPath.revert());
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof NoRouteToCellException) {
                    throw new IllegalArgumentException("Cell does not exist.");
                }
                // Some other failure, but apparently the cell exists
                _log.info("Cell probe failed: {}", e.getCause().toString());
                return new Position(cell, address);
            }
        }
    }

    @Command(name = "\\q", hint = "quit")
    class QuitCommand implements Callable<Serializable>
    {
        @Override
        public Serializable call() throws Exception
        {
            throw new CommandExitException("Done", 0);
        }
    }

    @Command(name = "\\?", hint = "display help for shell commands",
            description = "Shows help for shell commands. Commands that begin with a backslash are always " +
                          "accessible, while other commands are only available when not connected to a cell." +
                          "\n\n" +
                          "When invoked with a specific command, detailed help for that " +
                          "command is displayed. When invoked with a partial command or without " +
                          "an argument, a summary of all matching commands is shown.")
    class ShellHelpCommand implements Callable<String>
    {
        @Argument(valueSpec = "COMMAND", required = false,
                usage = "Partial or full command for which to show help.")
        String[] command = {};

        @Override
        public String call()
        {
            return getHelp(getPreferredHelpFormat(), command);
        }
    }

    private HelpFormat getPreferredHelpFormat()
    {
        return Ansi.isEnabled() ? HelpFormat.ANSI : HelpFormat.PLAIN;
    }

    @Command(name = "\\h", hint = "display help for cell commands",
            description = "Shows help for cell commands." +
                          "\n\n" +
                          "When invoked with a specific command, detailed help for that " +
                          "command is displayed. When invoked with a partial command or without " +
                          "an argument, a summary of all matching commands is shown.")
    class HelpCommand implements Callable<Serializable>
    {
        @Argument(valueSpec = "COMMAND", required = false,
                usage = "Partial or full command for which to show help.")
        String[] command = {};

        @Override
        public Serializable call() throws InterruptedException, CommandException, NoRouteToCellException
        {
            if (_currentPosition == null) {
                return "You are not connected to any cell. Use \\? to display shell commands.";
            } else {
                String cmd = "help -format=" + getPreferredHelpFormat() + " " + String.join(" ", command);
                Serializable reply = sendObject(_currentPosition.remote, new AuthorizedString(_user, cmd));
                return filterHelp(Objects.toString(reply, ""));
            }
        }

        private String filterHelp(String help)
        {
            return Joiner.on('\n').join(filter(Splitter.on('\n').split(help), input -> !input.startsWith("help ")));
        }

    }

    @Command(name = "\\sn", hint = "send pnfsmanager command", allowAnyOption = true,
            acl = {"cell.*.execute", "cell.PnfsManager.execute"},
            description = "Sends COMMAND to the pnfsmanager service. Use \\sn help for a list of supported commands.")
    class NameSpaceCommand implements Callable<Serializable>
    {
        @Argument(usage = "A pnfsmanager command.")
        String[] command;

        @CommandLine
        Args args;

        @Override
        public Serializable call() throws InterruptedException, CommandException, NoRouteToCellException
        {
            return sendObject(_pnfsManager.getDestinationPath(), args.toString());
        }
    }

    @Command(name = "\\sp", hint = "send poolmanager command", allowAnyOption = true,
            acl = {"cell.*.execute", "cell.PoolManager.execute"},
            description = "Sends COMMAND to the poolmanager service. Use \\sp help for a list of supported commands.")
    class PoolManagerCommand implements Callable<Serializable>
    {
        @Argument(usage = "A poolmanager command.")
        String[] command;

        @CommandLine
        Args args;

        @Override
        public Serializable call() throws InterruptedException, NoRouteToCellException, CommandException
        {
            return sendObject(_poolManager.getDestinationPath(), args.toString());
        }
    }

    @Command(name = "\\s", hint = "send command", allowAnyOption = true,
            description = "Sends COMMAND to one or more cells.")
    class SendCommand implements Callable<Serializable>
    {
        @Argument(index = 0, valueSpec = "CELL[@DOMAIN][,CELL[@DOMAIN]]...",
                usage = "List of cell addresses. Wildcards are expanded. An empty CELL or DOMAIN string matches any name.")
        String destination;

        @Argument(index = 1, usage = "A cell command")
        String[] command;

        @CommandLine
        Args args;

        @Override
        public Serializable call()
                throws InterruptedException, ExecutionException, CacheException, AclException,
                CommandException, NoRouteToCellException
        {
            args.shift();
            AuthorizedString command = new AuthorizedString(_user, args.toString());

            /* Special case non-wildcard single cell destinations to avoid the indentation and
             * addition of a cell name header. Makes the command nicer to use in scripts.
             */
            if (!destination.contains(",") && !isExpandable(destination)) {
                return sendObject(destination, command);
            }

            /* Expand wildcards.
             */
            Map<Boolean, List<String>> expandable =
                    Arrays.stream(destination.split(",")).collect(partitioningBy(UserAdminShell::isExpandable));
            Iterable<String> destinations = concat(expandable.get(false), expandCellPatterns(expandable.get(true)));

            return sendToMany(destinations, command);
        }
    }

    @Command(name = "\\sl", hint = "send to locations", allowAnyOption = true,
            description = "Sends COMMAND to all pools hosting a copy of the given file. If the " +
                          "string $1 occurs in the command, the string is replaced by the PNFS ID " +
                          "of the given file.")
    class SendLocationsCommand implements Callable<String>
    {
        @Argument(index = 0, valueSpec = "PNFSID|PATH",
                usage = "The command is submitted to all pools hosting a copy of this file.")
        String file;

        @Argument(index = 1, usage = "A pool command. $1 is substituted for the PNFS ID.")
        String[] command;

        @CommandLine
        Args args;

        @Override
        public String call() throws Exception
        {
            FileAttributes attributes = getFileAttributes(file);
            args.shift();
            AuthorizedString command =
                    new AuthorizedString(_user, args.toString().replace("$1", attributes.getPnfsId().toString()));
            return sendToMany(attributes.getLocations(), command);
        }
    }

    private void checkCdPermission(String remoteName) throws AclException
    {
        int pos = remoteName.indexOf('-');
        String prefix = null;
        if (pos > 0) {
            prefix = remoteName.substring(0, pos);
        }
        try {
            checkPermission("cell.*.execute");
        } catch (AclException acle) {
            try {
                checkPermission("cell." + remoteName + ".execute");
            } catch (AclException acle2) {
                if (prefix == null) {
                    throw acle2;
                }
                try {
                    checkPermission("cell." + prefix + "-pools.execute");
                } catch (AclException acle3) {
                    throw new AclException(getUser(), remoteName);
                }
            }
        }
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (buffer.startsWith("\\") || _currentPosition == null) {
            return completeShell(buffer, cursor, candidates);
        }
        return completeRemote(buffer, cursor, candidates);
    }

    /**
     * Completion function using the currently connected remote cell as a source
     * for completion candidates.
     */
    private int completeRemote(String buffer, int cursor, List<CharSequence> candidates)
    {
        try {
            if (_completer == null) {
                Object help = executeCommand("help");
                if (help == null) {
                    return -1;
                }
                _completer = new HelpCompleter(String.valueOf(help));
            }
            return _completer.complete(buffer, cursor, candidates);
        } catch (CommandException | NoRouteToCellException e) {
            _log.info("Completion failed: {}", e.toString());
            return -1;
        } catch (InterruptedException e) {
            return -1;
        }
    }

    /**
     * Completes the \c command with well-known cells and local cells of the connected domain
     * as a source for completion candidates.
     */
    private int completeConnectCommand(String buffer, int cursor, List<CharSequence> candidates)
    {
        try {
            if (CharMatcher.WHITESPACE.or(CharMatcher.is('/')).matchesAnyOf(buffer)) {
                return -1;
            }
            candidates.addAll(expandCellPatterns(singletonList(buffer + "*")));
            if (!buffer.contains("@") && _currentPosition != null) {
                /* Add local cells in the connected domain too. */
                candidates.addAll(
                        getCells(_currentPosition.remote.getDestinationAddress().getCellDomainName(),
                                 toGlobPredicate(buffer + "*")).get());
            }
            return 0;
        } catch (CacheException | ExecutionException e) {
            _log.info("Completion failed: {}", e.toString());
            return -1;
        } catch (InterruptedException e) {
            return -1;
        }
    }

    /**
     * Completes a cell address wildcard. Does not provide completion for cell paths.
     */
    private int completeCellWildcard(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (buffer.contains(":")) {
            return -1;
        }

        try {
            int i = buffer.indexOf('@');
            if (i > -1) {
                expandCellPatterns(asList(buffer + "*")).stream()
                        .map(s -> s.substring(s.indexOf("@") + 1))
                        .forEach(candidates::add);
                return i + 1;
            }

            i = buffer.indexOf('/');
            if (i  > -1) {
                Predicate<String> predicate = toGlobPredicate(buffer.substring(i + 1) + "*");
                getPoolGroups().get().stream().filter(predicate).forEach(candidates::add);
                return i + 1;
            }

            candidates.addAll(expandCellPatterns(singletonList(buffer + "*")));
            if (_currentPosition != null) {
                candidates.addAll(
                        getCells(_currentPosition.remote.getDestinationAddress().getCellDomainName(),
                                 toGlobPredicate(buffer + "*")).get());
            }
            return 0;
        } catch (CacheException | ExecutionException e) {
            _log.info("Completion failed: {}", e.toString());
            return -1;
        } catch (InterruptedException e) {
            return -1;
        }
    }

    /**
     * Completes the \l command.
     */
    private int completeListCommand(String buffer, int cursor, List<CharSequence> candidates)
    {
        int lastDestinationStart = buffer.lastIndexOf(' ') + 1;
        String lastDestination = buffer.substring(lastDestinationStart);
        int i = completeCellWildcard(lastDestination, lastDestination.length(), candidates);
        return (i == -1) ? -1 : lastDestinationStart + i;
    }

    /**
     * Completes the \s command. Is able to complete the last address of the destination
     * argument. If only a single cell is provided as a destination, the command argument
     * itself is completed too (using that cell as a source for completion candidates).
     */
    private int completeSendCommand(String buffer, int cursor, List<CharSequence> candidates)
    {
        Completable destination = new Completable(buffer, cursor, candidates);

        if (!destination.hasArguments()) {
            int lastDestinationStart = destination.value.lastIndexOf(',') + 1;
            String lastDestination = destination.value.substring(lastDestinationStart);
            int i = completeCellWildcard(lastDestination, lastDestination.length(), candidates);
            return (i == -1) ? -1 : lastDestinationStart + i;
        } else if (!destination.value.contains(",") && !isExpandable(destination.value)) {
            return destination.completeArguments(createRemoteCompleter(destination.value));
        }
        return -1;
    }

    private int completePath(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (buffer.isEmpty()) {
            candidates.add("/");
            return 0;
        } else if (buffer.startsWith("/")) {
            int endIndex = buffer.lastIndexOf('/');
            String dir = buffer.length() == 1 ? "/" : buffer.substring(0, endIndex);
            String file = buffer.substring(endIndex + 1);

            try (DirectoryStream stream = list(dir, file + "*")) {
                for (DirectoryEntry entry : stream) {
                    if (entry.getFileAttributes().getFileType() == FileType.DIR) {
                        candidates.add(entry.getName() + "/");
                    } else {
                        candidates.add(entry.getName());
                    }
                }
            } catch (InterruptedException e) {
                return -1;
            } catch (CacheException e) {
                _log.info("Completion failed: {}", e.toString());
                return -1;
            }
            return endIndex + 1;
        }
        return -1;
    }

    private int completeSendLocationsCommand(String buffer, int cursor, List<CharSequence> candidates)
    {
        Completable path = new Completable(buffer, cursor, candidates);

        if (!path.hasArguments()) {
            return path.complete(this::completePath);
        } else {
            try {
                Collection<String> locations = getFileAttributes(path.value).getLocations();
                if (!locations.isEmpty()) {
                    /* Assume all pools have the same commands. */
                    return path.completeArguments(createRemoteCompleter(Iterables.get(locations, 0)));
                }
            } catch (CacheException e) {
                _log.info("Completion failed: {}", e.toString());
                return -1;
            } catch (InterruptedException e) {
                return -1;
            }
        }
        return -1;
    }

    private DirectoryStream list(String dir, String pattern) throws InterruptedException, CacheException
    {
        return _list.list(Subjects.ROOT, new FsPath(dir), new Glob(pattern), Range.<Integer>all(), EnumSet.of(
                FileAttribute.TYPE));
    }

    private FileAttributes getFileAttributes(String file) throws CacheException, InterruptedException
    {
        /* Lookup file in name space */
        PnfsGetFileAttributes request;
        EnumSet<FileAttribute> attributeSet = EnumSet.of(FileAttribute.LOCATIONS, FileAttribute.PNFSID);
        if (PnfsId.isValid(file)) {
            request = new PnfsGetFileAttributes(new PnfsId(file), attributeSet);
        } else {
            request = new PnfsGetFileAttributes(file, attributeSet);
        }
        return _pnfsManager.sendAndWait(request).getFileAttributes();
    }

    /**
     * Completes local shell commands.
     */
    private int completeShell(String buffer, int cursor, List<CharSequence> candidates)
    {
        Completable command = new Completable(buffer, cursor, candidates);
        if (!command.hasArguments()) {
            return command.complete(SHELL_COMMAND_COMPLETER);
        }

        switch (command.value) {
        case "\\?":
            return command.completeArguments(SHELL_COMMAND_COMPLETER);
        case "\\h":
            if (_currentPosition != null) {
                return command.completeArguments(this::completeRemote);
            }
            break;
        case "\\c":
            return command.completeArguments(this::completeConnectCommand);
        case "\\l":
            return command.completeArguments(this::completeListCommand);
        case "\\s":
            return command.completeArguments(this::completeSendCommand);
        case "\\sl":
            return command.completeArguments(this::completeSendLocationsCommand);
        case "\\sp":
            return command.completeArguments(POOL_MANAGER_COMPLETER);
        case "\\sn":
            return command.completeArguments(PNFS_MANAGER_COMPLETER);
        }
        return -1;
    }

    private Completer createRemoteCompleter(String cell)
    {
        return (buffer, cursor, candidates) -> completeRemote(cell, buffer, cursor, candidates);
    }

    /**
     * Completes remote commands using a particular cell as a source for completion candidates.
     */
    private int completeRemote(String cell, String buffer, int cursor, List<CharSequence> candidates)
    {
        try {
            Serializable help = sendObject(cell, "help");
            if (help == null) {
                return -1;
            }
            HelpCompleter completer = new HelpCompleter(String.valueOf(help));
            return completer.complete(buffer, cursor, candidates);
        } catch (NoRouteToCellException | CommandException e) {
            _log.info("Completion failed: {}", e.toString());
            return -1;
        } catch (InterruptedException e) {
            return -1;
        }
    }

    public Object executeCommand(String str) throws CommandException, InterruptedException, NoRouteToCellException
    {
        _log.info("String command (super) " + str);

        if (str.trim().equals("")) {
            return "";
        }

        Args args = new Args(str);

        if (_currentPosition == null || str.startsWith("\\")) {
            return localCommand(args);
        } else {
            return sendObject(_currentPosition.remote, new AuthorizedString(_user, str));
        }
    }

    private Serializable localCommand(Args args) throws CommandException
    {
        _log.info("Local command {}", args);
        Object or = command(args);
        if (or == null) {
            return "";
        }
        String r = or.toString();
        if (r.length() < 1) {
            return "";
        }
        if (r.substring(r.length() - 1).equals("\n")) {
            return r;
        } else {
            return r + "\n";
        }


    }

    private Serializable sendObject(String cellPath, Serializable object)
            throws NoRouteToCellException, InterruptedException, CommandException
    {
        return sendObject(new CellPath(cellPath), object);
    }

    private Serializable sendObject(CellPath cellPath, Serializable object)
            throws NoRouteToCellException, InterruptedException, CommandException
    {
        try {
            return _cellStub.send(cellPath, object, Serializable.class, _timeout).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (_fullException) {
                return getStackTrace(cause);
            }
            Throwables.propagateIfInstanceOf(cause, Error.class);
            Throwables.propagateIfInstanceOf(cause, NoRouteToCellException.class);
            Throwables.propagateIfInstanceOf(cause, CommandException.class);
            throw new CommandThrowableException(cause.toString(), cause);
        }
    }

    private String sendToMany(Iterable<String> destinations, Serializable object) throws AclException
    {
        /* Check permissions */
        try {
            checkPermission("cell.*.execute");
        } catch (AclException e) {
            for (String cell : destinations) {
                checkPermission("cell." + cell + ".execute");
            }
        }

        /* Submit */
        List<Map.Entry<String, ListenableFuture<Serializable>>> futures = new ArrayList<>();
        for (String cell : destinations) {
            futures.add(immutableEntry(cell, _cellStub.send(new CellPath(cell), object, Serializable.class, _timeout)));
        }

        /* Collect results */
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, ListenableFuture<Serializable>> entry : futures) {
            result.append(Ansi.ansi().bold().a(entry.getKey()).boldOff()).append(":");
            try {
                String reply = Objects.toString(entry.getValue().get(), "");
                if (reply.isEmpty()) {
                    result.append(Ansi.ansi().fg(GREEN).a(" OK").reset()).append("\n");
                } else {
                    result.append("\n");
                    for (String s : reply.split("\n")) {
                        result.append("    ").append(s).append("\n");
                    }
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof NoRouteToCellException) {
                    result.append(Ansi.ansi().fg(RED).a(" Cell is unreachable.").reset()).append("\n");
                } else {
                    result.append(" ").append(Ansi.ansi().fg(RED).a(cause.getMessage()).reset()).append("\n");
                }
            } catch (InterruptedException e) {
                result.append(" ^C\n");

                /* Cancel all uncompleted tasks. Doesn't actually cancel any requests, but will cause
                 * the remaining uncompleted futures to throw a CancellationException.
                 */
                for (Map.Entry<String, ListenableFuture<Serializable>> entry2 : futures) {
                    entry2.getValue().cancel(true);
                }
            } catch (CancellationException e) {
                result.append(" ^C\n");
            }
        }

        return result.toString();
    }

    private String getStackTrace(Throwable obj)
    {
        CharArrayWriter ca = new CharArrayWriter();
        obj.printStackTrace(new PrintWriter(ca));
        return ca.toString();
    }

    private static boolean isExpandable(String s)
    {
        return !s.contains(":") && (s.startsWith("@") || s.endsWith("@") || Glob.isGlob(s) || s.indexOf('/') > -1);
    }

    /**
     * Utility class for completing an input buffer.
     */
    private static class Completable
    {
        final String buffer;
        final String value;
        final String arguments;
        final int argumentPosition;
        final int cursor;
        final List<CharSequence> candidates;

        Completable(String buffer, int cursor, List<CharSequence> candidates)
        {
            int offset = CharMatcher.WHITESPACE.indexIn(buffer);
            if (offset > -1) {
                value = buffer.substring(0, offset);
                int i = CharMatcher.WHITESPACE.negate().indexIn(buffer, offset);
                offset = (i > -1) ? i : buffer.length();
                arguments = buffer.substring(offset);
            } else {
                value = buffer;
                arguments = null;
            }
            this.buffer = buffer;
            this.argumentPosition = offset;
            this.cursor = cursor;
            this.candidates = candidates;
        }

        boolean hasArguments()
        {
            return arguments != null;
        }

        int complete(Completer completer)
        {
            return completer.complete(buffer, cursor, candidates);
        }

        int completeArguments(Completer completer)
        {
            int i = completer.complete(arguments, cursor - argumentPosition, candidates);
            return (i == -1) ? -1 : i + argumentPosition;
        }
    }

    private static class Position
    {
        final String remoteName;
        final CellPath remote;

        Position(String name, CellPath path)
        {
            remoteName = name;
            remote = path;
        }
    }
}
