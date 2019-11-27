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
package org.dcache.srm.shell;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.net.UrlEscapers;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.PEMCredential;
import org.apache.axis.types.URI;
import org.apache.axis.types.UnsignedLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.ExpandWith;
import dmg.util.command.GlobExpander;
import dmg.util.command.Option;

import gov.fnal.srm.util.ConnectionConfiguration;
import gov.fnal.srm.util.OptionParser;

import org.dcache.commons.stats.RequestCounter;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauge;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.client.Transport;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTExtraInfo;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.SrmPingResponse;
import org.dcache.srm.v2_2.SrmRmResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TExtraInfo;
import org.dcache.srm.v2_2.TFileLocality;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TFileType;
import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TPermissionReturn;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLPermissionReturn;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TSupportedTransferProtocol;
import org.dcache.srm.v2_2.TUserPermission;
import org.dcache.util.Args;
import org.dcache.util.ByteUnit;
import org.dcache.util.ColumnWriter;
import org.dcache.util.ColumnWriter.DateStyle;
import org.dcache.util.Glob;
import org.dcache.util.cli.ShellApplication;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.commons.stats.MonitoringProxy.decorateWithMonitoringProxy;
import static org.dcache.srm.SRMInvalidPathException.checkValidPath;
import static org.dcache.util.ByteUnit.KiB;
import static org.dcache.util.StringMarkup.percentEncode;
import static org.dcache.util.TimeUtils.TimeUnitFormat.SHORT;
import static org.dcache.util.TimeUtils.duration;

public class SrmShell extends ShellApplication
{
    private static final long LS_BLOCK_SIZE = KiB.toBytes(4);

    @VisibleForTesting
    static final Pattern DN_WITH_CAPTURED_CN = Pattern.compile("^(?:/.+?=.+?)+?/CN=(?<cn>[^/=]+)(?:/.+?=[^/]*)*$");

    private static abstract class FilenameComparator<T> implements Comparator<T>
    {
        private String stripNonAlphNum(String original)
        {
            int i = CharMatcher.javaLetterOrDigit().indexIn(original);
            return (i > -1) ? original.subSequence(i, original.length()).toString() : original;
        }

        @Override
        public int compare(T o1, T o2)
        {
            String f1 = stripNonAlphNum(getName(o1));
            String f2 = stripNonAlphNum(getName(o2));
            return f1.compareToIgnoreCase(f2);
        }

        protected abstract String getName(T item);
    }

    private static final Comparator<File> FILE_COMPARATOR = new FilenameComparator<File>() {
                @Override
                public String getName(File item)
                {
                    return item.getName();
                }
            };

    private static final Comparator<String> STRING_FILENAME_COMPARATOR = new FilenameComparator<String>() {
                @Override
                public String getName(String item)
                {
                    return item;
                }
            };

    private static final Comparator<StatItem<File,TMetaDataPathDetail>> STATITEM_FILE_COMPARATOR = new FilenameComparator<StatItem<File,TMetaDataPathDetail>>() {
                @Override
                public String getName(StatItem<File,TMetaDataPathDetail> item)
                {
                    return item.getPath().getName();
                }
            };

    private static final Comparator<StatItem<Path,PosixFileAttributes>> STATITEM_PATH_COMPARATOR = new FilenameComparator<StatItem<Path,PosixFileAttributes>>() {
                @Override
                public String getName(StatItem<Path,PosixFileAttributes> item)
                {
                    return item.getPath().getFileName().toString();
                }
            };

    private final FileSystem lfs = FileSystems.getDefault();
    private final SrmFileSystem fs;
    private final URI home;
    private final Map<Integer,FileTransfer> ongoingTransfers = new ConcurrentHashMap<>();
    private final Map<Integer,FileTransfer> completedTransfers = new ConcurrentHashMap<>();
    private final List<String> notifications = new ArrayList<>();
    private final RequestCounters<Method> counters = new RequestCounters<>("requests");
    private final RequestExecutionTimeGauges<Method> gauges = new RequestExecutionTimeGauges<>("requests");
    private final Args shellArgs;

    private enum PromptType { LOCAL, SRM, SIMPLE };
    private enum PermissionOperation { SRM_CHECK_PERMISSION, SRM_LS };

    private URI pwd;
    private Path lcwd = lfs.getPath(".").toRealPath();
    private int nextTransferId = 1;
    private PromptType promptType = PromptType.SRM;
    private volatile boolean isClosed;
    private PermissionOperation checkCdPermission = PermissionOperation.SRM_CHECK_PERMISSION;

    private static File getPath(TMetaDataPathDetail metadata)
    {
        File absPath = new File(metadata.getPath());
        try {
            /* Work-around DPM bug that returns paths like '//dpm' or
             * '/dpm//gla.scotgrid.ac.uk'.  See:
             *
             *    https://ggus.eu/index.php?mode=ticket_info&ticket_id=125321
             */
            return absPath.getCanonicalFile();
        } catch (IOException e) {
            return absPath;
        }
    }

    private void consolePrintln()
    {
        try {
            console.println();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void consolePrintln(CharSequence msg)
    {
        try {
            console.println(msg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void consolePrint(CharSequence msg)
    {
        try {
            console.print(msg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void consolePrintColumns(Collection<? extends CharSequence> items)
    {
        try {
            console.printColumns(items);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] arguments) throws Throwable
    {
        Args args = new Args(arguments);
        if (args.argc() == 0) {
            System.err.println("Usage: srmfs srm://HOST[:PORT][/DIRECTORY]");
            System.err.println("       srmfs httpg://HOST[:PORT]/WEBSERVICE");
            System.exit(4);
        }

        URI uri;
        try {
            uri = new URI(args.argv(0));
        } catch (URI.MalformedURIException e) {
            uri = null;
            System.err.println(args.argv(0) + ":" + e.getMessage());
            System.exit(1);
        }
        args.shift();

        try (SrmShell shell = new SrmShell(uri, args)) {
            closeOnShutdown(shell);
            shell.start(shell.getShellArgs());
            shell.awaitTransferCompletion();
        } catch (SRMException e) {
            System.err.println(uri + " failed request: " + e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * A Ctrl-C will bypass the try-with-resource pattern leaving the
     * {@link Closeable#close} method uncalled.  This method adds a
     * shutdown hook to call this method as part of the JVM shutdown, which
     * ensures the method is called at the risk of calling it twice.
     */
    private static void closeOnShutdown(final Closeable closeable)
    {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    closeable.close();
                } catch (IOException e) {
                    System.err.println("Problem shutting down: " + e);
                }
            }
        });
    }

    public SrmShell(URI uri, Args args) throws Exception
    {
        super();

        ConnectionConfiguration configuration = new ConnectionConfiguration();
        shellArgs = OptionParser.parseOptions(configuration, args);

        String wsPath;
        java.net.URI srmUrl;
        switch (uri.getScheme()) {
        case "srm":
            srmUrl = new java.net.URI(uri.toString());
            wsPath = null; // auto-detect
            break;
        case "httpg":
            srmUrl = new java.net.URI("srm", null,  uri.getHost(), (uri.getPort() > -1 ? uri.getPort() : -1), "/", null, null);
            wsPath = uri.getPath();
            break;
        default:
            throw new IllegalArgumentException("Unknown scheme \"" + uri.getScheme() + "\"");
        }

        X509Credential credential;
        if (configuration.isUseproxy()) {
            credential = new PEMCredential(configuration.getX509_user_proxy(), (char[]) null);
        } else {
            credential = new PEMCredential(configuration.getX509_user_key(), configuration.getX509_user_cert(), null);
        }
        fs = new AxisSrmFileSystem(decorateWithMonitoringProxy(new Class<?>[]{ISRM.class},
                new SRMClientV2(srmUrl, credential,
                        configuration.getRetry_timeout(),
                        configuration.getRetry_num(),
                        configuration.isDelegate(),
                        configuration.isFull_delegation(),
                        configuration.getGss_expected_name(),
                        wsPath,
                        configuration.getX509_user_trusted_certificates(),
                        Transport.GSI),
                counters, gauges));
        fs.setCredential(credential);
        fs.start();
        cd(srmUrl.toASCIIString());
        home = pwd;
    }

    private Args getShellArgs()
    {
        return shellArgs;
    }

    @Override
    protected String getCommandName()
    {
        return "srmfs";
    }

    private List<String> extractPendingNotifications()
    {
        List<String> messages;

        synchronized (notifications) {
            if (notifications.isEmpty()) {
                messages = Collections.emptyList();
            } else {
                messages = new ArrayList<>(notifications);
                notifications.clear();
            }
        }

        return messages;
    }

    @Override
    protected String getPrompt()
    {
        StringBuilder prompt = new StringBuilder();

        List<String> messages = extractPendingNotifications();
        if (!messages.isEmpty()) {
            prompt.append('\n');
            for (String notification : notifications) {
                prompt.append(notification).append('\n');
            }
        }

        switch (promptType) {
        case SRM:
            String uri = pwd.toString();
            if (pwd.getPath().length() > 1) {
                uri = uri.substring(0, uri.length()-1);
            }
            prompt.append(uri).append(' ');
            break;
        case LOCAL:
            prompt.append(lcwd.toString()).append(' ');
            break;
        }
        prompt.append("# ");
        return prompt.toString();
    }

    @Override
    public void close() throws IOException
    {
        if (!isClosed) {
            try {
                fs.close();
                isClosed = true;
            } catch (Exception e) {
                Throwables.propagateIfPossible(e, IOException.class);
                throw new RuntimeException(e);
            }
        }
    }

    public void awaitTransferCompletion() throws InterruptedException
    {
        synchronized (ongoingTransfers) {
            if (!ongoingTransfers.isEmpty()) {
                consolePrintln("Awaiting transfers to finish (Ctrl-C to abort)");

                while (!ongoingTransfers.isEmpty()) {
                    ongoingTransfers.wait();
                    for (String message : extractPendingNotifications()) {
                        System.out.println(message);
                    }
                }
            }
        }
    }

    @Nonnull
    private URI lookup(@Nullable File path) throws URI.MalformedURIException
    {
        if (path == null) {
            return pwd;
        } else {
            return new URI(pwd, percentEncode(path.getPath()));
        }
    }

    private URI[] lookup(File[] paths) throws URI.MalformedURIException
    {
        URI[] surls = new URI[paths.length];
        for (int i = 0; i < surls.length; i++) {
            surls[i] = lookup(paths[i]);
        }
        return surls;
    }

    @SuppressWarnings("fallthrough")
    private void cd(String path) throws URI.MalformedURIException, RemoteException, SRMException, InterruptedException
    {
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        URI uri = new URI(pwd, path);
        checkValidPath(fs.stat(uri).getType() == TFileType.DIRECTORY,
                "Not a directory");

        switch (checkCdPermission) {
        case SRM_CHECK_PERMISSION:
            try {
                TPermissionMode permission = fs.checkPermission(uri);
                if (permission != TPermissionMode.RWX && permission != TPermissionMode.RX && permission != TPermissionMode.WX && permission != TPermissionMode.X) {
                    throw new SRMAuthorizationException("Access denied");
                }
                break;
            } catch (SRMNotSupportedException e) {
                /* StoRM does not support checkPermission:
                 *
                 *     https://ggus.eu/index.php?mode=ticket_info&ticket_id=124634
                 */
                notifications.add("The CheckPermission operation is not supported, using directory listing instead.");
                checkCdPermission = PermissionOperation.SRM_LS;
                // fall-through: use srmLs
            }
        case SRM_LS:
            fs.list(uri, false);
        }

        pwd = uri;
    }

    private String permissionsFor(PosixFileAttributes attr)
    {
        StringBuilder sb = new StringBuilder();

        if (attr.isDirectory()) {
            sb.append('d');
        } else if(attr.isSymbolicLink()) {
            sb.append('l');
        } else if(attr.isOther()) {
            sb.append('o');
        } else if (attr.isRegularFile()) {
            sb.append('-');
        } else {
            sb.append('?');
        }

        Set<PosixFilePermission> permissions = attr.permissions();
        sb.append(permissions.contains(PosixFilePermission.OWNER_READ) ? 'r' : '-');
        sb.append(permissions.contains(PosixFilePermission.OWNER_WRITE) ? 'w' : '-');
        sb.append(permissions.contains(PosixFilePermission.OWNER_EXECUTE) ? 'x' : '-');
        sb.append(permissions.contains(PosixFilePermission.GROUP_READ) ? 'r' : '-');
        sb.append(permissions.contains(PosixFilePermission.GROUP_WRITE) ? 'w' : '-');
        sb.append(permissions.contains(PosixFilePermission.GROUP_EXECUTE) ? 'x' : '-');
        sb.append(permissions.contains(PosixFilePermission.OTHERS_READ) ? 'r' : '-');
        sb.append(permissions.contains(PosixFilePermission.OTHERS_WRITE) ? 'w' : '-');
        sb.append(permissions.contains(PosixFilePermission.OTHERS_EXECUTE) ? 'x' : '-');

        return sb.toString();
    }

    private String permissionsFor(TMetaDataPathDetail entry)
    {
        return permissionsFor(entry.getType())
                + ((entry.getOwnerPermission() == null) ? "???" : permissionsFor(entry.getOwnerPermission().getMode()))
                + ((entry.getGroupPermission() == null) ? "???" : permissionsFor(entry.getGroupPermission().getMode()))
                + permissionsFor(entry.getOtherPermission());
    }

    private String permissionsFor(TFileType type)
    {
        if (type == null) {
            return "?";
        }
        switch (type.getValue()) {
        case TFileType._DIRECTORY:
            return "d";
        case TFileType._LINK:
            return "l";
        case TFileType._FILE:
            return "-";
        default:
            throw new IllegalArgumentException(type.getValue());
        }
    }

    private String permissionsFor(TPermissionMode mode)
    {
        if (mode == null) {
            return "???";
        }
        switch (mode.getValue()) {
        case TPermissionMode._NONE:
            return "---";
        case TPermissionMode._X:
            return "--x";
        case TPermissionMode._W:
            return "-w-";
        case TPermissionMode._WX:
            return "-wx";
        case TPermissionMode._R:
            return "r--";
        case TPermissionMode._RX:
            return "r-x";
        case TPermissionMode._RW:
            return "rw-";
        case TPermissionMode._RWX:
            return "rwx";
        default:
            throw new IllegalArgumentException(mode.getValue());
        }
    }

    private void append(PrintWriter writer, TMetaDataSpace space)
    {
        Integer lifetimeOfReservedSpace = space.getLifetimeAssigned();
        Integer lifetimeLeft = space.getLifetimeLeft();
        TRetentionPolicyInfo retentionPolicyInfo = space.getRetentionPolicyInfo();
        UnsignedLong sizeOfTotalReservedSpace = space.getTotalSize();
        UnsignedLong sizeOfGuaranteedReservedSpace = space.getGuaranteedSize();
        UnsignedLong unusedSize = space.getUnusedSize();

        writer.append("Space token       : ").println(space.getSpaceToken());
        if (space.getOwner() != null) {
            writer.append("Owner             : ").println(space.getOwner());
        }
        if (sizeOfTotalReservedSpace != null) {
            writer.append("Total size        : ").println(sizeOfTotalReservedSpace.longValue());
        }
        if (sizeOfGuaranteedReservedSpace != null) {
            writer.append("Guaranteed size   : ").println(sizeOfGuaranteedReservedSpace.longValue());
        }
        if (unusedSize != null) {
            writer.append("Unused size       : ").println(unusedSize.longValue());
        }
        if (lifetimeOfReservedSpace != null) {
            writer.append("Assigned lifetime : ").println(lifetimeOfReservedSpace);
        }
        if (lifetimeLeft != null) {
            writer.append("Remaining lifetime: ").println(lifetimeLeft);
        }
        if (retentionPolicyInfo != null) {
            TRetentionPolicy retentionPolicy = retentionPolicyInfo.getRetentionPolicy();
            TAccessLatency accessLatency = retentionPolicyInfo.getAccessLatency();
            writer.append("Retention         : ").append(retentionPolicy.toString());
            if (accessLatency != null) {
                writer.append("Access latency: ").append(accessLatency.toString());
            }
            writer.println();
        }
    }

    @Command(name="prompt", hint = "modify prompt",
                    description = "Modify the prompt to show different information."
                            + "\n\n"
                            + "There are three choices of prompt:"
                            + "\n\n"
                            + "    local   show the local current working directory,"
                            + "\n\n"
                            + "    srm     show the current working SURL,"
                            + "\n\n"
                            + "    simple  show only the '#' symbol.")
    public class PromptCommand implements Callable<Serializable>
    {
        @Argument(valueSpec = "local|simple|srm")
        String style;

        @Override
        public Serializable call() throws IllegalArgumentException
        {
            switch (style) {
            case "srm":
                promptType = PromptType.SRM;
                break;
            case "local":
                promptType = PromptType.LOCAL;
                break;
            case "simple":
                promptType = PromptType.SIMPLE;
                break;
            default:
                throw new IllegalArgumentException("Unknown style \"" + style + "\"");
            }
            return null;
        }
    }

    private abstract class AbstractFilesystemExpander implements GlobExpander<String>
    {
        abstract protected void expandInto(List<File> matches, File directory, String glob) throws Exception;

        protected List<String> expand(Glob argument, File cwd)
        {
            String[] elements = argument.toString().split("/");
            boolean isAbsolute = elements.length > 0 && elements [0].isEmpty();

            List<File> expansions = Lists.newArrayList(isAbsolute ? new File("/") : cwd);

            try {
                for (String element : elements) {
                    if (element.isEmpty() || element.equals(".")) {
                        continue;
                    }

                    List<File> newExpansions = new ArrayList<>();

                    if (element.equals("..")) {
                        for (File expansion : expansions) {
                            File parent = expansion.getParentFile();
                            if (parent != null) {
                                newExpansions.add(parent);
                            }
                        }
                    } else {
                        for (File expansion : expansions) {
                            expandInto(newExpansions, expansion, element);
                        }
                    }
                    Collections.sort(newExpansions, FILE_COMPARATOR);
                    expansions = newExpansions;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Collections.emptyList();
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                try {
                    console.println("Failed to expand argument: " + e);
                } catch (IOException ignored) {
                    System.out.println("Failed to expand argument: " + e);
                }
                return Collections.emptyList();
            }

            List<String> results = new ArrayList<>(expansions.size());
            for (File expansion : expansions) {
                if (isAbsolute) {
                    results.add(expansion.toString());
                } else {
                    results.add(cwd.toPath().relativize(expansion.toPath()).toString());
                }
            }

            return results;
        }
    }

    /**
     * A namespace item with corresponding attributes from stat.
     */
    protected static class StatItem<P,A>
    {
        private final P path;
        private final A attrs;

        public StatItem(P path, A attrs)
        {
            this.path = path;
            this.attrs = attrs;
        }

        public P getPath()
        {
            return path;
        }

        public A getAttributes()
        {
            return attrs;
        }
    }

    private abstract class AbstractLsCommand<P,A> implements Callable<Serializable>
    {
        @Option(name = "time", values = { "modify", "atime", "mtime", "create" },
                usage = "Show alternative time instead of modification time: "
                        + "modify/mtime is the last write time, create is the "
                        + "creation time.")
        String time = "mtime";

        @Option(name = "l",
                usage = "List in long format.")
        boolean verbose;

        @Option(name = "h",
                usage = "Use abbreviated file sizes. The values use decimal "
                        + "prefixes; for example, 1 kB is 1000 bytes.")
        boolean abbrev;

        @Option(name = "a",
                usage = "Do not hide files that are normally hidden.")
        boolean showHidden;

        @Option(name = "d",
                usage = "display information about a directory rather than "
                        + "listing the contents.")
        boolean directory;

        protected void acceptArguments(String[] items)
        {
            List<P> listTodo;
            List<StatItem<P,A>> statTodo;

            if (items == null || items.length == 0) {
                listTodo = Collections.singletonList(getCwd());
                statTodo = Collections.emptyList();
            } else {
                listTodo = new ArrayList<>();
                statTodo = new ArrayList<>();

                // The cwd or an ancestor path of cwd are always a directory,
                // so we can avoid doing a stat.
                List<String> statMultipleTodo = new ArrayList<>();
                for (String item : items) {
                    P path = convert(item);
                    if (!directory && isAncestorOrCwd(path)) {
                        listTodo.add(path);
                    } else {
                        statMultipleTodo.add(item);
                    }
                }

                for (StatItem<P,A> item : statMultiple(statMultipleTodo)) {
                    if (!directory && isDirectory(item.getAttributes())) {
                        listTodo.add(item.getPath());
                    } else {
                        statTodo.add(item);
                    }
                }
            }

            if (verbose) {
                listEntries(statTodo, false);
            } else {
                listNames(statTodo);
            }

            boolean needBlank = !statTodo.isEmpty();
            boolean printHeader = !(statTodo.isEmpty() && listTodo.size() == 1);

            for (P dir : listTodo) {
                if (needBlank) {
                    consolePrintln();
                }
                if (printHeader) {
                    consolePrintln(dir + ":");
                }
                if (verbose) {
                    listDirectoryStat(dir);
                } else {
                    listDirectoryNames(dir);
                }
                needBlank = true;
                printHeader = true;
            }
        }

        private void listNames(List<StatItem<P,A>> items)
        {
            List<String> names = new ArrayList<>(items.size());
            for (StatItem<P,A> item : items) {
                names.add(item.getPath().toString());
            }
            consolePrintColumns(names);
        }

        private void listDirectoryNames(P dir)
        {
            try {
                List<String> filtered = new ArrayList<>();
                for (String item : lsDirNames(dir)) {
                    if (!isHidden(convert(item)) || showHidden) {
                        filtered.add(item);
                    }
                }

                Collections.sort(filtered, STRING_FILENAME_COMPARATOR);

                if (showHidden) {
                    filtered.add(0, ".");
                    filtered.add(1, "..");
                }

                consolePrintColumns(filtered);
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                consolePrintln("Failed to list " + dir + ": " + e.getMessage());
            }
        }

        private void listDirectoryStat(P dir)
        {
            try {
                List<StatItem<P,A>> filtered = new ArrayList<>();
                for (StatItem<P,A> item : lsDirStats(dir)) {
                    if (!isHidden(item.getPath()) || showHidden) {
                        filtered.add(item);
                    }
                }

                sortStats(filtered);

                if (showHidden) {
                    try {
                        A dirAttr = stat(dir);

                        P parent = getParent(dir);
                        try {
                            A parentAttr = parent == null ? dirAttr : stat(parent);
                            filtered.add(0, new StatItem<>(getChild(dir, ".."), parentAttr));
                        } catch (Exception e) {
                            Throwables.throwIfUnchecked(e);
                            consolePrintln("Failed to stat ..: " + e.getMessage());
                        }

                        filtered.add(0, new StatItem<>(getChild(dir, "."), dirAttr));
                    } catch (Exception e) {
                        Throwables.throwIfUnchecked(e);
                        consolePrintln("Failed to stat .: " + e.getMessage());
                    }
                }
                listEntries(filtered, true);
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                consolePrintln("Failed to list " + dir + ": " + e.getMessage());
            }
        }

        protected void listEntryNames(Iterable<P> entries, boolean isDirectory)
        {
            List<String> names = new ArrayList<>();
            for (P item : entries) {
                names.add(isDirectory ? name(item) : item.toString());
            }
            consolePrintColumns(names);
        }

        protected void listEntries(Iterable<StatItem<P,A>> entries, boolean isDirectory)
        {
            assert verbose;

            long total = 0;
            ColumnWriter writer = buildColumnWriter();
            for (StatItem<P,A> item : entries) {
                try {
                    A attrs = item.getAttributes();
                    long size = size(attrs);

                    if (!isDirectory(attrs) && size > 0) {
                        total += 1 + (size - 1) / LS_BLOCK_SIZE;
                    }

                    acceptRow(writer, item.getPath(), item.getAttributes(), isDirectory);
                } catch (Exception e) {
                    Throwables.throwIfUnchecked(e);
                    consolePrintln("Cannot stat " + item + ": " + e.toString());
                }
            }

            if (isDirectory) {
                consolePrintln("total " + total*4);
            }
            consolePrint(writer.toString());
        }

        protected abstract P getCwd();
        protected abstract P convert(String item);
        protected abstract P getParent(P item);
        protected abstract P resolveAgainstCwd(P item);
        protected abstract boolean isAncestorOrCwd(P path);
        protected abstract boolean isHidden(P path);
        protected abstract boolean isDirectory(A attrs);
        protected abstract String name(P path);
        protected abstract ColumnWriter buildColumnWriter();
        protected abstract void acceptRow(ColumnWriter writer, P name, A attrs,
                boolean omitPath) throws Exception;

        // Return stated items, if item was found. If item is relative then StatItem path is too.
        protected abstract List<StatItem<P,A>> statMultiple(List<String> items);

        protected abstract A stat(P absPath) throws Exception;
        protected abstract long size(A attr) throws Exception;

        // List contents of directories, returned values are simple names
        protected abstract List<String> lsDirNames(P dir) throws Exception;

        // if dir is relative, StatCache#getPath is relative
        protected abstract List<StatItem<P,A>> lsDirStats(P dir) throws Exception;
        protected abstract void sortStats(List<StatItem<P,A>> contents);
        protected abstract P getChild(P dir, String name);
    }

    /*
     * Commands for local filesystem manipulation
     */

    private class LocalFilesystemExpander extends AbstractFilesystemExpander
    {
        @Override
        protected void expandInto(List<File> matches, File directory, String glob) throws IOException
        {
            // REVISIT: this uses Java's built-in support for glob filters, which
            // is a superset of dCache's Glob class.
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory.toPath(), glob)) {
                for (Path path : directoryStream) {
                    matches.add(path.toFile());
                }
            }
        }

        @Override
        public List<String> expand(Glob argument)
        {
            return expand(argument, lcwd.toFile());
        }
    }

    @Command(name="lpwd", hint = "print local working directory",
                description = "Print the current working directory.  This "
                        + "directory is used as a default value for the lls "
                        + "command and when resolving local relative paths.")
    public class LpwdCommand implements Callable<Serializable>
    {
        @Override
        public Serializable call()
        {
            return lcwd.toString();
        }
    }

    @Command(name="lcd", hint = "change local directory",
                    description = "Change the local directory.  The new path is "
                            + "used as a default value for lls commands "
                            + "and to resolve relative (local) paths.")
    public class LcdCommand implements Callable<Serializable>
    {
        @Argument
        String path;

        @Override
        public Serializable call() throws IllegalArgumentException, IOException
        {
            Path newPath = lcwd.resolve(path).normalize();

            if (!Files.exists(newPath)) {
                throw new IllegalArgumentException("No such directory: " + path);
            }

            if (!Files.isDirectory(newPath)) {
                throw new IllegalArgumentException("Not a directory: " + path);
            }

            if (!Files.isExecutable(newPath)) {
                throw new IllegalArgumentException("Permission denied: " + path);
            }

            lcwd = newPath;
            return null;
        }
    }

    @Command(name="lls", hint = "list contents from local filesystem",
                    description = "List files and directories on the local "
                            + "filesystem.  The arguments may be glob patters "
                            + "that are expanded.  The format of the output is "
                            + "controlled via various options."
                            + "\n\n"
                            + "The output is either in short or long format. "
                            + "Short format lists only the file or directory "
                            + "names.  Long format shows one file or directory "
                            + "per line with additional metadata."
                            + "\n\n"
                            + "Normally files and directories that start with "
                            + "a dot are not shown, but the -a option may be "
                            + "used to see them.  If the -d option is "
                            + "specified then information about a directory is "
                            + "shown rather than than showing information about "
                            + "the content of that directory."
                            + "\n\n"
                            + "If long format is used then further options "
                            + "allow a choice of which timestamp is printed "
                            + "and whether the file size is shown as a number "
                            + "of bytes or using decimal (powers of ten)"
                            + "prefixes.")
    public class LlsCommand extends AbstractLsCommand<Path,PosixFileAttributes>
    {
        @ExpandWith(LocalFilesystemExpander.class)
        @Argument(required = false, metaVar="PATH")
        String[] items;

        @Override
        public Serializable call()
        {
            acceptArguments(items);
            return null;
        }

        @Override
        protected Path getCwd()
        {
            return lcwd;
        }

        @Override
        protected Path convert(String item)
        {
            Path abs = lcwd.resolve(item);
            return item.startsWith("/") ? abs : lcwd.relativize(abs);
        }

        @Override
        protected Path resolveAgainstCwd(Path path)
        {
            return lcwd.resolve(path);
        }

        @Override
        protected boolean isHidden(Path path)
        {
            try {
                return Files.isHidden(path);
            } catch (IOException e) {
                return false;
            }
        }

        @Override
        protected boolean isDirectory(PosixFileAttributes attrs)
        {
            return attrs.isDirectory();
        }

        @Override
        protected boolean isAncestorOrCwd(Path path)
        {
            return lcwd.startsWith(lcwd.resolve(path));
        }

        @Override
        protected Path getParent(Path item)
        {
            return item.getParent();
        }

        @Override
        protected long size(PosixFileAttributes attr) throws IOException
        {
            return attr.size();
        }

        @Override
        protected String name(Path path)
        {
            return path.getFileName().toString();
        }

        @Override
        protected ColumnWriter buildColumnWriter()
        {
            Optional<ByteUnit> displayUnit = abbrev
                    ? Optional.empty()
                    : Optional.of(ByteUnit.BYTES);
            return new ColumnWriter()
                    .left("mode")
                    .space().right("ncount")
                    .space().left("owner")
                    .space().left("group")
                    .space().bytes("size", displayUnit, ByteUnit.Type.DECIMAL)
                    .space().date("time", DateStyle.LS)
                    .space().left("name");
        }

        @Override
        protected void acceptRow(ColumnWriter writer, Path name,
                PosixFileAttributes attrs, boolean omitPath) throws IOException
        {
            writer.row()
                    .value("mode", permissionsFor(attrs))
                    .value("ncount", Files.getAttribute(resolveAgainstCwd(name), "unix:nlink"))
                    .value("owner", attrs.owner().getName())
                    .value("group", attrs.group().getName())
                    .value("size", attrs.size())
                    .value("time", new Date(getFileTime(attrs).toMillis()))
                    .value("name", omitPath ? name.getFileName() : name);
        }

        @Override
        protected List<String> lsDirNames(Path dir) throws IOException
        {
            List<String> names = new ArrayList<>();

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(lcwd.resolve(dir))) {
                for (Path item : stream) {
                    names.add(item.getFileName().toString());
                }
            }

            return names;
        }

        @Override
        protected List<StatItem<Path,PosixFileAttributes>> lsDirStats(Path dir) throws IOException
        {
            List<StatItem<Path,PosixFileAttributes>> statList = new ArrayList<>();

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(lcwd.resolve(dir))) {
                for (Path item : stream) {
                    statList.add(new StatItem<>(item, stat(item)));
                }
            }

            return statList;
        }

        @Override
        protected void sortStats(List<StatItem<Path,PosixFileAttributes>> contents)
        {
            Collections.sort(contents, STATITEM_PATH_COMPARATOR);
        }

        @Override
        protected Path getChild(Path dir, String name)
        {
            return dir.resolve(name);
        }


        @Override
        protected PosixFileAttributes stat(Path absPath) throws IOException
        {
            return Files.getFileAttributeView(absPath,
                    PosixFileAttributeView.class).readAttributes();
        }

        @Override
        protected List<StatItem<Path,PosixFileAttributes>> statMultiple(List<String> items)
        {
            List<StatItem<Path,PosixFileAttributes>> statItems = new ArrayList<>(items.size());

            for (String item : items) {
                Path path = convert(item);
                Path absPath = resolveAgainstCwd(path);
                try {
                    PosixFileAttributes attrs = stat(absPath);
                    statItems.add(new StatItem<>(path, attrs));
                } catch (IOException e) {
                    // simply fail to populate the list.
                }
            }

            return statItems;
        }

        private FileTime getFileTime(PosixFileAttributes attrs)
        {
            switch (time) {
            case "mtime":
            case "modify":
                return attrs.lastModifiedTime();
            case "atime":
                return attrs.lastAccessTime();
            case "create":
                return attrs.creationTime();
            }
            throw new RuntimeException("Unknown time value \"" + time + "\"");
        }
    }

    @Command(name="lrm", hint="remove local files",
                    description = "Remove one or more files from the local "
                            + "filesystem.")
    public class LrmCommand implements Callable<Serializable>
    {
        @Argument
        @ExpandWith(LocalFilesystemExpander.class)
        String[] paths;

        @Override
        public Serializable call() throws IOException
        {
            for (String path : paths) {
                Path file = lcwd.resolve(path);

                if (!Files.exists(file)) {
                    console.println("No such file: " + path);
                    continue;
                }

                if (Files.getFileAttributeView(file, PosixFileAttributeView.class).readAttributes().isDirectory()) {
                    console.println("Is directory: " + path);
                    continue;
                }

                try {
                    Files.delete(file);
                } catch (IOException e) {
                    console.println(e.toString() + ": " + path);
                }
            }

            return null;
        }
    }

    @Command(name="lrmdir", hint="remove local directory",
                    description = "Remove one or more directories if they are "
                            + "empty.")
    public class LrmdirCommand implements Callable<Serializable>
    {
        @Argument
        @ExpandWith(LocalFilesystemExpander.class)
        String[] paths;

        @Override
        public Serializable call() throws IOException
        {
            for (String path : paths) {
                Path file = lcwd.resolve(path);

                if (!Files.exists(file)) {
                    console.println("No such directory: " + path);
                }

                if (!Files.getFileAttributeView(file, PosixFileAttributeView.class).readAttributes().isDirectory()) {
                    console.println("Not a directory: " + path);
                }

                try {
                    Files.delete(file);
                } catch (DirectoryNotEmptyException e) {
                    console.println("Directory not empty: " + path);
                } catch (IOException e) {
                    console.println(e.toString() + ": " + path);
                }
            }

            return null;
        }
    }

    @Command(name="lmkdir", hint="create local directory",
            description = "Create one or more new subdirectories.  The parent "
                    + "directories must already exist.")
    public class LmkdirCommand implements Callable<Serializable>
    {
        @Argument
        @ExpandWith(LocalFilesystemExpander.class)
        String[] paths;

        @Override
        public Serializable call() throws IOException
        {
            for (String path : paths) {
                Path file = lcwd.resolve(path);

                if (Files.exists(file)) {
                    console.println("Already exists: " + path);
                }

                Path parent = file.getParent();

                if (!Files.exists(parent)) {
                    console.println("Does not exist: " + parent);
                }

                if (!Files.getFileAttributeView(parent, PosixFileAttributeView.class).readAttributes().isDirectory()) {
                    console.println("Not a directory: " + parent);
                }

                try {
                    Files.createDirectory(file);
                } catch (IOException e) {
                    console.println(e.toString() + ": " + file);
                }
            }
            return null;
        }
    }

    @Command(name="lmv", hint="move (rename) local files",
            description = "Rename SOURCE to DEST.  If DEST exists and is a file "
                    + "then it is replaced, unless the -n option is specified. "
                    + "If DEST is a directory then the SOURCE is moved into that "
                    + "directory with the same name, unless -T option is "
                    + "specified.  The -n option prevents the comment from "
                    + "overwriting existing data.")
    public class LmvCommand implements Callable<Serializable>
    {
        @Argument(index=0, metaVar="SOURCE")
        String source;

        @Argument(index=1, metaVar="DEST")
        String dest;

        @Option(name="n", usage="fail if the target already exists")
        boolean noClobber;

        @Option(name="T", usage="If DEST is a directory then do not move SOURCE "
                        + "into DEST; instead, SOURCE will replace DEST.")
        boolean destNormal;

        @Override
        public Serializable call() throws IOException
        {
            Path dst = lcwd.resolve(dest);
            Path src = lcwd.resolve(source);

            if (!destNormal && Files.getFileAttributeView(dst, PosixFileAttributeView.class).readAttributes().isDirectory()) {
                dst = dst.resolve(src.getFileName());
            }

            CopyOption[] options = (noClobber)
                    ? new CopyOption[0]
                    : new CopyOption[] {StandardCopyOption.REPLACE_EXISTING};
            try {
                Files.move(src, dst, options);
            } catch (FileAlreadyExistsException e) {
                throw new IllegalArgumentException("File already exists: " + dst, e);
            } catch (DirectoryNotEmptyException e) {
                throw new IllegalArgumentException("Directory not empty: " + dst, e);
            }
            return null;
        }
    }

    /*
     * SRM commands
     */

    /**
     * Expand Glob based on SRM filesystem.  Directory listings are cached
     * for the duration of the command.
     */
    private class SrmFilesystemExpander extends AbstractFilesystemExpander
    {
        private final boolean verboseList;

        private final LoadingCache<URI, TMetaDataPathDetail[]> lsCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<URI, TMetaDataPathDetail[]>() {
                    @Override
                    public TMetaDataPathDetail[] load(URI key) throws Exception
                    {
                        TMetaDataPathDetail[] contents = fs.list(key, verboseList);
                        if (verboseList) {
                            for (TMetaDataPathDetail item : contents) {
                                statCache.put(getPath(item), item);
                            }
                        }
                        return contents;
                    }
                });

        private final LoadingCache<File, TMetaDataPathDetail> statCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<File, TMetaDataPathDetail>() {
                    @Override
                    public TMetaDataPathDetail load(File key) throws Exception
                    {
                        return fs.stat(asURI(key));
                    }
                });


        public SrmFilesystemExpander()
        {
            this(false);
        }

        public SrmFilesystemExpander(boolean verboseList)
        {
            this.verboseList = verboseList;
        }

        private File resolveAgainstCwd(File path)
        {
            return path.isAbsolute() ? path : new File(pwd.getPath(), path.getPath());
        }

        private String escape(String path)
        {
            if (path.isEmpty() || path.equals("/")) {
                return path;
            }

            StringBuilder sb = new StringBuilder();
            for (String element : path.split("/")) {
                sb.append(UrlEscapers.urlPathSegmentEscaper().escape(element));
                sb.append('/');
            }
            return sb.toString();
        }

        private URI asURI(File directory)
        {
            String absPath = resolveAgainstCwd(directory).getPath();
            String escaped = escape(absPath);
            URI path = new URI(pwd);
            try {
                path.setPath(escaped);
            } catch (URI.MalformedURIException e) {
                // Shouldn't happen.
                throw new RuntimeException(e);
            }
            return path;
        }

        private RuntimeException propagate(ExecutionException e) throws RemoteException, SRMException, InterruptedException
        {
            Throwable cause = e.getCause();
            Throwables.throwIfInstanceOf(cause, RemoteException.class);
            Throwables.throwIfInstanceOf(cause, SRMException.class);
            Throwables.throwIfInstanceOf(cause, InterruptedException.class);
            throw new RuntimeException(cause);
        }

        protected TMetaDataPathDetail[] list(File directory) throws RemoteException,
                SRMException, InterruptedException
        {
            try {
                return lsCache.get(asURI(directory));
            } catch (ExecutionException e) {
                throw propagate(e);
            }
        }

        protected TMetaDataPathDetail[] listIfPresent(File directory)
        {
            return lsCache.getIfPresent(asURI(directory));
        }

        protected TMetaDataPathDetail stat(File item) throws RemoteException,
                URI.MalformedURIException, SRMException, InterruptedException
        {
            File absPath = resolveAgainstCwd(item);
            try {
                return statCache.get(absPath);
            } catch (ExecutionException e) {
                throw propagate(e);
            }
        }

        @Override
        protected void expandInto(List<File> matches, File directory, String glob)
                throws URI.MalformedURIException, RemoteException, SRMException, InterruptedException
        {
            Pattern pattern = Glob.parseGlobToPattern(glob);

            for (TMetaDataPathDetail detail : list(directory)) {
                File item = getPath(detail);
                if (!item.getName().startsWith(".") &&
                        pattern.matcher(item.getName()).matches()) {
                    matches.add(item);
                }
            }
        }

        @Override
        public List<String> expand(Glob argument)
        {
            return expand(argument, new File(pwd.getPath()));
        }
    }

    @Command(name = "cd", hint = "change current directory",
                    description = "Modify the current working directory within "
                            + "the SRM endpoint.  This path is used as a default "
                            + "value for the ls command, and to resolve relative "
                            + "paths.")
    public class CdCommand implements Callable<Serializable>
    {
        @Argument(required = false)
        String path;

        @Override
        public Serializable call() throws URI.MalformedURIException, RemoteException, SRMException, InterruptedException
        {
            if (path == null) {
                pwd = home;
            } else {
                cd(path);
            }
            return null;
        }
    }

    @Command(name = "ls", hint = "list directory contents",
            description = "List the contents of a directory or information "
                    + "about a file or directory.  Various options modify "
                    + "which information is provided.")
    public class LsCommand  extends AbstractLsCommand<File,TMetaDataPathDetail>
    {
        private class DelegatingExpander implements GlobExpander<String>
        {
            @Override
            public List<String> expand(Glob glob)
            {
                return expander.expand(glob);
            }
        }

        @Option(name = "full-dn",
                usage = "If server identifies owner with a Distinguished Name, " +
                        "show complete value with long format.  By default, " +
                        "only the first common name (CN) element is shown.")
        boolean fullDn;

        @ExpandWith(DelegatingExpander.class)
        @Argument(required = false, metaVar="PATH")
        String[] items;

        SrmFilesystemExpander expander = new SrmFilesystemExpander(true);

        @Override
        public Serializable call()
        {
            acceptArguments(items);
            return null;
        }

        @Override
        protected File getCwd()
        {
            return new File(pwd.getPath());
        }


        @Override
        protected File convert(String item)
        {
            return new File(item);
        }

        @Override
        protected File resolveAgainstCwd(File path)
        {
            if (path.isAbsolute()) {
                return path;
            } else {
                File resolved = new File(getCwd(), path.getPath());
                try {
                    return resolved.getCanonicalFile();
                } catch (IOException e) {
                    consolePrintln("Problem canonicalising " + resolved + ": " + e.toString());
                    return resolved;
                }
            }
        }


        private String simplifyUserId(String id)
        {
            Matcher m = DN_WITH_CAPTURED_CN.matcher(id);
            return m.matches() ? m.group("cn") : id;
        }

        @Override
        protected File getChild(File dir, String name)
        {
            return new File(dir, name);
        }

        @Override
        protected boolean isHidden(File path)
        {
            return path.getName().startsWith(".");
        }

        @Override
        protected boolean isDirectory(TMetaDataPathDetail attrs)
        {
            return attrs.getType() == TFileType.DIRECTORY;
        }

        @Override
        protected boolean isAncestorOrCwd(File path)
        {
            String cwd = pwd.getPath();
            String absPath = resolveAgainstCwd(path).getPath();
            return cwd.startsWith(absPath) || cwd.equals(absPath);
        }

        @Override
        protected File getParent(File item)
        {
            return item.getParentFile();
        }

        @Override
        protected long size(TMetaDataPathDetail attr) throws Exception
        {
            UnsignedLong size = attr.getSize();
            return size == null ? 0 : size.longValue();
        }

        @Override
        protected String name(File path)
        {
            return path.getName();
        }

        @Override
        protected ColumnWriter buildColumnWriter()
        {
            Optional<ByteUnit> displayUnit = abbrev
                    ? Optional.empty()
                    : Optional.of(ByteUnit.BYTES);
            return new ColumnWriter()
                    .left("mode")
                    .space().left("owner")
                    .space().left("group")
                    .space().bytes("size", displayUnit, ByteUnit.Type.DECIMAL)
                    .space().date("time", DateStyle.LS)
                    .space().left("name");
        }

        @Override
        protected void acceptRow(ColumnWriter writer, File name,
                TMetaDataPathDetail attr, boolean omitPath) throws Exception
        {
            String userId = attr.getOwnerPermission().getUserID();
            writer.row()
                    .value("mode", permissionsFor(attr))
                    .value("owner", fullDn ? userId : simplifyUserId(userId))
                    .value("group", attr.getGroupPermission().getGroupID())
                    .value("size", (attr.getType() == TFileType.FILE) ? attr.getSize().longValue() : null)
                    .value("time", getTime(attr).getTime())
                    .value("name", omitPath ? name.getName() : name);
        }

        @Override
        protected List<StatItem<File,TMetaDataPathDetail>> statMultiple(List<String> items)
        {
            List<StatItem<File,TMetaDataPathDetail>> statItems = new ArrayList<>(items.size());

            Map<File,TMetaDataPathDetail> statCache = buildStatCache(items);
            for (String item : items) {
                File path = convert(item);
                File absPath = resolveAgainstCwd(path);
                TMetaDataPathDetail attrs = statCache.get(absPath);

                try {
                    if (attrs == null && getParent(absPath) == null) {
                        attrs = stat(absPath);
                    }

                    if (attrs == null) {
                        consolePrintln("No such file or directory: " + item);
                    } else {
                        statItems.add(new StatItem<>(path, attrs));
                    }
                } catch (RemoteException | URI.MalformedURIException | SRMException | InterruptedException e) {
                    consolePrintln("Cannot stat /: " + e.toString());
                }
            }

            return statItems;
        }

        private Map<File,TMetaDataPathDetail> buildStatCache(List<String> items)
        {
            Map<File,TMetaDataPathDetail> statCache = new HashMap<>();

            SetMultimap<File,String> plan = HashMultimap.create();
            for (String item : items) {
                File absPath = resolveAgainstCwd(convert(item));
                File dir = getParent(absPath);
                if (dir != null) {
                    plan.put(dir, name(absPath));
                }
            }

            for (File dir : plan.keySet()) {
                Set<String> names = plan.get(dir);
                for (StatItem<File,TMetaDataPathDetail> s : lsDirStatsIfPresent(dir, names)) {
                    statCache.put(s.getPath(), s.getAttributes());
                    plan.remove(dir, name(s.getPath()));
                }
            }

            try {
                if (!plan.isEmpty()) {
                    ArrayList<URI> surlList = new ArrayList<>(plan.size());
                    for (Map.Entry<File,Collection<String>> dirFiles : plan.asMap().entrySet()) {
                        for (String name : dirFiles.getValue()) {
                            File path = new File(dirFiles.getKey() + "/" + name);
                            try {
                                surlList.add(lookup(path));
                            } catch (URI.MalformedURIException ee) {
                                consolePrintln("Failed to stat " + path + ": " + ee.getMessage());
                            }
                        }
                    }

                    URI[] surls = surlList.toArray(new URI[surlList.size()]);
                    for (TMetaDataPathDetail attrs : fs.stat(surls)) {
                        TReturnStatus status = attrs.getStatus();
                        TStatusCode code = status.getStatusCode();
                        File path = getPath(attrs);
                        if (code == TStatusCode.SRM_SUCCESS) {
                            statCache.put(path, attrs);
                        } else {
                            consolePrintln("Problem with " + path + ": "
                                    + code + " " + status.getExplanation());
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (SRMException e) {
                if (e.getStatusCode() != TStatusCode.SRM_FAILURE) {
                    consolePrintln("Problem with multistat: " + e.getMessage());
                }
            } catch (RemoteException e) {
                consolePrintln("Problem with multistat: " + e.getMessage());
            }

            return statCache;
        }

        private List<StatItem<File,TMetaDataPathDetail>> lsDirStatsIfPresent(File dir, Set<String> names)
        {
            TMetaDataPathDetail[] dirContents = expander.listIfPresent(dir);

            if (dirContents == null) {
                return Collections.emptyList();
            }

            List<StatItem<File,TMetaDataPathDetail>> contents = new ArrayList<>(names.size());
            for (TMetaDataPathDetail attrs : dirContents) {
                File absPath = getPath(attrs);

                if (names.contains(absPath.getName())) {
                    contents.add(new StatItem<>(absPath, attrs));
                }

                if (contents.size() == names.size()) {
                    break;
                }
            }

            return contents;
        }

        @Override
        protected List<StatItem<File,TMetaDataPathDetail>> lsDirStats(File dir) throws RemoteException, SRMException, InterruptedException
        {
            List<StatItem<File,TMetaDataPathDetail>> contents = new ArrayList<>();
            for (TMetaDataPathDetail item : expander.list(dir)) {
                contents.add(new StatItem<>(getPath(item), item));
            }
            return contents;
        }

        @Override
        protected List<String> lsDirNames(File dir) throws RemoteException, SRMException, InterruptedException
        {
            List<String> names = new ArrayList<>();
            for (TMetaDataPathDetail item : expander.list(dir)) {
                names.add(getPath(item).getName());
            }
            return names;
        }

        @Override
        protected void sortStats(List<StatItem<File,TMetaDataPathDetail>> items)
        {
            Collections.sort(items, STATITEM_FILE_COMPARATOR);
        }

        @Override
        protected TMetaDataPathDetail stat(File item) throws RemoteException, URI.MalformedURIException, SRMException, InterruptedException
        {
            return expander.stat(item);
        }

        private Calendar getTime(TMetaDataPathDetail entry)
        {
            Calendar time;
            switch (this.time) {
            case "modify":
            case "mtime":
                time = entry.getLastModificationTime();
                break;
            case "create":
                time = entry.getCreatedAtTime();
                break;
            default:
                throw new IllegalArgumentException("Unknown time field: " + this.time);
            }
            return time;
        }
    }

    @Command(name = "stat", hint = "display file status",
                    description = "Provide detailed information about a file or "
                            + "directory.")
    public class StatCommand implements Callable<String>
    {
        private final DateFormat format = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL);

        @Argument(required = false)
        File path;

        @Override
        public String call() throws URI.MalformedURIException, RemoteException, SRMException, InterruptedException
        {
            TMetaDataPathDetail detail = fs.stat(lookup(path));

            UnsignedLong size = detail.getSize();
            TUserPermission ownerPermission = detail.getOwnerPermission();
            TGroupPermission groupPermission = detail.getGroupPermission();
            Calendar createdAtTime = detail.getCreatedAtTime();
            Calendar lastModificationTime = detail.getLastModificationTime();
            TRetentionPolicyInfo retentionPolicyInfo = detail.getRetentionPolicyInfo();
            TFileLocality fileLocality = detail.getFileLocality();
            TFileStorageType fileStorageType = detail.getFileStorageType();
            ArrayOfString arrayOfSpaceTokens = detail.getArrayOfSpaceTokens();
            String checkSumType = detail.getCheckSumType();
            String checkSumValue = detail.getCheckSumValue();

            StringWriter out = new StringWriter();
            PrintWriter writer = new PrintWriter(out);

            writer.append("      Path: ").println(detail.getPath());
            writer.append("      Size: ").append(String.format("%,d", size.longValue())).append("      File type: ").append(detail.getType().getValue().toLowerCase()).append("");
            if (!isNullOrEmpty(checkSumType) || !isNullOrEmpty(checkSumValue)) {
                writer.append("  Checksum: ").append(nullToEmpty(checkSumType))
                        .append("/").println(nullToEmpty(checkSumValue));
            }
            writer.append("    Access: (").append(permissionsFor(detail)).append(")    Uid: (").append(ownerPermission.getUserID());
            if (groupPermission != null) {
                writer.append(")    Gid: (").append(groupPermission.getGroupID());
            }
            writer.println(")");
            if (createdAtTime != null) {
                writer.append("    Create: ").println(format.format(createdAtTime.getTime()));
            }
            writer.append("    Modify: ").println(format.format(lastModificationTime.getTime()));
            if (retentionPolicyInfo != null) {
                TRetentionPolicy retentionPolicy = retentionPolicyInfo.getRetentionPolicy();
                TAccessLatency accessLatency = retentionPolicyInfo.getAccessLatency();
                writer.append(" Retention: ").append(retentionPolicy.getValue().toLowerCase());
                if (accessLatency != null) {
                    writer.append("     Latency: ").append(
                            accessLatency.getValue().toLowerCase());
                }
                writer.println();
            }
            if (arrayOfSpaceTokens != null) {
                writer.append("    Spaces: ").println(asList(arrayOfSpaceTokens.getStringArray()));
            }
            if (fileLocality != null) {
                writer.append("  Locality: ").println(fileLocality.getValue().toLowerCase());
            }
            if (fileStorageType != null) {
                writer.append("Durability: ").append(fileStorageType.getValue().toLowerCase());
                if (fileStorageType != TFileStorageType.PERMANENT) {
                    Integer lifetimeAssigned = detail.getLifetimeAssigned();
                    Integer lifetimeLeft = detail.getLifetimeLeft();
                    if (lifetimeAssigned != null) {
                        writer.append("    Lifetime assigned: ").print(lifetimeAssigned.intValue());
                    }
                    writer.append("    Lifetime left: ").print(lifetimeLeft.intValue());
                }
                writer.println();
            }
            return out.toString();
        }
    }

    @Command(name = "ping", hint = "ping server",
            description = "Test whether the SRM endpoint is responding and "
                    + "discover the information the server provides.")
    public class PingCommand implements Callable<String>
    {
        @Override
        public String call() throws RemoteException, SRMException
        {
            SrmPingResponse response = fs.ping();

            StringBuilder sb = new StringBuilder();
            sb.append(response.getVersionInfo()).append("\n");
            if (response.getOtherInfo() != null) {
                ArrayOfTExtraInfo info = response.getOtherInfo();
                TExtraInfo[] extraInfoArray = info.getExtraInfoArray();
                if (extraInfoArray != null) {
                    for (TExtraInfo extraInfo : extraInfoArray) {
                        sb.append(extraInfo.getKey()).append(" = ").append(extraInfo.getValue()).append("\n");
                    }
                }
            }

            return sb.toString();
        }
    }

    @Command(name = "show transfer protocols", hint = "discover supported transfer protocols",
                    description = "Query the SRM server to discover which "
                            + "transfer protocols it supports.")
    public class TransferProtocolsCommand implements Callable<String>
    {
        @Override
        public String call() throws RemoteException, SRMException
        {
            ColumnWriter writer = new ColumnWriter().left("protocol").space().left("extra");
            for (TSupportedTransferProtocol protocol : fs.getTransferProtocols()) {
                ColumnWriter.TabulatedRow row = writer.row();
                row.value("protocol", protocol.getTransferProtocol());
                if (protocol.getAttributes() != null) {
                    row.value("extra",
                              Joiner.on(",").withKeyValueSeparator("=")
                                      .join(transform(asList(protocol.getAttributes().getExtraInfoArray()),
                                                      new ToEntry())));
                }
            }
            return writer.toString();
        }

        private class ToEntry implements Function<TExtraInfo, Map.Entry<?, ?>>
        {
            @Override
            public Map.Entry<?, ?> apply(TExtraInfo info)
            {
                return Maps.immutableEntry(info.getKey(),
                                           info.getValue());
            }
        }
    }

    @Command(name = "mkdir", hint = "make directory", description = "Create "
                    + "one or more subdirectories on the server.  By default, "
                    + "the parent directories must already exist.  If the -p "
                    + "option is specified then missing parent directories "
                    + "are created as necessary.")
    public class MkdirCommand implements Callable<String>
    {
        @Argument
        @ExpandWith(SrmFilesystemExpander.class)
        File path;

        @Option(name = "p", usage = "do not fail if the directory already "
                        + "exists and create parent directories as necessary.")
        boolean parent;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException, InterruptedException
        {
            if (parent) {
                recursiveMkdir(path);
            } else {
                fs.mkdir(lookup(path));
            }
            return null;
        }

        private void recursiveMkdir(File path) throws RemoteException, URI.MalformedURIException, SRMException, InterruptedException
        {
            URI surl = lookup(path);
            try {
                fs.mkdir(surl);
            } catch (SRMInvalidPathException e) {
                File parent = path.getParentFile();
                if (parent != null) {
                    recursiveMkdir(parent);
                    fs.mkdir(surl);
                }
            } catch (SRMDuplicationException e) {
                if (fs.stat(surl).getType() != TFileType.DIRECTORY) {
                    throw e;
                }
            }
        }
    }

    @Command(name = "rmdir", hint = "remove empty directories",
                    description = "Remove one or more directories that are "
                            + "empty.  If the -r option is specified then the "
                            + "removal is recursive, so that the command will "
                            + "succeed if the target directory and any "
                            + "subdirectories contain no files.")
    public class RmdirCommand implements Callable<String>
    {
        @Argument
        @ExpandWith(SrmFilesystemExpander.class)
        File[] paths;

        @Option(name = "r", usage = "delete recursively")
        boolean recursive;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException
        {
            for (File path : paths) {
                try {
                    fs.rmdir(lookup(path), recursive);
                } catch (RemoteException | SRMException e) {
                    try {
                        console.println(e.toString() + ": " + path);
                    } catch (IOException ignored) {
                        // ignored
                    }
                }
            }
            return null;
        }
    }

    @Command(name = "rm", hint = "remove directory entries",
                    description = "Remove one or more directory items.  All of "
                            + "the targets must be non-directory items.")
    public class RmCommand implements Callable<String>
    {
        @Argument
        @ExpandWith(SrmFilesystemExpander.class)
        File[] paths;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException
        {
            SrmRmResponse response = fs.rm(lookup(paths));
            if (response.getReturnStatus().getStatusCode() != TStatusCode.SRM_SUCCESS) {
                return Joiner.on('\n').join(
                        transform(filter(asList(response.getArrayOfFileStatuses().getStatusArray()),
                                         new HasFailed()),
                                  new GetExplanation()));
            }
            return null;
        }

        private class HasFailed implements Predicate<TSURLReturnStatus>
        {
            @Override
            public boolean apply(TSURLReturnStatus status)
            {
                return status.getStatus().getStatusCode() != TStatusCode.SRM_SUCCESS;
            }
        }

        private class GetExplanation implements Function<TSURLReturnStatus, Object>
        {
            @Override
            public Object apply(TSURLReturnStatus status)
            {
                return status.getSurl().getPath() + ": " + status.getStatus().getExplanation();
            }
        }
    }

    @Command(name = "mv", hint = "move (rename) file or directory",
            description = "Move or rename a file or directory.")
    public class MvCommand implements Callable<String>
    {
        @Argument(index = 0)
        File source;

        @Argument(index = 1)
        File dest;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException
        {
            fs.mv(lookup(source), lookup(dest));
            return null;
        }
    }

    @Command(name = "list spaces", hint = "discover space reservations",
                    description = "Discover the space reservations currently "
                            + "available to this user.  If description is "
                            + "supplied then only reservations that were "
                            + "created with this description are listed; "
                            + "otherwise all reservations are listed.")
    public class GetSpaceTokensCommand implements Callable<String>
    {
        @Argument(required = false, usage = "The description supplied when "
                        + "creating this reservation.")
        String description;

        @Override
        public String call() throws RemoteException, IOException, SRMException
        {
            console.printColumns(asList(fs.getSpaceTokens(description)));
            return null;
        }
    }

    @Command(name = "show permissions", hint = "describe permissions on SURL",
                    description = "Query detailed information about the "
                            + "permissions of files and directories.")
    public class ShowPermissionCommand implements Callable<String>
    {
        @Argument
        @ExpandWith(SrmFilesystemExpander.class)
        File[] paths;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException
        {
            TPermissionReturn[] permissions = fs.getPermissions(lookup(paths));

            StringWriter out = new StringWriter();
            PrintWriter writer = new PrintWriter(out);
            if (permissions.length == 1) {
                TPermissionReturn permission = permissions[0];
                append(writer, "", permission);
            } else {
                for (TPermissionReturn permission : permissions) {
                    writer.append(permission.getSurl().getPath()).println(':');
                    append(writer, "\t", permission);
                    writer.println();
                }
            }
            return out.toString();
        }

        private void append(PrintWriter writer, String prefix, TPermissionReturn permission)
        {
            TReturnStatus status = permission.getStatus();
            if (status != null && status.getStatusCode() != TStatusCode.SRM_SUCCESS) {
                writer.append(prefix).println(status.getExplanation());
            } else {
                if (permission.getOwnerPermission() != null) {
                    append(writer, prefix, "owner", permission.getOwnerPermission(), permission.getOwner());
                }
                if (permission.getArrayOfUserPermissions() != null) {
                    for (TUserPermission p : permission.getArrayOfUserPermissions().getUserPermissionArray()) {
                        append(writer, prefix, "user", p.getMode(), p.getUserID());
                    }
                }
                if (permission.getArrayOfGroupPermissions() != null) {
                    for (TGroupPermission p : permission.getArrayOfGroupPermissions().getGroupPermissionArray()) {
                        append(writer, prefix, "group", p.getMode(), p.getGroupID());
                    }
                }
                if (permission.getOtherPermission() != null) {
                    append(writer, prefix, "other", permission.getOtherPermission(), "");
                }
            }
        }

        private void append(PrintWriter writer, String prefix, String type, TPermissionMode mode, String name)
        {
            writer.append(prefix).append(permissionsFor(mode)).append(' ').append(type).append(' ').println(name == null ? "(unknown)" : name);
        }
    }

    @Command(name = "check permissions", hint = "check client permissions on SURLs",
                    description = "Check the (effective) permissions on files "
                            + "and directories for the current user.  The result "
                            + "is a list of operations that this user is "
                            + "allowed to do.")
    public class CheckPermissionCommand implements Callable<String>
    {
        @Argument
        @ExpandWith(SrmFilesystemExpander.class)
        File[] paths;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException
        {
            TSURLPermissionReturn[] permissions = fs.checkPermissions(lookup(paths));

            if (permissions.length == 1) {
                TSURLPermissionReturn permission = permissions[0];
                if (permission.getStatus().getStatusCode() != TStatusCode.SRM_SUCCESS) {
                    return permission.getStatus().getExplanation();
                }
                return permissionsFor(permission.getPermission());
            } else {
                StringWriter out = new StringWriter();
                PrintWriter writer = new PrintWriter(out);
                for (TSURLPermissionReturn permission : permissions) {
                    writer.append(permissionsFor(permission.getPermission())).append(' ').append(
                            permission.getSurl().getPath());
                    if (permission.getStatus().getStatusCode() != TStatusCode.SRM_SUCCESS) {
                        writer.append(" (").append(permission.getStatus().getExplanation()).append(')');
                    }
                    writer.println();
                }
                return out.toString();
            }
        }
    }

    @Command(name = "reserve space", hint = "create space reservation",
                    description = "Reserve space into which data may be uploaded. "
                            + "A space reservation is a promise from the "
                            + "storage system to accept some amount of data.")
    public class ReserveSpaceCommand implements Callable<String>
    {
        @Option(name = "al", required = false,
                values = { "NEARLINE", "ONLINE" },
                usage = "The desired access latency of the space reservation. "
                        + "If not specified then the remote system will use "
                        + "an implementation-specific strategy to decide which "
                        + "value is used. The values have the following meaning:"
                        + "\n\n"
                        + "    ONLINE    the lowest latency possible."
                        + "\n\n"
                        + "    NEARLINE  files can have their latency improved "
                        + "automatically.")
        String al;

        @Option(name = "rp", required = true,
                values = { "REPLICA", "OUTPUT", "CUSTODIAL" },
                usage = "The desired retention policy of the space reservation."
                        + "The values have the following meaning:"
                        + "\n\n"
                        + "    REPLICA    highest probability of loss,"
                        + "\n\n"
                        + "    OUTPUT     intermediate probability of loss,"
                        + "\n\n"
                        + "    CUSTODIAL  lowest probability of loss.")
        String rp;

        @Option(name = "lifetime", usage = "The number of seconds the "
                        + "reservation should be honoured.  If not specified "
                        + "then the resulting lifetime is implementation "
                        + "specific; however, it will be infinite if that is "
                        + "supported by the SRM service.")
        int lifetime = -1;

        @Argument(index = 0)
        long size;

        @Argument(index = 1, required = false)
        String description;

        @Override
        public String call() throws RemoteException, SRMException, InterruptedException
        {
            TMetaDataSpace space =
                    fs.reserveSpace(size, description,
                                    (al == null) ? null : TAccessLatency.fromString(al),
                                    TRetentionPolicy.fromString(rp),
                                    lifetime);
            StringWriter out = new StringWriter();
            PrintWriter writer = new PrintWriter(out);
            append(writer, space);
            return out.toString();
        }
    }

    @Command(name = "release space", hint = "release space reservation",
                    description = "Release the reserved space from within "
                            + "storage system.  Once this operation completes "
                            + "successfully, no further uploads may use the "
                            + "reservation.  Files that have been uploaded "
                            + "into the released space reservation are "
                            + "unaffected by this operation.")
    public class ReleaseSpaceCommand implements Callable<String>
    {
        @Argument
        String spaceToken;

        @Override
        public String call() throws RemoteException, SRMException
        {
            fs.releaseSpace(spaceToken);
            return null;
        }
    }

    @Command(name = "show space", hint = "show information about a space reservation",
                    description = "Discover information about a specific space "
                            + "reservation.")
    public class SpaceMetaDataCommand implements Callable<String>
    {
        @Argument
        String spaceToken;

        @Override
        public String call() throws RemoteException, SRMException
        {
            StringWriter out = new StringWriter();
            PrintWriter writer = new PrintWriter(out);
            TMetaDataSpace space = fs.getSpaceMetaData(spaceToken);
            append(writer, space);
            return out.toString();
        }
    }

    private int addOngoingTransfer(FileTransfer transfer)
    {
        final int id = nextTransferId++;

        synchronized (ongoingTransfers) {
            ongoingTransfers.put(id,transfer);
            ongoingTransfers.notifyAll();
        }

        return id;
    }

    private FileTransfer removeOngoingTransfer(int id)
    {
        FileTransfer transfer;

        synchronized (ongoingTransfers) {
            transfer = ongoingTransfers.remove(id);
            ongoingTransfers.notifyAll();
        }

        return transfer;
    }

    @Command(name = "get", hint = "download a file",
                    description = "Download a file from the storage system.  "
                            + "The remote file path is optional.  If not "
                            + "specified then a file is created in the current "
                            + "local working directory with the same name as the"
                            + "remote file ")
    public class GetCommand implements Callable<String>
    {
        @Argument(index=0, usage="path to the remote file")
        File remote;

        @Argument(index=1, required=false, usage="path of the downloaded file")
        String local;

        @Override
        public String call() throws URI.MalformedURIException
        {
            URI surl = lookup(remote);
            Path target = local == null ? lcwd.resolve(remote.getName()) : lcwd.resolve(local);

            FileTransfer transfer = fs.get(surl, target.toFile());

            if (transfer == null) {
                return "No support for download.";
            }

            final int id = addOngoingTransfer(transfer);

            Futures.addCallback(transfer, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result)
                {
                    synchronized (notifications) {
                        notifications.add("[" + id + "] Transfer completed.");
                    }
                    FileTransfer successfulTransfer = removeOngoingTransfer(id);
                    completedTransfers.put(id, successfulTransfer);
                }

                @Override
                public void onFailure(Throwable t)
                {
                    String msg = t.getMessage();
                    synchronized (notifications) {
                        notifications.add("[" + id + "] Transfer failed: " + msg == null ? t.toString() : msg);
                    }
                    FileTransfer failedTransfer = removeOngoingTransfer(id);
                    completedTransfers.put(id, failedTransfer);
                }
            });

            return "[" + id + "] transfer started.";
        }
    }

    @Command(name = "put", hint = "upload a file", description = "Upload a file "
                    + "into the SRM storage.  The remote argument is optional."
                    + "If omitted, the file will be uploaded in the current "
                    + "working directory in the SRM with the same name as the "
                    + "source.")
    public class PutCommand implements Callable<String>
    {
        @Argument(index=0, usage="path of the file to upload")
        String local;

        @Argument(index=1, usage = "path to store the file under", required=false)
        File remote;

        @Override
        public String call() throws URI.MalformedURIException
        {
            File source = lcwd.resolve(local).toFile();

            if (!source.exists()) {
                return "does not exist: " + local;
            }

            if (!source.isFile()) {
                return "not a file: " + local;
            }

            if (!source.canRead()) {
                return "cannot read: " + local;
            }

            URI surl = (remote != null) ? lookup(remote) : lookup(new File(source.getName()));

            FileTransfer transfer = fs.put(source, surl);

            if (transfer == null) {
                return "No support for upload.";
            }

            final int id = addOngoingTransfer(transfer);

            Futures.addCallback(transfer, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result)
                {
                    synchronized (notifications) {
                        notifications.add("[" + id + "] Transfer completed.");
                    }
                    FileTransfer successfulTransfers = removeOngoingTransfer(id);
                    completedTransfers.put(id, successfulTransfers);
                }

                @Override
                public void onFailure(Throwable t)
                {
                    synchronized (notifications) {
                        notifications.add("[" + id + "] Transfer failed: " + t.toString());
                    }
                    FileTransfer failedTransfer = removeOngoingTransfer(id);
                    completedTransfers.put(id, failedTransfer);
                }
            });

            return "[" + id + "] transfer started.";
        }
    }

    @Command(name = "transfer clear", hint="clear completed transfers",
                    description = "Clear the log of completed and failed "
                            + "transfers.  Ongoing transfers are unaffected "
                            + "by this command")
    public class TransferClearCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            completedTransfers.clear();
            return "";
        }
    }

    @Command(name = "transfer ls", hint = "show all ongoing transfers",
                    description = "Show a list of all ongoing transfers.")
    public class TransferListCommand implements Callable<String>
    {
        @Argument(required=false, usage="the ID of a specific transfer")
        Integer id;

        @Override
        public String call()
        {
            StringBuilder sb = new StringBuilder();

            synchronized (ongoingTransfers) {
                if (id == null) {
                    if (ongoingTransfers.isEmpty() && completedTransfers.isEmpty()) {
                        sb.append("No transfers.");
                    } else {
                        if (!ongoingTransfers.isEmpty()) {
                            sb.append("Ongoing transfer:\n");
                            for (Map.Entry<Integer,FileTransfer> e : ongoingTransfers.entrySet()) {
                                sb.append("  [").append(e.getKey()).append("] ");
                                sb.append(e.getValue().getStatus()).append('\n');
                            }
                        }
                        if (!completedTransfers.isEmpty()) {
                            if (!ongoingTransfers.isEmpty()) {
                                sb.append('\n');
                            }
                            sb.append("Completed transfers:\n");
                            for (Map.Entry<Integer,FileTransfer> e : completedTransfers.entrySet()) {
                                sb.append("  [").append(e.getKey()).append("] ");
                                sb.append(e.getValue().getStatus()).append('\n');
                            }
                        }
                        sb.deleteCharAt(sb.length()-1);
                    }
                } else {
                    FileTransfer transfer = ongoingTransfers.get(id);
                    if (transfer == null) {
                        transfer = completedTransfers.get(id);
                    }
                    if (transfer == null) {
                        return "Unknown transfer: " + id;
                    }
                    sb.append('[').append(id).append("] ").append(transfer.getStatus());
                }
            }

            return sb.toString();
        }
    }

    @Command(name = "transfer cancel", hint = "abort an ongoing transfer",
                    description = "Stop a queued or active transfer.")
    public class TransferCancelCommand implements Callable<String>
    {
        @Argument(usage="the ID of the transfer")
        int id;

        @Override
        public String call()
        {
            FileTransfer transfer = removeOngoingTransfer(id);
            if (transfer == null) {
                if (completedTransfers.containsKey(id)) {
                    return "Transfer " + id + " has already completed.";
                } else {
                    return "No such transfer " + id + ".";
                }
            }

            transfer.cancel(true);
            return "Transfer aborted.";
        }
    }

    @Command(name = "option ls", hint = "show available configuration",
                    description = "Show the available list of options.")
    public class OptionLsCommand implements Callable<String>
    {
        @Argument(usage="the specific option to query", required=false)
        String key;

        @Override
        public String call()
        {
            StringBuilder sb = new StringBuilder();
            Map<String,String> options = fs.getTransportOptions();

            if (key == null) {
                for (Map.Entry<String,String> entry : options.entrySet()) {
                    sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
                }

                if (!options.isEmpty()) {
                    sb.deleteCharAt(sb.length()-1);
                }
            } else {
                String value = options.get(key);

                if (value != null) {
                    sb.append(key).append(": ").append(value);
                } else {
                    sb.append("Unknown key: ").append(key);
                }
            }

            return sb.toString();
        }
    }

    @Command(name = "option set", hint = "alter a configuration setting",
                    description = "List all the available options.")
    public class OptionSetCommand implements Callable<String>
    {
        @Argument(index=0, usage="the specific option to update")
        String key;

        @Argument(index=1, usage="the specific option to update")
        String value;

        @Override
        public String call()
        {
            fs.setTransportOption(key, value);
            return "";
        }
    }

    @Command(name = "show statistics", hint = "show SRM call statistics",
                    description = "Show statistics on SRM requests.")
    public class StatisticsShowCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            ColumnWriter requests = new ColumnWriter()
                    .header("Operation").left("operation").space()
                    .header("Requests").right("requests").space()
                    .header("Success").right("success").space()
                    .header("Fail").right("fail").space()
                    .header("Mean").right("mean")
                    .header(" ").right("mean-sd")
                    .header("StdDev").right("sd").space()
                    .header("Min").right("min").space()
                    .header("Max").right("max");

            for (Method m : counters.keySet()) {
                RequestCounter c = counters.getCounter(m);
                RequestExecutionTimeGauge g = gauges.getGauge(m);
                requests.row().value("operation", m.getName())
                        .value("requests", c.getTotalRequests())
                        .value("success", c.getSuccessful())
                        .value("fail", c.getFailed())
                        .value("mean", duration((long)Math.floor(g.getAverageExecutionTime()+0.5), MILLISECONDS, SHORT))
                        .value("mean-sd", "\u00B1")
                        .value("sd", duration((long)Math.floor(g.getStandardDeviation()+0.5), MILLISECONDS, SHORT))
                        .value("min", duration(g.getMinExecutionTime(), MILLISECONDS, SHORT))
                        .value("max", duration(g.getMaxExecutionTime(), MILLISECONDS, SHORT));
            }

            requests.row("");

            RequestCounter total = counters.getTotalRequestCounter();
            requests.row().value("operation", "TOTALS")
                    .value("requests", total.getTotalRequests())
                    .value("success", total.getSuccessful())
                    .value("fail", total.getFailed());

            return requests.toString();
        }
    }

    @Command(name = "clear statistics", hint = "reset all statistics",
                    description = "Reset SRM call statistics.")
    public class StatisticsResetCommand  implements Callable<String>
    {
        @Override
        public String call()
        {
            counters.reset();
            gauges.reset();
            return "";
        }
    }
}
