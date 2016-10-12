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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.PEMCredential;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import gov.fnal.srm.util.Configuration;
import gov.fnal.srm.util.OptionParser;

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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.client.Transport;
import org.dcache.srm.util.SrmUrl;
import org.dcache.srm.v2_2.ArrayOfString;
import org.dcache.srm.v2_2.ArrayOfTExtraInfo;
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
import org.dcache.util.ColumnWriter;
import org.dcache.util.Glob;
import org.dcache.util.cli.ShellApplication;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;

public class SrmShell extends ShellApplication
{
    private final FileSystem lfs = FileSystems.getDefault();
    private final SrmFileSystem fs;
    private final URI home;
    private final Map<Integer,FileTransfer> ongoingTransfers = new ConcurrentHashMap<>();
    private final Map<Integer,FileTransfer> completedTransfers = new ConcurrentHashMap<>();
    private final List<String> notifications = new ArrayList<>();

    private enum PromptType { LOCAL, SRM, SIMPLE };

    private URI pwd;
    private Path lcwd = lfs.getPath(".").toRealPath();
    private int nextTransferId = 1;
    private PromptType promptType = PromptType.SRM;
    private volatile boolean isClosed;

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
            shell.start(args);
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

        Configuration configuration = new Configuration();
        OptionParser.parseOptions(configuration, args);
        configuration.setSrmProtocolVersion(2);

        switch (uri.getScheme()) {
        case "srm":
            configuration.setSrmUrl(new java.net.URI(uri.toString()));
            break;
        case "httpg":
            configuration.setSrmUrl(new java.net.URI("srm", null,  uri.getHost(), (uri.getPort() > -1 ? uri.getPort() : -1), "/", null, null));
            configuration.setWebservice_path(uri.getPath());
            break;
        }

        X509Credential credential;
        if (configuration.isUseproxy()) {
            credential = new PEMCredential(configuration.getX509_user_proxy(), (char[]) null);
        } else {
            credential = new PEMCredential(configuration.getX509_user_key(), configuration.getX509_user_cert(), null);
        }
        fs = new AxisSrmFileSystem(
                new SRMClientV2(SrmUrl.withDefaultPort(configuration.getSrmUrl()),
                                credential,
                                configuration.getRetry_timeout(),
                                configuration.getRetry_num(),
                                configuration.isDelegate(),
                                configuration.isFull_delegation(),
                                configuration.getGss_expected_name(),
                                configuration.getWebservice_path(),
                                configuration.getX509_user_trusted_certificates(),
                                Transport.GSI));

        fs.setCredential(credential);
        fs.start();
        cd(configuration.getSrmUrl().toASCIIString());
        home = pwd;
    }

    @Override
    protected String getCommandName()
    {
        return "srmfs";
    }

    @Override
    protected String getPrompt()
    {
        StringBuilder prompt = new StringBuilder();

        synchronized (notifications) {
            if (!notifications.isEmpty()) {
                prompt.append('\n');
                for (String notification : notifications) {
                    prompt.append(notification).append('\n');
                }
                notifications.clear();
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
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Nonnull
    private URI lookup(@Nullable File path) throws URI.MalformedURIException
    {
        if (path == null) {
            return pwd;
        } else {
            return new URI(pwd, path.getPath());
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

    private void cd(String path) throws URI.MalformedURIException, RemoteException, SRMException, InterruptedException
    {
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        URI uri = new URI(pwd, path);
        if (fs.stat(uri).getType() != TFileType.DIRECTORY) {
            throw new SRMInvalidPathException("Not a directory");
        }
        TPermissionMode permission = fs.checkPermission(uri);
        if (permission != TPermissionMode.RWX && permission != TPermissionMode.RX && permission != TPermissionMode.WX && permission != TPermissionMode.X) {
            throw new SRMAuthorizationException("Access denied");
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
        public Serializable call()
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

    /*
     * Commands for local filesystem manipulation
     */

    @Command(name="lpwd", hint = "print local working directory",
                description = "Print the current working directory.  This "
                        + "directory is used as a default value for the lls "
                        + "command and when resolving local relative paths.")
    public class LpwdCommand implements Callable<Serializable>
    {
        @Override
        public Serializable call() throws Exception
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

    @Command(name="lls", hint = "list local directory",
                    description = "List a file or directory on the local "
                            + "filesystem.  The format of the "
                            + "output is controlled via various options."
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
    public class LlsCommand implements Callable<Serializable>
    {
        private static final String DEFAULT_TIME = "mtime";

        @Argument(required = false)
        String path;

        @Option(name = "time", values = { "modify", "atime", "mtime", "create" },
                usage = "Show alternative time instead of modification time: "
                        + "modify/mtime is the last write time, create is the "
                        + "creation time.")
        String time = DEFAULT_TIME;

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

        @Override
        public Serializable call() throws IOException
        {
            Path dir = path == null ? lcwd : lcwd.resolve(path);

            boolean isPattern = !Files.exists(dir) ||
                !Files.getFileAttributeView(dir, PosixFileAttributeView.class).readAttributes().isDirectory();

            final Pattern glob = isPattern ? Glob.parseGlobToPattern(dir.getFileName().toString())
                    : directory ? Pattern.compile(Pattern.quote(dir.getFileName().toString())) : Pattern.compile(".*");

            if (isPattern || directory) {
                dir = dir.getParent();
            }

            DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
                @Override
                public boolean accept(Path entry) throws IOException
                {
                    String name = entry.getFileName().toString();
                    return glob.matcher(name).matches() &&
                            (!name.startsWith(".") || showHidden);
                }
            };

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, filter)) {
                if (verbose) {
                    ColumnWriter writer = new ColumnWriter()
                            .abbreviateBytes(abbrev)
                            .left("mode")
                            .space().left("owner")
                            .space().left("group")
                            .space().bytes("size")
                            .space().date("time")
                            .space().left("name");
                    for (Path entry : stream) {
                        PosixFileAttributes attrs = Files.getFileAttributeView(entry, PosixFileAttributeView.class).readAttributes();

                        writer.row()
                                .value("mode", permissionsFor(attrs))
                                .value("owner", attrs.owner().getName())
                                .value("group", attrs.group().getName())
                                .value("size", attrs.size())
                                .value("time", new Date(getFileTime(attrs).toMillis()))
                                .value("name", entry.getFileName().toString());
                    }
                    console.print(writer.toString());
                } else {
                    List<String> names = new ArrayList<>();
                    for (Path entry : stream) {
                        names.add(entry.getFileName().toString());
                    }
                    console.printColumns(names);
                }

            }
            return null;
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

    @Command(name="lrm", hint="remove local file",
                    description = "Remove a file from the local filesystem.")
    public class LrmCommand implements Callable<Serializable>
    {
        @Argument
        String path;

        @Override
        public Serializable call() throws IOException
        {
            Path file = lcwd.resolve(path);

            if (!Files.exists(file)) {
                throw new IllegalArgumentException("No such file: " + path);
            }

            if (Files.getFileAttributeView(file, PosixFileAttributeView.class).readAttributes().isDirectory()) {
                throw new IllegalArgumentException("Is directory: " + path);
            }

            Files.delete(file);

            return null;
        }
    }

    @Command(name="lrmdir", hint="remove local directory",
                    description = "Remove a directory if it is empty.")
    public class LrmdirCommand implements Callable<Serializable>
    {
        @Argument
        String path;

        @Override
        public Serializable call() throws IOException
        {
            Path file = lcwd.resolve(path);

            if (!Files.exists(file)) {
                throw new IllegalArgumentException("No such directory: " + path);
            }

            if (!Files.getFileAttributeView(file, PosixFileAttributeView.class).readAttributes().isDirectory()) {
                throw new IllegalArgumentException("Not a directory: " + path);
            }

            try {
                Files.delete(file);
            } catch (DirectoryNotEmptyException e) {
                throw new IllegalArgumentException("Directory not empty: " + path);
            }

            return null;
        }
    }

    @Command(name="lmkdir", hint="create local directory",
            description = "Create a new subdirectory.  The parent directory "
                    + "must already exist")
    public class LmkdirCommand implements Callable<Serializable>
    {
        @Argument
        String path;

        @Override
        public Serializable call() throws IOException
        {
            Path file = lcwd.resolve(path);

            if (Files.exists(file)) {
                throw new IllegalArgumentException("Already exists: " + path);
            }

            Path parent = file.getParent();

            if (!Files.exists(parent)) {
                throw new IllegalArgumentException("Does not exist: " + parent);
            }

            if (!Files.getFileAttributeView(parent, PosixFileAttributeView.class).readAttributes().isDirectory()) {
                throw new IllegalArgumentException("Not a directory: " + parent);
            }

            Files.createDirectory(file);

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
    public class LsCommand implements Callable<Serializable>
    {
        private static final String DEFAULT_TIME = "mtime";

        private final DateFormat format = DateFormat.getDateTimeInstance();

        @Option(name = "time", values = { "modify", "mtime", "create" },
                usage = "Show alternative time instead of modification time: modify/mtime is the last write time, " +
                        "create is the creation time.")
        String time = DEFAULT_TIME;

        @Option(name = "l",
                usage = "List in long format.")
        boolean verbose;

        @Option(name = "h",
                usage = "Use abbreviated file sizes.")
        boolean abbrev;

        @Argument(required = false)
        File path;

        @Override
        public Serializable call() throws IOException, SRMException, InterruptedException
        {
            if (verbose) {
                ColumnWriter writer = new ColumnWriter()
                        .abbreviateBytes(abbrev)
                        .left("mode")
                        .space().left("owner")
                        .space().left("group")
                        .space().bytes("size")
                        .space().date("time")
                        .space().left("name");
                for (TMetaDataPathDetail entry : fs.list(lookup(path), verbose)) {
                    writer.row()
                            .value("mode", permissionsFor(entry))
                            .value("owner", entry.getOwnerPermission().getUserID())
                            .value("group", entry.getGroupPermission().getGroupID())
                            .value("size", (entry.getType() == TFileType.FILE) ? entry.getSize().longValue() : null)
                            .value("time", getTime(entry).getTime())
                            .value("name", new File(entry.getPath()).getName());
                }
                console.print(writer.toString());
            } else {
                List<String> names = new ArrayList<>();
                for (TMetaDataPathDetail entry : fs.list(lookup(path), verbose)) {
                    names.add(new File(entry.getPath()).getName());
                }
                console.printColumns(names);
            }
            return null;
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
        public String call() throws Exception
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
            writer.append("  Locality: ").println(fileLocality.getValue().toLowerCase());
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

    @Command(name = "get transfer protocols", hint = "retrieves supported transfer protocols",
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
                    + "a subdirectory on the server.  By default, the parent "
                    + "directory must already exist.  If the -p option is "
                    + "specified then missing parent directories are created "
                    + "as necessary.")
    public class MkdirCommand implements Callable<String>
    {
        @Argument
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
                    description = "Remove a directory that is empty.  If "
                            + "the -r option is specified then the removal is "
                            + "recursive, so that the command will succeed if "
                            + "the target directory and any subdirectories "
                            + "contain no files.")
    public class RmdirCommand implements Callable<String>
    {
        @Argument
        File path;

        @Option(name = "r", usage = "delete recursively")
        boolean recursive;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException
        {
            fs.rmdir(lookup(path), recursive);
            return null;
        }
    }

    @Command(name = "rm", hint = "remove directory entries",
                    description = "Remove one or more directory items.  All of "
                            + "the targets must be non-directory items.")
    public class RmCommand implements Callable<String>
    {
        @Argument
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
                return status.getSurl() + ":\n\t" + status.getStatus().getExplanation();
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

    @Command(name = "get space tokens", hint = "get space tokens matching description",
                    description = "Discover the space tokens currently "
                            + "available to this user.  If description is "
                            + "supplied then only space tokens that were "
                            + "created with this description are listed; "
                            + "otherwise all space reservations are listed.")
    public class GetSpaceTokensCommand implements Callable<String>
    {
        @Argument(required = false, usage = "The description supplied when "
                        + "creating this reservation.")
        String description;

        @Override
        public String call() throws Exception
        {
            console.printColumns(asList(fs.getSpaceTokens(description)));
            return null;
        }
    }

    @Command(name = "get permission", hint = "get permissions on SURLs",
                    description = "Query detailed information about the "
                            + "permissions of a file or directory.")
    public class GetPermissionCommand implements Callable<String>
    {
        @Argument
        File[] paths;

        @Override
        public String call() throws Exception
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
                String owner = permission.getOwner();
                if (owner != null) {
                    append(writer, prefix, "owner", permission.getOwnerPermission(), owner);
                }
                for (TUserPermission p : permission.getArrayOfUserPermissions().getUserPermissionArray()) {
                    append(writer, prefix, "user ", p.getMode(), p.getUserID());
                }
                for (TUserPermission p : permission.getArrayOfUserPermissions().getUserPermissionArray()) {
                    append(writer, prefix, "user ", p.getMode(), p.getUserID());
                }
            }
        }

        private void append(PrintWriter writer, String prefix, String type, TPermissionMode mode, String user)
        {
            writer.append(prefix).append(permissionsFor(mode)).append(' ').append(type).append(' ').println(user);
        }
    }

    @Command(name = "check permission", hint = "check client permissions on SURLs",
                    description = "Check the (effective) permissions on a file "
                            + "or directory for the current user.  The result "
                            + "is a list of operations that this user is "
                            + "allowed to do.")
    public class CheckPermissionCommand implements Callable<String>
    {
        @Argument
        File[] paths;

        @Override
        public String call() throws Exception
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
        public String call() throws Exception
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
        public String call() throws Exception
        {
            fs.releaseSpace(spaceToken);
            return null;
        }
    }

    @Command(name = "get space meta data", hint = "get information about a space reservation",
                    description = "Discover information about a specific space "
                            + "reservation.")
    public class GetSpaceMetaDataCommand implements Callable<String>
    {
        @Argument
        String spaceToken;

        @Override
        public String call() throws Exception
        {
            StringWriter out = new StringWriter();
            PrintWriter writer = new PrintWriter(out);
            TMetaDataSpace space = fs.getSpaceMetaData(spaceToken);
            append(writer, space);
            return out.toString();
        }
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
        public String call() throws Exception
        {
            URI surl = lookup(remote);
            Path target = local == null ? lcwd.resolve(remote.getName()) : lcwd.resolve(local);

            FileTransfer transfer = fs.get(surl, target.toFile());

            if (transfer == null) {
                return "No support for download.";
            }

            final int id = nextTransferId++;

            ongoingTransfers.put(id,transfer);
            Futures.addCallback(transfer, new FutureCallback() {
                @Override
                public void onSuccess(Object result)
                {
                    synchronized (notifications) {
                        notifications.add("[" + id + "] Transfer completed.");
                        FileTransfer successfulTransfer = ongoingTransfers.remove(id);
                        completedTransfers.put(id, successfulTransfer);
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    synchronized (notifications) {
                        notifications.add("[" + id + "] Transfer failed: " + t.toString());
                        FileTransfer failedTransfer = ongoingTransfers.remove(id);
                        completedTransfers.put(id, failedTransfer);
                    }
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
        public String call() throws Exception
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

            final int id = nextTransferId++;

            ongoingTransfers.put(id,transfer);
            Futures.addCallback(transfer, new FutureCallback() {
                @Override
                public void onSuccess(Object result)
                {
                    synchronized (notifications) {
                        notifications.add("[" + id + "] Transfer completed.");
                        FileTransfer successfulTransfers = ongoingTransfers.remove(id);
                        completedTransfers.put(id, successfulTransfers);
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    synchronized (notifications) {
                        notifications.add("[" + id + "] Transfer failed: " + t.toString());
                        FileTransfer failedTransfer = ongoingTransfers.remove(id);
                        completedTransfers.put(id, failedTransfer);
                    }
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
        public String call() throws Exception
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
        public String call() throws Exception
        {
            StringBuilder sb = new StringBuilder();

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
        public String call() throws Exception
        {
            FileTransfer transfer = ongoingTransfers.remove(id);
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
        public String call() throws Exception
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
        public String call() throws Exception
        {
            fs.setTransportOption(key, value);
            return "";
        }
    }
}
