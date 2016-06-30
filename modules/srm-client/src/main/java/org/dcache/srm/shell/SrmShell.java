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
import com.google.common.collect.Maps;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.PEMCredential;
import gov.fnal.srm.util.Configuration;
import gov.fnal.srm.util.OptionParser;
import org.apache.axis.types.URI;
import org.apache.axis.types.UnsignedLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

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
import org.dcache.util.cli.ShellApplication;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;

public class SrmShell extends ShellApplication
{
    private final SrmFileSystem fs;
    private final URI home;
    private URI pwd;

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
            shell.start(args);
        } catch (MalformedURLException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
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
        String uri = pwd.toString();
        if (pwd.getPath().length() > 1) {
            uri = uri.substring(0, uri.length()-1);
        }
        return uri + " # ";
    }

    @Override
    public void close() throws IOException
    {
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

    private void cd(String path) throws URI.MalformedURIException, RemoteException, SRMException
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

    @Command(name = "cd", hint = "change current directory")
    public class CdCommand implements Callable<Serializable>
    {
        @Argument(required = false)
        String path;

        @Override
        public Serializable call() throws URI.MalformedURIException, RemoteException, SRMException
        {
            if (path == null) {
                pwd = home;
            } else {
                cd(path);
            }
            return null;
        }
    }

    @Command(name = "ls", hint = "list directory contents")
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

    @Command(name = "stat", hint = "display file status")
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

    @Command(name = "ping", hint = "ping server")
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

    @Command(name = "get transfer protocols", hint = "retrieves supported transfer protocols")
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

    @Command(name = "mkdir", hint = "make directory")
    public class MkdirCommand implements Callable<String>
    {
        @Argument
        File path;

        @Option(name = "p", usage = "no error if existing, make parent directories as needed")
        boolean parent;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException
        {
            if (parent) {
                recursiveMkdir(path);
            } else {
                fs.mkdir(lookup(path));
            }
            return null;
        }

        private void recursiveMkdir(File path) throws RemoteException, URI.MalformedURIException, SRMException
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

    @Command(name = "rmdir", hint = "remove empty directories")
    public class RmdirCommand implements Callable<String>
    {
        @Argument
        File path;

        @Option(name = "r", usage = "")
        boolean recursive;

        @Override
        public String call() throws RemoteException, URI.MalformedURIException, SRMException
        {
            fs.rmdir(lookup(path), recursive);
            return null;
        }
    }

    @Command(name = "rm", hint = "remove directory entries")
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

    @Command(name = "mv", hint = "move (rename) file or directory")
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

    @Command(name = "get space tokens", hint = "get space tokens matching description")
    public class GetSpaceTokensCommand implements Callable<String>
    {
        @Argument(required = false)
        String description;

        @Override
        public String call() throws Exception
        {
            console.printColumns(asList(fs.getSpaceTokens(description)));
            return null;
        }
    }

    @Command(name = "get permission", hint = "get permissions on SURLs")
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

    @Command(name = "check permission", hint = "check client permissions on SURLs")
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

    @Command(name = "reserve space", hint = "create space reservation")
    public class ReserveSpaceCommand implements Callable<String>
    {
        @Option(name = "al", required = false,
                values = { "NEARLINE", "ONLINE" })
        String al;

        @Option(name = "rp", required = true,
                values = { "REPLICA", "OUTPUT", "CUSTODIAL" })
        String rp;

        @Option(name = "lifetime")
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

    @Command(name = "release space", hint = "release space reservation")
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

    @Command(name = "get space meta data", hint = "get information about a space reservation")
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
}
