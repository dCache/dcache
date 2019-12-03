/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.dcache.auth.IGTFPolicyPrincipal;
import org.dcache.auth.IGTFStatusPrincipal;
import org.dcache.auth.LoA;
import org.dcache.auth.LoAPrincipal;
import org.dcache.gplazma.util.IGTFInfo.Status;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.gplazma.util.IGTFInfo.Type.POLICY;
import static org.dcache.gplazma.util.IGTFInfo.Type.TRUST_ANCHOR;

/**
 * Represents all *.info files in a directory.
 */
public class IGTFInfoDirectory
{
    private static final Logger LOG = LoggerFactory.getLogger(IGTFInfoDirectory.class);
    private static final int MIN_STAT_DURATION = 2_000;
    private static final Map<Status,String> TO_NAME = ImmutableMap.<Status,String>builder()
            .put(Status.ACCREDITED_CLASSIC, "classic")
            .put(Status.ACCREDITED_IOTA, "iota")
            .put(Status.ACCREDITED_MICS, "mics")
            .put(Status.ACCREDITED_SLCS, "slcs")
            .put(Status.DISCONTINUED, "discontinued")
            .put(Status.EXPERIMENTAL, "experimental")
            .put(Status.UNACCREDITED, "unaccredited")
            .build();

    private static final Map<Status,LoA> TO_LOA = ImmutableMap.<Status,LoA>builder()
            .put(Status.ACCREDITED_CLASSIC, LoA.IGTF_AP_CLASSIC)
            .put(Status.ACCREDITED_IOTA, LoA.IGTF_AP_IOTA)
            .put(Status.ACCREDITED_MICS, LoA.IGTF_AP_MICS)
            .put(Status.ACCREDITED_SLCS, LoA.IGTF_AP_SLCS)
            .put(Status.EXPERIMENTAL, LoA.IGTF_AP_EXPERIMENTAL)
            .build();

    private final Path directory;
    private final Map<Path,IGTFInfoFile> files = new HashMap<>();
    private final Map<GlobusPrincipal,IGTFStatusPrincipal> taStatus = new HashMap<>();
    private final SetMultimap<GlobusPrincipal,IGTFPolicyPrincipal> taPolicies = HashMultimap.create();

    private long lastStat;
    private FileTime lastScanned;

    public IGTFInfoDirectory(String directory)
    {
        this(Paths.get(directory));
    }

    public IGTFInfoDirectory(Path directory)
    {
        checkArgument(Files.isDirectory(directory));
        this.directory = directory;
    }

    public Set<Principal> getPrincipals(GlobusPrincipal certificateAuthority)
    {
        Set<Principal> principals = new HashSet<>();

        try {
            verifyUptoDate();
        } catch (IOException e) {
            LOG.warn("Problem scanning directory {}: {}", directory, e.toString());
        }

        IGTFStatusPrincipal status = taStatus.get(certificateAuthority);
        if (status != null) {
            principals.add(status);
            status.getLoA()
                    .map(LoAPrincipal::new)
                    .ifPresent(principals::add);
        }

        principals.addAll(taPolicies.get(certificateAuthority));

        return principals;
    }

    private synchronized void verifyUptoDate() throws IOException
    {
        if (System.currentTimeMillis() - lastStat > MIN_STAT_DURATION) {
            lastStat = System.currentTimeMillis();

            FileTime mtime = Files.getLastModifiedTime(directory);
            if (lastScanned == null || mtime.compareTo(lastScanned) > 0) {
                lastScanned = mtime;
                updateDirectory();
            }
        }
    }

    public void updateDirectory() throws IOException
    {
        List<Path> contents = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.info")) {
            stream.forEach(contents::add);
        }

        files.keySet().retainAll(contents);
        contents.removeAll(files.keySet());
        contents.forEach(p -> files.put(p, new IGTFInfoFile(p)));

        taStatus.clear();
        taPolicies.clear();
        files.values().stream()
                .map(IGTFInfoFile::get)
                .flatMap(o -> o.isPresent() ? Stream.of(o.get()) : Stream.empty())
                .forEach(this::addPrincipals);
    }

    private void addPrincipals(IGTFInfo info)
    {
        switch (info.getType()) {
        case TRUST_ANCHOR:
            Status s = info.getStatus();
            IGTFStatusPrincipal st = new IGTFStatusPrincipal(TO_NAME.get(s),
                    s.isAccredited(), Optional.ofNullable(TO_LOA.get(s)));
            taStatus.put(info.getSubjectDN(), st);
            break;
        case POLICY:
            IGTFPolicyPrincipal policy = new IGTFPolicyPrincipal(info.getName());
            info.getSubjectDNs().forEach(dn -> taPolicies.put(dn, policy));
            break;
        }
    }
}
