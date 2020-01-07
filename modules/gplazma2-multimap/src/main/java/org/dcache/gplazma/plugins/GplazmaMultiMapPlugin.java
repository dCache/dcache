package org.dcache.gplazma.plugins;

import com.google.common.base.Strings;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;

import org.dcache.gplazma.AuthenticationException;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.dcache.auth.GidPrincipal;
import org.dcache.gplazma.plugins.GplazmaMultiMapFile.PrincipalMatcher;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

public class GplazmaMultiMapPlugin implements GPlazmaMappingPlugin
{
    private final GplazmaMultiMapFile mapFile;
    private static final String GPLAZMA2_MAP_FILE = "gplazma.multimap.file";

    public GplazmaMultiMapPlugin(Properties properties)
    {
        this(FileSystems.getDefault(), properties);
    }

    public GplazmaMultiMapPlugin(FileSystem fs, Properties properties)
    {
        String path = properties.getProperty(GPLAZMA2_MAP_FILE);
        checkArgument(!Strings.isNullOrEmpty(path), "Undefined property: " + GPLAZMA2_MAP_FILE);
        mapFile = new GplazmaMultiMapFile(fs.getPath(path));
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException
    {
        Map<PrincipalMatcher,Set<Principal>> mapping = mapFile.mapping();

        // List of mapped principals in file order.
        List<Principal> mappedPrincipals = mapping.entrySet().stream()
                                                    .filter(e -> principals.stream().anyMatch(e.getKey()::matches))
                                                    .map(Map.Entry::getValue)
                                                    .flatMap(Set::stream)
                                                    .collect(Collectors.toList());

        checkAuthentication(!mappedPrincipals.isEmpty(), "no mappable principals");

        if (principals.stream().anyMatch(GidPrincipal::isPrimaryGid)) {
            if (mappedPrincipals.stream().anyMatch(GidPrincipal::isPrimaryGid)) {
                mappedPrincipals = mappedPrincipals.stream()
                        .map(p -> p instanceof GidPrincipal
                                ? ((GidPrincipal)p).withPrimaryGroup(false)
                                : p)
                        .collect(Collectors.toList());
            }
        } else {
            long primaryGidCount = mappedPrincipals.stream().filter(GidPrincipal::isPrimaryGid).limit(2).count();
            if (primaryGidCount > 1) {
                List<Principal> singlePrimaryGid = new ArrayList<>(mappedPrincipals.size());
                boolean isFirstPrimaryGid = true;
                for (Principal p : mappedPrincipals) {
                    if (GidPrincipal.isPrimaryGid(p)) {
                        if (isFirstPrimaryGid) {
                            isFirstPrimaryGid = false;
                        } else {
                            p = ((GidPrincipal)p).withPrimaryGroup(false);
                        }
                    }
                    singlePrimaryGid.add(p);
                }
                mappedPrincipals = singlePrimaryGid;
            }
        }

        principals.addAll(mappedPrincipals);
    }
}
