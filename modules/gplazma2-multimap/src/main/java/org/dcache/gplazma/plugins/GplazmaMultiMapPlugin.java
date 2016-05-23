package org.dcache.gplazma.plugins;

import com.google.common.base.Strings;

import org.dcache.gplazma.AuthenticationException;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

public class GplazmaMultiMapPlugin implements GPlazmaMappingPlugin
{
    private GplazmaMultiMapFile mapFile;
    private static final String GPLAZMA2_MAP_FILE = "gplazma.multimap.file";

    public GplazmaMultiMapPlugin(Properties properties)
    {
        String path = properties.getProperty(GPLAZMA2_MAP_FILE);
        checkArgument(!Strings.isNullOrEmpty(path), "Undefined property: " + GPLAZMA2_MAP_FILE);
        mapFile = new GplazmaMultiMapFile(path);
    }

    public GplazmaMultiMapPlugin(GplazmaMultiMapFile mapFile)
    {
        this.mapFile = checkNotNull(mapFile, "Multi-mapfile can't be null");
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException
    {
        mapFile.ensureUpToDate();
        Set<Principal> mappedPrincipals = principals.stream()
                                                    .flatMap(p -> mapFile.getMappedPrincipals(p).stream())
                                                    .collect(Collectors.toSet());

        checkAuthentication(!mappedPrincipals.isEmpty(), "no mappable principals");
        principals.addAll(mappedPrincipals);
    }
}
