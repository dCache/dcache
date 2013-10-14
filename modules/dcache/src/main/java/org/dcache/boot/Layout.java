package org.dcache.boot;

import com.google.common.base.Joiner;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.util.FsPath;

import org.dcache.util.ConfigurationProperties;
import org.dcache.util.ConfigurationProperties.ProblemConsumer;
import org.dcache.util.NetworkUtils;

import static org.dcache.boot.Properties.PROPERTY_DOMAINS;
import static org.dcache.boot.Properties.PROPERTY_DOMAIN_CELLS;

/**
 * Layout encapsulates the configuration of a set of domains.
 */
public class Layout
{
    private static final int READ_AHEAD_LIMIT = 256;

    private static final Pattern SECTION_HEADER =
        Pattern.compile("^\\s*\\[([^\\]/]+)(/([^\\]/]+))?\\]\\s*$");

    private final ConfigurationProperties _properties;
    private final Map<String,Domain> _domains =
        new LinkedHashMap<>();
    private String _source = "<unknown>";

    public Layout(ConfigurationProperties config)
    {
        _properties = new ConfigurationProperties(config, new DcacheConfigurationUsageChecker());
    }

    public ConfigurationProperties properties()
    {
        return _properties;
    }

    public Collection<Domain> getDomains()
    {
        return Collections.unmodifiableCollection(_domains.values());
    }

    public Domain getDomain(String name)
    {
        return _domains.get(name);
    }

    public Domain createDomain(String name)
    {
        Domain domain = _domains.get(name);
        if (domain == null) {
            domain = new Domain(name, _properties);
            _domains.put(name, domain);
            _properties.put(PROPERTY_DOMAINS,
                            Joiner.on(" ").join(_domains.keySet()));
        }
        return domain;
    }

    /**
     * Reads a layout definition from the URI.
     *
     * @param uri The URI of the layout definition.
     */
    public void load(URI uri)
        throws URISyntaxException, IOException
    {
        URL url = NetworkUtils.toURL(uri);
        _source = new FsPath(url.getPath()).getName();
        try (Reader reader = new InputStreamReader(url.openStream())) {
            load(reader);
        }

    }

    /**
     * Reads a layout definition from the input character stream.
     *
     * @param in the input character stream.
     */
    public void load(Reader in)
        throws IOException
    {
        LineNumberReader reader = new LineNumberReader(in);
        _properties.load(_source, loadSection(reader));

        String s;
        while ((s = reader.readLine()) != null) {
            Matcher matcher = SECTION_HEADER.matcher(s);
            if (!matcher.matches()) {
                throw new RuntimeException("Bug detected: Section header expected: " + s);
            }

            String domainName =
                _properties.replaceKeywords(matcher.group(1));
            String serviceType =
                matcher.group(3);

            if (serviceType == null) {
                Domain domain = createDomain(domainName);
                domain.properties().load(_source, loadSection(reader));
            } else {
                Domain domain = getDomain(domainName);
                if (domain == null) {
                    String message = String.format("Service declaration " +
                            "%s/%s lacks definition of domain %s",
                            domainName, serviceType, domainName);
                    discardSection(reader, message);
                } else {
                    LineNumberReader sectionReader = loadSection(reader);
                    domain.createService(_source, sectionReader, serviceType);
                }
            }
        }

        // Cannot do this until all services have been defined
        for (Domain domain : _domains.values()) {
            domain.properties().put(PROPERTY_DOMAIN_CELLS, Joiner.on(" ").join(domain.getCellNames()));
        }
    }

    /**
     * Reads properties until the next section header, returning the result as a
     * reader. The position is advanced until the next section header or the end
     * of file.
     *
     * @param reader The reader to read from
     * @return LineNumberReader for the section
     */
    private LineNumberReader loadSection(LineNumberReader reader) throws IOException
    {
        int lineNumber = reader.getLineNumber();
        StringBuilder section = new StringBuilder();
        reader.mark(READ_AHEAD_LIMIT);
        String line;
        while ( (line = reader.readLine()) != null &&
                !SECTION_HEADER.matcher(line).matches()) {
            section.append(line).append('\n');
            reader.mark(READ_AHEAD_LIMIT);
        }
        reader.reset();
        LineNumberReader sectionReader = new LineNumberReader(new StringReader(section.toString()));
        sectionReader.setLineNumber(lineNumber);
        return sectionReader;
    }

    private void discardSection(LineNumberReader reader, String message)
        throws IOException
    {
        ProblemConsumer consumer = _properties.getProblemConsumer();
        consumer.setFilename(_source);
        consumer.setLineNumberReader(reader);
        consumer.error(message);
        loadSection(reader); // discard any configuration for this section
    }

}
