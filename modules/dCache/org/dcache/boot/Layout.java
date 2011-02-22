package org.dcache.boot;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
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

import org.dcache.util.ConfigurationProperties;
import org.dcache.util.ConfigurationProperties.ProblemConsumer;
import org.dcache.util.NetworkUtils;
import org.dcache.commons.util.Strings;

import diskCacheV111.util.FsPath;

import static org.dcache.boot.Properties.*;

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
        new LinkedHashMap<String,Domain>();
    private String _source = "<unknown>";

    public Layout(ConfigurationProperties config)
    {
        _properties = new ConfigurationProperties(config);
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
                            Strings.join(_domains.keySet(), " "));
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
        Reader reader = new InputStreamReader(url.openStream());
        try {
            load(reader);
        } finally {
            reader.close();
        }
    }

    /**
     * Reads a layout definition from the input character stream.
     *
     * @param reader the input character stream.
     */
    public void load(Reader in)
        throws IOException
    {
        LineNumberReader reader = new LineNumberReader(in);
        loadSection(reader, _properties);

        String s;
        while ((s = reader.readLine()) != null) {
            Matcher matcher = SECTION_HEADER.matcher(s);
            if (!matcher.matches()) {
                throw new RuntimeException("Bug detected: Section header expected: " + s);
            }

            String domainName =
                _properties.replaceKeywords(matcher.group(1));
            String serviceName =
                matcher.group(3);

            if (serviceName == null) {
                Domain domain = createDomain(domainName);
                loadSection(reader, domain.properties());
            } else {
                Domain domain = getDomain(domainName);
                if (domain == null) {
                    String message = String.format("Service declaration " +
                            "%s/%s lacks definition of domain %s",
                            domainName, serviceName, domainName);
                    discardSection(reader, message);
                } else {
                    loadSection(reader, domain.createService(serviceName));
                }
            }
        }
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

    /**
     * Reads properties until the next section header. The position is
     * advanced until the next section header or the end of file.
     *
     * @param reader The reader to read from
     * @param config The Properties to which to add the properties
     */
    private void loadSection(LineNumberReader reader, ConfigurationProperties config)
        throws IOException
    {
        int linesRead = reader.getLineNumber();
        String section = loadSection(reader);
        config.load(_source, linesRead, new StringReader(section));
    }

    /**
     * Reads properties until the next section header. The position is
     * advanced until the next section header or the end of file.
     *
     * @param reader The reader to read from
     * @return Property declarations.
     */
    private String loadSection(BufferedReader reader) throws IOException
    {
        String line;
        StringBuilder section = new StringBuilder();
        reader.mark(READ_AHEAD_LIMIT);
        while ( (line = reader.readLine()) != null &&
                !SECTION_HEADER.matcher(line).matches()) {
            section.append(line).append('\n');
            reader.mark(READ_AHEAD_LIMIT);
        }
        reader.reset();
        return section.toString();
    }

    /**
     * Prints the list of domains in the layout.
     */
    public void printDomainNames(PrintStream out)
    {
        for (Domain domain: _domains.values()) {
            out.println(domain.getName());
        }
    }

    /**
     * Prints the domains in the layout matching one of the
     * patterns.
     */
    public void printMatchingDomainNames(PrintStream out, Collection<Pattern> patterns)
    {
        for (Domain domain: _domains.values()) {
            for (Pattern pattern: patterns) {
                String name = domain.getName();
                if (pattern.matcher(name).matches()) {
                    out.println(name);
                    break;
                }
            }
        }
    }
}
