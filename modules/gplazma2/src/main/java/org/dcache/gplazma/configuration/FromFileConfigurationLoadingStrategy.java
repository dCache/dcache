package org.dcache.gplazma.configuration;

import static com.google.common.base.Preconditions.checkArgument;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.dcache.gplazma.configuration.parser.ConfigurationParserFactories;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.dcache.gplazma.configuration.parser.ParseException;
import org.dcache.util.Result;
import org.dcache.util.files.LineBasedParser;
import org.dcache.util.files.LineByLineParser;
import org.dcache.util.files.ParsableFile;

/**
 * This loading strategy loads the configuration from a file.  The code to
 * parse the file's format is pluggable, allowing for custom file formats.
 * <p>
 * The {@link #hasUpdated} method returns true if the underlying file's mtime has
 * changed and the result of parsing the file has changed.
 */
public class FromFileConfigurationLoadingStrategy
        implements ConfigurationLoadingStrategy {

    private final ParsableFile configurationFile;
    private Result<Configuration,String> mostRecentResult;

    public FromFileConfigurationLoadingStrategy(String configurationFileName)
            throws FactoryConfigurationException {
        checkArgument(configurationFileName != null && !configurationFileName.isBlank(),
                  "configuration file argument wasn't specified correctly");

        Path path = FileSystems.getDefault().getPath(configurationFileName);
        checkArgument(Files.exists(path),
                  "configuration file does not exists at %s", configurationFileName);

        Supplier<LineBasedParser<Configuration>> parserFactory = ConfigurationParserFactories.getInstance();
        configurationFile = new ParsableFile(new LineByLineParser(parserFactory), path);

        mostRecentResult = configurationFile.get();
    }

    @Override
    public synchronized boolean hasUpdated() {
        var previousResult = mostRecentResult;
        mostRecentResult = configurationFile.get();
        return !mostRecentResult.equals(previousResult);
    }

    @Override
    public synchronized Configuration load() throws ParseException {
        mostRecentResult = configurationFile.get();
        return mostRecentResult.orElseThrow(ParseException::new);
    }
}
