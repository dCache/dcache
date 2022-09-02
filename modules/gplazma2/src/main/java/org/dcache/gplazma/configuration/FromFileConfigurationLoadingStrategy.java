package org.dcache.gplazma.configuration;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.dcache.gplazma.configuration.parser.ConfigurationParserFactories;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.dcache.gplazma.configuration.parser.ParseException;
import org.dcache.util.files.LineBasedParser;
import org.dcache.util.files.LineBasedParser.UnrecoverableParsingException;

/**
 * This loading strategy loads the configuration from file, if file has been updated. This class is
 * not thread safe.
 *
 * @author timur
 */
public class FromFileConfigurationLoadingStrategy
      implements ConfigurationLoadingStrategy {

    private static final long CONFIGURATION_UPDATE_FREQUENCY_MILLIS =
          TimeUnit.SECONDS.toMillis(1);

    private final Supplier<LineBasedParser<Configuration>> parserFactory;
    private final File configurationFile;
    private long configurationFileLastModified;
    private long configurationFileLastChecked;

    public FromFileConfigurationLoadingStrategy(String configurationFileName)
            throws FactoryConfigurationException {
        checkArgument(configurationFileName != null && !configurationFileName.isBlank(),
                  "configuration file argument wasn't specified correctly");

        configurationFile = new File(configurationFileName);

        checkArgument(configurationFile.exists(),
                  "configuration file does not exists at %s", configurationFileName);
        parserFactory = ConfigurationParserFactories.getInstance();
    }

    /**
     * @return true if the configuration file has been modified, false otherwise
     */
    @Override
    public boolean hasUpdated() {
        if ((System.currentTimeMillis() - configurationFileLastChecked) <
              CONFIGURATION_UPDATE_FREQUENCY_MILLIS) {
            /* we checked less then CONFIGURATION_UPDATE_FREQUNCY_MILLIS
             * milliseconds ago */
            return false;
        }
        if (configurationFileLastModified == configurationFile.lastModified()) {
            //configuration file has not been updated. no need to reread
            return false;
        }
        return true;
    }

    /**
     * @return configuration loaded from the configuration file
     */
    @Override
    public Configuration load() throws ParseException {
        configurationFileLastModified = configurationFile.lastModified();
        configurationFileLastChecked = System.currentTimeMillis();

        try {
            var parser = parserFactory.get();
            for (String line : Files.readAllLines(configurationFile.toPath(),
                    Charset.defaultCharset())) {
                parser.accept(line);
            }
            return parser.build();
        } catch (UnrecoverableParsingException e) {
            throw new ParseException(e.getMessage(), e);
        } catch (IOException e) {
            throw new ParseException("GPlazma Configuration parsing failed", e);
        }
    }
}
