package org.dcache.gplazma.configuration.parser;

import java.io.BufferedReader;
import java.io.File;
import org.dcache.gplazma.configuration.Configuration;

/**
 *
 * @author timur
 */
public interface ConfigurationParser {

    /**
     *
     * @param configuration a string containing configuration, not a file name
     * @return Configuration based on the configuration
     * @throws ParseException
     */
    Configuration parse(String configuration) throws ParseException;

    /**
     *
     * @param configurationFile a file containing configuration
     * @return Configuration based on the configuration
     * @throws ParseException
     */
    Configuration parse(File configurationFile) throws ParseException;

    /**
     * @param bufferedReader, a reader of the configuration,
     * @return Configuration based on the configuration
     * @throws ParseException
     * line number reported in the exception will be correct iff the bufferedReader
     * was initially pointing to the first char in the underlying char stream
     */
    Configuration parse(BufferedReader bufferedReader) throws ParseException;

}
