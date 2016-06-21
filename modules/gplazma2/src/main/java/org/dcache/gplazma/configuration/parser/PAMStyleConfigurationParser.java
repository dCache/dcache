package org.dcache.gplazma.configuration.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.dcache.util.Strings;
import org.dcache.gplazma.configuration.Configuration;
import org.dcache.gplazma.configuration.ConfigurationItem;
import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.configuration.ConfigurationItemType;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.Files.newReader;
/**
 *
 * @author timur
 */
public class PAMStyleConfigurationParser implements ConfigurationParser {
    private static final Logger logger =
            LoggerFactory.getLogger(PAMStyleConfigurationParser.class);

    // max number of times the reg expression is applied
    // when splitting the non empty configuration file line
    // is SPLIT_LIMIT-1
    // SPLIT_LIMIT is also the max number of results of split
    private static final int SPLIT_LIMIT=4;

    //min required number of the string resulting from the split
    // <type>  <control>  <plugin>
    private static final int MIN_SPLIT_RESULTS=3;

    // any line starting with <white space> followed by COMMENT_START
    // is a comment and is ignored by the parser
    private static final String COMMENT_START="#";

    /**
     *
     * @param configuration a string containing configuration, not a file name
     * @return Configuration based on the configuration
     * @throws ParseException
     */
    @Override
    public Configuration parse(String configuration) throws ParseException {

        checkNotNull(configuration, "Configuration must not be NULL");

        return parse(new BufferedReader(new StringReader(configuration)));
    }

    /**
     *
     * @param configurationFile a file containing configuration
     * @return Configuration based on the configuration
     * @throws ParseException
     */
    @Override
    public Configuration parse(File configurationFile) throws ParseException {

        checkNotNull(configurationFile, "ConfigurationFile must not be NULL.");

        try {
            return parse(newReader(configurationFile, Charset.defaultCharset()));
        } catch(FileNotFoundException e) {
            throw new ParseException("GPlazma Configuration parsing failed",e);
        }
    }

   /**
     * @param bufferedReader, a reader of the configuration,
     * @return Configuration based on the configuration
     * @throws ParseException
     * line number reported in the exception will be correct iff the bufferedReader
     *  was initially pointing to the first char in the underlying char stream
     */
    @Override
    public Configuration parse (BufferedReader bufferedReader)
    throws ParseException {
        List<ConfigurationItem> configItemList =
                new ArrayList<>();
        int offset = 0;
        String line;
        try {
            while ( (line = bufferedReader.readLine()) != null) {
                try {
                    ConfigurationItem configItem = parseLine(line);
                    if(configItem != null) {
                        configItemList.add(configItem);
                    }

                }  catch (ParseException pe) {
                    pe.setOffset(offset);
                    throw pe;
                }
                offset ++;
            }
        } catch (IOException ioe) {
            throw new ParseException("GPlazma Configuration parsing failed",ioe,
                    offset);
        }
        return new Configuration(configItemList);
    }

    /**
     * Parses single line of configuration file
     *
     * @param line
     * @return instance of ConfigurationItem or null if line is empty or
     * contains comments
     * @throws ParseExeption if the syntax is incorrect
     */
    private ConfigurationItem parseLine(String line) throws ParseException {
        String trimmed = line.trim();

        if(trimmed.isEmpty() || trimmed.startsWith(COMMENT_START)) {
            return null;
        }

        String[] splitLine = trimmed.split("\\s+",SPLIT_LIMIT);
        logger.debug("splitLine = "+Arrays.toString(splitLine));

        if(splitLine.length == 0) {
            return null;
        }

        if(splitLine.length < MIN_SPLIT_RESULTS) {
            throw new ParseException("Syntax violation for line \""+line+'"');
        }

        try {
            // configuration line should have the following syntax
            // <type>  <control>  <plugin>  [<configuration>]
            ConfigurationItemType type =
                ConfigurationItemType.getConfigurationItemType(splitLine[0]);
            ConfigurationItemControl control =
                ConfigurationItemControl.getConfigurationItemControl(splitLine[1]);
            String name = splitLine[2];

            Properties properties = new Properties();
            if (splitLine.length>MIN_SPLIT_RESULTS) {
                String[] args = Strings.splitArgumentString(splitLine[3]);
                for (String arg : args) {
                    String[] kv = arg.split("=", 2);
                    if (kv.length==2) {
                        properties.put(kv[0].trim(), kv[1].trim());
                    }
                }
            }

            return new ConfigurationItem(type,
                    control,
                    name,
                    properties);

        } catch (IllegalArgumentException iae) {
            throw new ParseException("Syntax violation for line \""+line+'"', iae);
        }

    }

}
