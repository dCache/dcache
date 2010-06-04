/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.gplazma.configuration.parser;

import java.io.BufferedReader;
import java.io.File;
import org.dcache.gplazma.configuration.Configuration;

/**
 * This is the parser that will support parsing classical
 * gPlazma policy file and provide a Configuration sutable for the
 * new gPlazma 2.0 strategy consumption
 * @author timur
 */
public class ClassicConfigurationParser implements ConfigurationParser {

    public Configuration parse(String configuration) throws ParseException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Configuration parse(File configurationFile) throws ParseException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Configuration parse(BufferedReader bufferedReader) throws ParseException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
