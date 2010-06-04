/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.gplazma.configuration.parser;

/**
 *
 * @author timur
 */
public class FactoryConfigurationError  extends Error{

    public FactoryConfigurationError(String msg) {
        super(msg);
    }

    public FactoryConfigurationError(String msg, Throwable cause) {
        super(msg, cause);
    }


}
