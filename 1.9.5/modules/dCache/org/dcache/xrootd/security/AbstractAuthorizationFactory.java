package org.dcache.xrootd.security;

import java.security.GeneralSecurityException;
import java.util.Map;


public abstract class AbstractAuthorizationFactory {

    /**
     * return the names of all options required by the concrete factory
     * @return the names of all options
     */
    public abstract String[] getRequiredOptions();



    /**
     * Initializes the concrete factory. After this method was called, instances of the product
     * (concrete Authorization handler) can be fetched.
     * @param options a hashtable containing all options (String:key, String:value)
     * @throws GeneralSecurityException if for some reason the factory can not be initialized
     */
    public abstract void initialize(Map options) throws GeneralSecurityException;


    /**
     * Produces a concrete authorization handler instance.
     * @return the new authz handler instance
     */
    public abstract AuthorizationHandler getAuthzHandler();


    /**
     * Instantiates the concrete factory specified by name
     * @param name the full classified classname of the concrete factory
     * @return the instance of the concrete factory
     * @throws ClassNotFoundException if the factory class was not found
     */
    public static AbstractAuthorizationFactory getFactory(String name) throws ClassNotFoundException {

        AbstractAuthorizationFactory result = null;

        try {
            result = (AbstractAuthorizationFactory) Class.forName(name).newInstance();
        } catch (Exception e) {
            throw new ClassNotFoundException(e.getMessage());
        }
        return result;
    }
}
