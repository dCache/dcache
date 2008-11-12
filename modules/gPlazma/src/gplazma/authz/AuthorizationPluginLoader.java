package gplazma.authz;

import gplazma.authz.plugins.AuthorizationPlugin;
import gplazma.authz.plugins.LoggingPlugin;
import gplazma.authz.plugins.vorolemap.VORoleMapAuthzPlugin;
import gplazma.authz.plugins.gridmapfile.GridMapFileAuthzPlugin;
import gplazma.authz.plugins.samlquery.XACMLAuthorizationPlugin;
import gplazma.authz.plugins.samlquery.SAML1AuthorizationPlugin;

import java.util.*;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;

import org.apache.log4j.*;

public class AuthorizationPluginLoader {

    static Logger log = Logger.getLogger(AuthorizationPluginLoader.class.getSimpleName());

    private AuthorizationConfig authConfig;
    private Vector pluginPriorityConfig;
    private long authRequestID;

        private List l = null;

    static final String external_plugin = "diskCacheV111.services.authorization.KPWDAuthorizationPlugin";
    static final Map<String, Class> plugins = new HashMap<String, Class>();

    static {
        try {
          Class ExternalAuthorizationPlugin = Class.forName(external_plugin);
          Constructor plugin_constructor;
          try { plugin_constructor = ExternalAuthorizationPlugin.getConstructor(String.class, long.class);
          } catch (NoSuchMethodException nsme) {
            log.warn("No constructor with int parameter for ExternalAuthorizationPlugin " + external_plugin);
          }
          plugins.put(external_plugin, ExternalAuthorizationPlugin);
        } catch (ClassNotFoundException cnfe) {
          log.warn("ClassNotFoundException for ExternalAuthorizationPlugin " + external_plugin);
        }
    }

    public AuthorizationPluginLoader(AuthorizationConfig authConfig, long authRequestID) {
        this.authConfig = authConfig;
        this.authRequestID=authRequestID;
    }

    public void setLogLevel	(Level level) {
        log.setLevel(level);
    }

    private void buildDCacheAuthzPolicy()
            throws AuthorizationException {

        String kpwdPath;
        String XACMLMapUrl;
        String VOMapUrl;
        String gridmapfilePath;
        String storageAuthzDbPath;
        String gPLAZMALiteVORoleMapPath;
        String gPLAZMALiteStorageAuthzDbPath;

        if(authConfig==null) return;

        try {
            pluginPriorityConfig = authConfig.getpluginPriorityConfig();
            ListIterator iter = pluginPriorityConfig.listIterator(0);
            while (iter.hasNext()) {
                String thisSignal = (String)iter.next();
                if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getXACMLMappingSignal())) ) {
                    try {
                        try {
                            XACMLMapUrl = authConfig.getXACMLMappingServiceUrl();
                        } catch(Exception e) {
                            log.error("Exception getting XACML Map Url from configuration : " +e);
                            throw new AuthorizationException(e.toString());
                        }
                        if (XACMLMapUrl != null && !XACMLMapUrl.equals("")) {
                            gPLAZMALiteStorageAuthzDbPath = authConfig.getGridVORoleStorageAuthzPath();
                            AuthorizationPlugin XACMLPlug = new XACMLAuthorizationPlugin(XACMLMapUrl, gPLAZMALiteStorageAuthzDbPath, authRequestID);
                            ((XACMLAuthorizationPlugin) XACMLPlug).setCacheLifetime(authConfig.getXACMLMappingServiceCacheLifetime());
                            addPlugin(XACMLPlug);
                        } else {
                            log.error("VO Map Url not well-formed in configuration.");
                        }
                    } catch (AuthorizationException ae) {
                        log.error("Exception : " +ae);
                    }
                }//end of xacml-based-vo-mapping-if
                else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getVOMappingSignal())) ) {
                    try {
                        try {
                            VOMapUrl = authConfig.getVOMappingServiceUrl();
                        } catch(Exception e) {
                            log.error("Exception getting VO Map Url from configuration : " +e);
                            throw new AuthorizationException(e.toString());
                        }
                        if (VOMapUrl != null && !VOMapUrl.equals("")) {
                            gPLAZMALiteStorageAuthzDbPath = authConfig.getGridVORoleStorageAuthzPath();
                            AuthorizationPlugin VOPlug = new SAML1AuthorizationPlugin(VOMapUrl, gPLAZMALiteStorageAuthzDbPath, authRequestID);
                            ((SAML1AuthorizationPlugin) VOPlug).setCacheLifetime(authConfig.getVOMappingServiceCacheLifetime());
                            addPlugin(VOPlug);
                        } else {
                            log.error("VO Map Url not well-formed in configuration.");
                        }
                    } catch (AuthorizationException ae) {
                        log.error("Exception : " +ae);
                    }
                }//end of saml-based-vo-mapping-if
                else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getGPLiteVORoleMappingSignal())) ) {
                    try {
                        try {
                            gPLAZMALiteVORoleMapPath = authConfig.getGridVORoleMapPath();
                            gPLAZMALiteStorageAuthzDbPath = authConfig.getGridVORoleStorageAuthzPath();
                        } catch(Exception e) {
                            log.error("Exception getting Grid VO Role Map or Storage Authzdb paths from configuration :" +e);
                            throw new AuthorizationException(e.toString());
                        }
                        if (gPLAZMALiteVORoleMapPath != null && gPLAZMALiteStorageAuthzDbPath != null &&
                                !gPLAZMALiteVORoleMapPath.equals("") && !gPLAZMALiteStorageAuthzDbPath.equals("")) {
                            AuthorizationPlugin liteVORolePlug = new VORoleMapAuthzPlugin(gPLAZMALiteVORoleMapPath, gPLAZMALiteStorageAuthzDbPath, authRequestID);
                            addPlugin(liteVORolePlug);
                        } else {
                            log.error("Grid VO Role Map or Storage Authzdb paths not well-formed in configuration");
                        }
                    } catch (AuthorizationException ae) {
                        log.error("Exception : " +ae);
                    }
                }//end of authz-vorole-mapping-if
                else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getGridMapFileSignal())) ) {
                    try {
                        try {
                            gridmapfilePath = authConfig.getGridMapFilePath();
                            storageAuthzDbPath = authConfig.getStorageAuthzPath();
                        } catch(Exception e) {
                            log.error("Exception getting GridMap or Storage Authzdb path from configuration :" +e);
                            throw new AuthorizationException(e.toString());
                        }
                        if (gridmapfilePath != null && storageAuthzDbPath != null &&
                                !gridmapfilePath.equals("") && !storageAuthzDbPath.equals("")) {
                            AuthorizationPlugin gPlug  = new GridMapFileAuthzPlugin(gridmapfilePath, storageAuthzDbPath, authRequestID);
                            addPlugin(gPlug);
                        } else {
                            log.error("GridMap or Storage Authzdb paths not well-formed in configuration");
                        }
                    } catch (AuthorizationException ae) {
                        log.error("Exception : " +ae);
                    }
                }//end of authz-gridmapfile-if
                else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getKpwdSignal())) ) {
                    // Look for external plugins
                    for ( String plugkey  : plugins.keySet()) {
                        Class ExternalAuthorizationPlugin = plugins.get(plugkey);
                        Constructor plugin_constructor = ExternalAuthorizationPlugin.getConstructor(String.class, long.class);
                        AuthorizationPlugin plugin = (AuthorizationPlugin) plugin_constructor.newInstance(authConfig.getAuthConfigFileName(), authRequestID);
                        addPlugin(plugin);
                    } //end of kpwd external plugin
                }
            }//end of while
        } catch(Exception cpe) {
            log.error("Exception processing Choice|Priority Configuration :" + cpe);
            throw new AuthorizationException(cpe.toString());
        }

    }

    public Iterator getPlugins() throws AuthorizationException {

        if(l==null) {
            l = new LinkedList();

            try {
                buildDCacheAuthzPolicy();
            }	catch(AuthorizationException aue) {
                log.error("Exception in building DCache Authz Policy: " + aue);
                throw new AuthorizationException(aue.toString());
            }

            if (l.size() == 0) {
                log.warn("All Authorization OFF!  System Quasi-firewalled!");
            }
        }

        return l.listIterator(0);
    }

    private void addPlugin(AuthorizationPlugin plugin)
    throws AuthorizationException {
        try {
            if(plugin == null) {
                log.error("Plugin is null and cannot be added.");
            } else {
                //forwardLogLevel(plugin);
                if(plugin instanceof LoggingPlugin) ((LoggingPlugin)plugin).setLogLevel(log.getLevel());
                l.add(plugin);
            }
        } catch (Exception e ) {
            throw new AuthorizationException("authRequestID " + authRequestID + " Exception adding Plugin: " +e);
        }
    }

}
