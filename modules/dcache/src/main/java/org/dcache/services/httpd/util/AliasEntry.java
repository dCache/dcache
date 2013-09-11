package org.dcache.services.httpd.util;

import org.eclipse.jetty.plus.jndi.EnvEntry;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Properties;

import dmg.cells.nucleus.CellInfoProvider;
import dmg.util.Args;

import org.dcache.services.httpd.HttpServiceCell;
import org.dcache.services.httpd.handlers.BadConfigHandler;
import org.dcache.services.httpd.handlers.ContextHandler;
import org.dcache.services.httpd.handlers.PathHandler;
import org.dcache.services.httpd.handlers.RedirectHandler;
import org.dcache.services.httpd.handlers.ResponseEngineHandler;

/**
 * Abstraction for httpd aliases. Integrates webapp type.
 *
 * @author arossi
 */
public class AliasEntry {

    enum AliasType {
        FILE, DIR, CONTEXT, REDIRECT, ENGINE, WEBAPP, BADCONFIG;

        public static AliasType fromType(String type) throws Exception {
            if (TYPE_FILE.equalsIgnoreCase(type)) {
                return FILE;
            } else if (TYPE_DIRECTORY.equalsIgnoreCase(type)) {
                return DIR;
            } else if (TYPE_CONTEXT.equalsIgnoreCase(type)) {
                return CONTEXT;
            } else if (TYPE_REDIRECT.equalsIgnoreCase(type)) {
                return REDIRECT;
            } else if (TYPE_CLASS.equalsIgnoreCase(type)) {
                return ENGINE;
            } else if (TYPE_WEBAPP.equalsIgnoreCase(type)) {
                return WEBAPP;
            } else if (TYPE_BAD_CONFIG.equalsIgnoreCase(type)) {
                return BADCONFIG;
            }
            throw new Exception("unknown Alias type: " + type);
        }

        public String toType() {
            switch (this) {
                case FILE:
                    return TYPE_FILE;
                case DIR:
                    return TYPE_DIRECTORY;
                case CONTEXT:
                    return TYPE_CONTEXT;
                case REDIRECT:
                    return TYPE_REDIRECT;
                case ENGINE:
                    return TYPE_CLASS;
                case WEBAPP:
                    return TYPE_WEBAPP;
                case BADCONFIG:
                    return TYPE_BAD_CONFIG;
                default:
                    return "unknown";
            }
        }
    }

    private static final String[] configClasses = {
                    "org.eclipse.jetty.webapp.WebInfConfiguration",
                    "org.eclipse.jetty.webapp.WebXmlConfiguration",
                    "org.eclipse.jetty.webapp.MetaInfConfiguration",
                    "org.eclipse.jetty.webapp.FragmentConfiguration",
                    "org.eclipse.jetty.plus.webapp.EnvConfiguration",
                    "org.eclipse.jetty.plus.webapp.PlusConfiguration",
                    "org.eclipse.jetty.webapp.JettyWebXmlConfiguration",
                    "org.eclipse.jetty.webapp.TagLibConfiguration" };

    public static final String TYPE_FILE = "file";
    public static final String TYPE_DIRECTORY = "directory";
    public static final String TYPE_CONTEXT = "context";
    public static final String TYPE_REDIRECT = "redirect";
    public static final String TYPE_CLASS = "class";
    public static final String TYPE_WEBAPP = "webapp";
    public static final String TYPE_BAD_CONFIG = "badconfig";
    public static final String HTTP_SERVICE = "httpService";
    public static final String CELL_ENDPOINT = "serviceCellEndpoint";
    public static final String JNDI_ARGS = "jndiArgs";

    public static AliasEntry createEntry(Args args, HttpServiceCell cell)
                    throws Exception {
        String alias = args.argv(0);
        String type = args.argv(1);
        String specific = args.argv(2);

        AliasType aliasType = AliasType.fromType(type);
        AliasEntry entry = null;
        Handler handler = null;
        String failure = null;

        switch (aliasType) {
            case FILE:
            case DIR:
                File dir = new File(specific);
                if ((!dir.isDirectory()) && (!dir.isFile())) {
                    throw new FileNotFoundException(specific);
                }
                handler = new PathHandler(dir);
                entry = new AliasEntry(alias, aliasType, handler, specific);
                entry.statusMessage = alias + " -> " + TYPE_DIRECTORY + "("
                                + specific + ")";
                break;
            case CONTEXT:
                handler = new ContextHandler(specific, cell.getDomainContext());
                entry = new AliasEntry(alias, aliasType, handler, specific);
                entry.setOnError(args.getOpt("onError"));
                entry.setOverwrite(args.getOpt("overwrite"));
                entry.statusMessage = alias + " -> " + TYPE_CONTEXT + "("
                                + specific + ")";
                break;
            case REDIRECT:
                handler = new RedirectHandler(alias, specific);
                entry = new AliasEntry(alias, aliasType, handler, specific);
                entry.statusMessage = alias + " -> " + TYPE_REDIRECT + "("
                                + specific + ")";
                break;
            case ENGINE:
                int argcount = args.argc() - 3;
                String[] handlerArgs = new String[argcount];
                StringBuilder sb = new StringBuilder();
                sb.append("class=").append(specific);

                for (int i = 0; i < argcount; i++) {
                    handlerArgs[i] = args.argv(3 + i);
                    sb.append(";").append(handlerArgs[i]);
                }

                ResponseEngineHandler rhandler
                    = new ResponseEngineHandler(specific, handlerArgs);

                try {
                    rhandler.initialize(cell);
                    handler = rhandler;
                } catch (Exception e) {
                    handler = new BadConfigHandler();
                    aliasType = AliasType.BADCONFIG;
                    failure = "failed to load class " + specific;
                    sb.append(" FAILED TO LOAD CLASS");
                }

                entry = new AliasEntry(alias, aliasType, handler, sb.toString());
                entry.setIntFailureMsg(failure);
                entry.statusMessage = alias + " -> " + TYPE_CLASS + "("
                                + entry.getSpecificString() + ")";
                break;
            case WEBAPP:
                handler = createWebAppContext(alias, specific, cell);
                entry = new AliasEntry(alias, aliasType, handler, args.toString());
                entry.statusMessage = alias + " -> " + TYPE_WEBAPP
                                            + "(" + args + ")";
                break;
            case BADCONFIG:
                handler = new BadConfigHandler();
                entry = new AliasEntry(alias, aliasType, handler, specific);
                entry.statusMessage = alias + " -> " + TYPE_BAD_CONFIG + "("
                                + specific + ")";
                break;
            default:
                /*
                 * should not get here (unknown alias type throws exception
                 *
                 * @ fromType())
                 */
        }

        if (handler instanceof BadConfigHandler && entry != null) {
            ((BadConfigHandler) handler).setFailureMessage(entry.getIntFailureMsg());
        }
        return entry;
    }

    private static Handler createWebAppContext(String alias,
                    String webappsPath, HttpServiceCell cell) throws Exception {
        String context = "/" + alias;
        Map<String, Object> env = cell.getEnvironment();

        File war = new File(webappsPath, context + ".war");
        File tmpDir = new File(cell.getTmpUnpackDir(), alias);

        WebAppContext webappContext = new WebAppContext();
        webappContext.setDefaultsDescriptor(cell.getDefaultWebappsXml());
        webappContext.setContextPath(context);
        webappContext.setWar(war.getAbsolutePath());
        webappContext.setExtractWAR(true);
        webappContext.setTempDirectory(tmpDir);
        webappContext.setConfigurationClasses(configClasses);

        /*
         * export to JNDI (constructor binds the entry into JNDI); all resources
         * and env entries are scoped to the webapp context
         */
        new EnvEntry(webappContext, CELL_ENDPOINT, cell.getEndpoint(), true);

        Properties properties = new Properties();
        for (String key : env.keySet()) {
            properties.setProperty(key, String.valueOf(env.get(key)));
        }

        new EnvEntry(webappContext, JNDI_ARGS, properties, true);

        webappContext.setServer(cell.getServer());
        webappContext.start();

        return webappContext;
    }

    private final String name;
    private final AliasType type;
    private final Handler handler;
    private final String spec;
    private final CellInfoProvider info;

    private String onError;
    private String overwrite;
    private String intFailureMsg;
    private String statusMessage;

    private AliasEntry(String name, AliasType type, Handler handler, String spec) {
        this.name = name;
        this.type = type;
        this.handler = handler;
        this.spec = spec;

        if (handler instanceof ResponseEngineHandler) {
            info = ((ResponseEngineHandler) handler).getCellInfoProvider();
        } else {
            info = null;
        }
    }

    public Handler getHandler() {
        return handler;
    }

    public void getInfo(PrintWriter pw) {
        if (info == null) {
            pw.println(toString());
        } else {
            info.getInfo(pw);
        }
    }

    public String getIntFailureMsg() {
        return intFailureMsg;
    }

    public String getName() {
        return name;
    }

    public String getOnError() {
        return onError;
    }

    public String getOverwrite() {
        return overwrite;
    }

    public String getSpecificString() {
        return spec;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getType() {
        return type.toType();
    }

    public void setIntFailureMsg(String entry) {
        intFailureMsg = entry;
    }

    public void setOnError(String entry) {
        onError = entry;
    }

    public void setOverwrite(String entry) {
        overwrite = entry;
    }

    public void shutdown() {
        if (handler instanceof ResponseEngineHandler) {
            ((ResponseEngineHandler) handler).getEngine().shutdown();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type.toType()).append("(").append(spec).append(")");
        if (onError != null) {
            sb.append(" [onError=").append(onError).append("]");
        }
        if (overwrite != null) {
            sb.append(" [overwrite ").append(overwrite).append("]");
        }
        return sb.toString();
    }
}
