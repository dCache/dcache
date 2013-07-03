package org.dcache.services.httpd.util;

import org.eclipse.jetty.plus.jndi.EnvEntry;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.Map;

import dmg.util.Args;
import dmg.util.HttpResponseEngine;

import org.dcache.services.httpd.HttpServiceCell;
import org.dcache.services.httpd.handlers.BadConfigHandler;
import org.dcache.services.httpd.handlers.ContextHandler;
import org.dcache.services.httpd.handlers.PathHandler;
import org.dcache.services.httpd.handlers.RedirectHandler;
import org.dcache.services.httpd.handlers.ResponseEngineHandler;

/**
 * Abstraction for httpd aliases.  Integrates Jetty webapp type.
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
    public static final String CELL_ENDPOINT = "serviceCellEndpoint";
    public static final String CELL_NUCLEUS = "serviceCellNucleus";
    public static final String OPT_UNPACK_DIR = "tempUnpackDir";

    public static AliasEntry createEntry(Args args, HttpServiceCell cell)
                    throws Exception {
        final String alias = args.argv(0);
        final String type = args.argv(1);
        String specific = args.argv(2);

        AliasType aliasType = AliasType.fromType(type);
        AliasEntry entry = null;
        Handler handler = null;
        String failure = null;

        switch (aliasType) {
            case FILE:
            case DIR:
                final File dir = new File(specific);
                if ((!dir.isDirectory()) && (!dir.isFile())) {
                    throw new Exception("Directory/File not found : " + specific);
                }
                handler = new PathHandler(dir);
                entry = new AliasEntry(alias, aliasType, handler, specific);
                entry.statusMessage = alias + " -> " + TYPE_DIRECTORY + "("
                                + specific + ")";
                break;
            case CONTEXT:
                final int pos = specific.indexOf("*");
                if (pos > -1) {
                    specific = specific.substring(0, pos);
                }
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
                final int argcount = args.argc() - 3;
                final String[] handlerArgs = new String[argcount];
                final StringBuilder sb = new StringBuilder();
                sb.append("class=").append(specific);

                for (int i = 0; i < argcount; i++) {
                    handlerArgs[i] = args.argv(3 + i);
                    sb.append(";").append(handlerArgs[i]);
                }

                final ResponseEngineHandler rhandler
                    = new ResponseEngineHandler(specific, handlerArgs);

                try {
                    rhandler.initialize(cell);
                    handler = rhandler;
                } catch (final Exception e) {
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
                handler = createWebAppContext(alias, specific, new Args(args.argv(3)), cell);
                entry = new AliasEntry(alias, aliasType, handler, args.toString());
                entry.statusMessage = alias + " -> " + TYPE_WEBAPP + "(" + args + ")";
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
                 * @ fromType())
                 */
        }

        if (handler instanceof BadConfigHandler && entry != null) {
            ((BadConfigHandler) handler).setFailureMessage(entry.getIntFailureMsg());
        }
        return entry;
    }

    private static Handler createWebAppContext(String alias, String webappsPath,
                    Args args, HttpServiceCell cell) throws Exception {
            final String context = "/" + alias;
            final String tmp = args.getOpt(OPT_UNPACK_DIR);

            final File war = new File(webappsPath, context + ".war");
            final File tmpDir = new File(tmp, alias);

            final WebAppContext webappContext = new WebAppContext();
            webappContext.setDefaultsDescriptor(cell.getDefaultWebappsXml());
            webappContext.setContextPath(context);
            webappContext.setWar(war.getAbsolutePath());
            webappContext.setExtractWAR(true);
            webappContext.setTempDirectory(tmpDir);
            webappContext.setConfigurationClasses(configClasses);

            /*
             * export to JNDI (constructor binds the entry into JNDI); all
             * resources and env entries are scoped to the webapp context
             */
            new EnvEntry(webappContext, CELL_ENDPOINT, cell, true);
            new EnvEntry(webappContext, CELL_NUCLEUS, cell.getNucleus(), true);

            final Map<String, String> opts = args.optionsAsMap();
            for (final String key : opts.keySet()) {
                new EnvEntry(webappContext, key, opts.get(key), true);
            }

            webappContext.setServer(cell.getServer());
            webappContext.start();

            return webappContext;
    }

    private final String name;
    private final AliasType type;
    private final Handler handler;
    private final String spec;

    private String onError;
    private String overwrite;
    private String intFailureMsg;
    private String statusMessage;
    private Method getInfo;

    private AliasEntry(String name, AliasType type, Handler handler, String spec)
                    throws NoSuchMethodException, SecurityException {
        this.name = name;
        this.type = type;
        this.handler = handler;
        this.spec = spec;

        if (handler instanceof ResponseEngineHandler) {
            HttpResponseEngine engine
                = ((ResponseEngineHandler) handler).getEngine();
            getInfo = engine.getClass().getMethod("getInfo",
                            new Class[] { PrintWriter.class });
        }
    }

    public Handler getHandler() {
        return handler;
    }

    public void getInfo(PrintWriter pw) {
        if (getInfo == null) {
            pw.println(toString());
            return;
        }

        try {
            getInfo.invoke(handler, pw);
        } catch (final Exception e) {
            pw.println("Exception : " + e);
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
        final StringBuilder sb = new StringBuilder();
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
