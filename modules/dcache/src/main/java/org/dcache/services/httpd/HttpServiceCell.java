package org.dcache.services.httpd;

import org.eclipse.jetty.plus.jndi.EnvEntry;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.DomainContextAware;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.CommandInterpreter;

import org.dcache.services.httpd.handlers.BadConfigHandler;
import org.dcache.services.httpd.handlers.ContextHandler;
import org.dcache.services.httpd.handlers.HandlerDelegator;
import org.dcache.services.httpd.handlers.PathHandler;
import org.dcache.services.httpd.handlers.RedirectHandler;
import org.dcache.services.httpd.handlers.ResponseEngineHandler;
import org.dcache.services.httpd.util.AliasEntry;
import org.dcache.util.Args;

import static org.dcache.services.httpd.util.AliasEntry.AliasType;

public class HttpServiceCell extends CommandInterpreter
                             implements CellMessageReceiver,
                                        CellMessageSender,
                                        CellCommandListener,
                                        CellInfoProvider,
                                        DomainContextAware,
                                        EnvironmentAware
{
    private static final Logger logger = LoggerFactory.getLogger(HttpServiceCell.class);

    private static final String[] configClasses = {
            "org.eclipse.jetty.webapp.WebInfConfiguration",
            "org.eclipse.jetty.webapp.WebXmlConfiguration",
            "org.eclipse.jetty.webapp.MetaInfConfiguration",
            "org.eclipse.jetty.webapp.FragmentConfiguration",
            "org.eclipse.jetty.plus.webapp.EnvConfiguration",
            "org.eclipse.jetty.plus.webapp.PlusConfiguration",
            "org.eclipse.jetty.webapp.JettyWebXmlConfiguration" };

    public static final String CELL_ENDPOINT = "serviceCellEndpoint";
    public static final String JNDI_ARGS = "jndiArgs";

    /**
     * Where the war should be unpacked
     */
    private String tmpUnpackDir;

    private CellEndpoint endpoint;
    private Server server;
    private String defaultWebappsXml;
    private Map<String, Object> domainContext;
    private Map<String, Object> environment;
    private HandlerDelegator delegator;

    public static final String hh_ls_alias = "[<alias>]";
    public String ac_ls_alias_$_0_1(Args args) throws NoSuchElementException
    {
        if (args.argc() == 0) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, AliasEntry> aliasEntry : delegator.getAliases().entrySet()) {
                sb.append(aliasEntry.getKey()).append(" -> ").append(
                                aliasEntry.getValue()).append("\n");
            }
            return sb.toString();
        } else {
            AliasEntry entry = delegator.getAlias(args.argv(0));
            if (entry == null) {
                throw new NoSuchElementException("Alias not found : " + args.argv(0));
            }
            return args.argv(0) + " -> " + entry;
        }
    }

    public static final String hh_set_alias = "<aliasName> directory|class|context <specification>";
    public static final String fh_set_alias = "set alias <alias>  <type> [<typeSpecific> <...>]\n"
                    + "   <type>             <specific> \n"
                    + "   directory          <fullDirectoryPath>\n"
                    + "   file               <fullFilePath> <arguments> <...>\n"
                    + "   class              <fullClassName> <...>\n"
                    + "   context            [options] <context> or  <contextNameStart>*\n"
                    + "                       options : -overwrite=<alias> -onError=<alias>\n"
                    + "   webapp             <webappsContext> <webappsPath> <tempUnpackDir> <...> \n"
                    + "   redirect           <forward-to-context>\n"
                    + "   predefined alias : <home>    =  default for http://host:port/ \n"
                    + "                      <default> =  default for any type or error \n";
    public String ac_set_alias_$_3_16(Args args) throws Exception
    {
        String alias = args.argv(0);
        String type = args.argv(1);
        args.shift(2);
        AliasEntry entry = createEntry(alias, type, args);
        logger.debug("Creating alias {}: {}", entry.getName(), entry);
        delegator.addAlias(entry.getName(), entry);
        return entry.getStatusMessage();
    }

    public static final String hh_unset_alias = "<aliasName>";
    public String ac_unset_alias_$_1(Args args)
    {
        delegator.removeAlias(args.argv(0));
        return "Done";
    }

    private AliasEntry createEntry(String alias, String type, Args args) throws Exception
    {
        String specific = args.argv(0);

        AliasType aliasType = AliasType.fromType(type);
        AliasEntry entry;
        Handler handler;
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
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + specific + ")");
            break;
        case CONTEXT:
            handler = new ContextHandler(specific, domainContext);
            entry = new AliasEntry(alias, aliasType, handler, specific);
            entry.setOnError(args.getOpt("onError"));
            entry.setOverwrite(args.getOpt("overwrite"));
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + specific + ")");
            break;
        case REDIRECT:
            handler = new RedirectHandler(alias, specific);
            entry = new AliasEntry(alias, aliasType, handler, specific);
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + specific + ")");
            break;
        case ENGINE:
            args.shift();
            int argcount = args.argc();
            String[] handlerArgs = new String[argcount];
            StringBuilder sb = new StringBuilder();
            sb.append("class=").append(specific);

            for (int i = 0; i < argcount; i++) {
                handlerArgs[i] = args.argv(i);
                sb.append(";").append(handlerArgs[i]);
            }

            ResponseEngineHandler rhandler
                    = new ResponseEngineHandler(specific, handlerArgs);

            try {
                rhandler.initialize(this);
                handler = rhandler;
            } catch (Exception e) {
                handler = new BadConfigHandler();
                aliasType = AliasType.BADCONFIG;
                failure = "failed to load class " + specific;
                sb.append(" FAILED TO LOAD CLASS");
            }

            entry = new AliasEntry(alias, aliasType, handler, sb.toString());
            entry.setIntFailureMsg(failure);
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + entry.getSpecificString() + ")");
            break;
        case WEBAPP:
            handler = createWebAppContext(alias, specific);
            entry = new AliasEntry(alias, aliasType, handler, args.toString());
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + args + ")");
            break;
        case BADCONFIG:
            handler = new BadConfigHandler();
            entry = new AliasEntry(alias, aliasType, handler, specific);
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + specific + ")");
            break;
        default:
            throw new RuntimeException("Unreachable statement (" + aliasType + ")");
        }

        if (handler instanceof BadConfigHandler) {
            ((BadConfigHandler) handler).setFailureMessage(entry.getIntFailureMsg());
        }
        return entry;
    }

    private Handler createWebAppContext(String alias, String webappsPath)
            throws Exception
    {
        String context = "/" + alias;
        Map<String, Object> env = getEnvironment();

        File war = new File(webappsPath, context + ".war");
        File tmpDir = new File(getTmpUnpackDir(), alias);

        WebAppContext webappContext = new WebAppContext();
        webappContext.setDefaultsDescriptor(defaultWebappsXml);
        webappContext.setContextPath(context);
        webappContext.setWar(war.getAbsolutePath());
        webappContext.setExtractWAR(true);
        webappContext.setTempDirectory(tmpDir);
        webappContext.setConfigurationClasses(configClasses);

        /*
         * export to JNDI (constructor binds the entry into JNDI); all resources
         * and env entries are scoped to the webapp context
         */
        new EnvEntry(webappContext, CELL_ENDPOINT, endpoint, true);

        Properties properties = new Properties();
        for (String key : env.keySet()) {
            properties.setProperty(key, String.valueOf(env.get(key)));
        }

        new EnvEntry(webappContext, JNDI_ARGS, properties, true);

        webappContext.setServer(server);
        webappContext.start();

        return webappContext;
    }

    public CellEndpoint getEndpoint() {
        return endpoint;
    }

    public Map<String, Object> getEnvironment() {
        return environment;
    }

    @Override
    public CellInfo getCellInfo(CellInfo info) {
        return info;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        for (Map.Entry<String, AliasEntry> aliasEntry : delegator.getAliases().entrySet()) {
            pw.println("<<<<< " + aliasEntry.getKey() + " >>>>>>>>>");
            aliasEntry.getValue().getInfo(pw);
        }
    }

    public Server getServer() {
        return server;
    }

    public String getTmpUnpackDir() {
        return tmpUnpackDir;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void setDomainContext(Map<String, Object> context) {
        this.domainContext = context;
    }

    @Override
    public void setEnvironment(Map<String, Object> environment) {
        this.environment = environment;
    }

    @Required
    public void setTmpUnpackDir(String tmpUnpackDir) {
        this.tmpUnpackDir = tmpUnpackDir;
    }

    @Required
    public void setWebappResourceUrl(String webappResourceUrl) {
        URL url = HttpServiceCell.class.getResource(webappResourceUrl);
        defaultWebappsXml = url.toExternalForm();
    }

    @Required
    public void setServer(Server server)
    {
        this.server = server;
    }

    @Required
    public void setDelegator(HandlerDelegator delegator)
    {
        this.delegator = delegator;
    }
}
