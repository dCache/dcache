package org.dcache.services.httpd;

import com.google.common.base.Joiner;
import jersey.repackaged.com.google.common.base.Throwables;
import org.eclipse.jetty.server.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.util.HttpResponseEngine;

import org.dcache.services.httpd.handlers.BadConfigHandler;
import org.dcache.services.httpd.handlers.ContextHandler;
import org.dcache.services.httpd.handlers.HandlerDelegator;
import org.dcache.services.httpd.handlers.PathHandler;
import org.dcache.services.httpd.handlers.RedirectHandler;
import org.dcache.services.httpd.handlers.ResponseEngineHandler;
import org.dcache.services.httpd.handlers.WebAppHandler;
import org.dcache.services.httpd.util.AliasEntry;
import org.dcache.util.Args;

import static org.dcache.services.httpd.util.AliasEntry.AliasType;

public class HttpdCommandLineInterface
        implements CellCommandListener
{
    private static final Logger logger = LoggerFactory.getLogger(HttpdCommandLineInterface.class);

    @Autowired
    private AutowireCapableBeanFactory beanFactory;

    private HandlerDelegator delegator;

    @Required
    public void setDelegator(HandlerDelegator delegator)
    {
        this.delegator = delegator;
    }

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
            + "   webapp             <warPath> <...> \n"
            + "   redirect           <forward-to-context>\n"
            + "   predefined alias : <home>    =  default for http://host:port/ \n"
            + "                      <default> =  default for any type or error \n";

    public String ac_set_alias_$_3_16(Args args)
            throws NoSuchMethodException, IllegalAccessException, InstantiationException, FileNotFoundException,
            InvocationTargetException, ClassNotFoundException, IllegalArgumentException
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
    public String ac_unset_alias_$_1(Args args) throws InvocationTargetException
    {
        delegator.removeAlias(args.argv(0));
        return "Done";
    }

    private AliasEntry createEntry(String alias, String type, Args args)
            throws FileNotFoundException, ClassNotFoundException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException
    {
        String specific = args.argv(0);
        args.shift();

        AliasType aliasType = AliasType.fromType(type);
        AliasEntry entry;
        Handler handler;

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
            handler = (Handler) beanFactory.initializeBean(new ContextHandler(specific), alias);
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
            StringBuilder sb = new StringBuilder();
            sb.append("class=").append(specific);
            Joiner.on(";").appendTo(sb, args.getArguments());
            String failure = null;

            Class<? extends HttpResponseEngine> c = Class.forName(specific).asSubclass(HttpResponseEngine.class);
            Constructor<? extends HttpResponseEngine> constr = c.getConstructor(String[].class);
            try {
                HttpResponseEngine engine = constr.newInstance(new Object[] { args.getArguments().toArray(new String[args.argc()]) });
                handler = new ResponseEngineHandler((HttpResponseEngine) beanFactory.initializeBean(engine, alias));
            } catch (InvocationTargetException e) {
                Throwables.propagateIfPossible(e.getCause());
                throw e;
            }

            entry = new AliasEntry(alias, aliasType, handler, sb.toString());
            entry.setIntFailureMsg(failure);
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + entry.getSpecificString() + ")");
            break;
        case WEBAPP:
            WebAppHandler webappContext = (WebAppHandler) beanFactory.getBean("webapp-handler");
            webappContext.setWar(new File(specific).getAbsolutePath());
            webappContext.setContextPath("/" + alias);
            handler = webappContext;
            entry = new AliasEntry(alias, aliasType, handler, specific);
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + args + ")");
            break;
        default:
            handler = new BadConfigHandler();
            entry = new AliasEntry(alias, aliasType, handler, specific);
            entry.setStatusMessage(alias + " -> " + aliasType.getType() + "(" + specific + ")");
            break;
        }

        if (handler instanceof BadConfigHandler) {
            ((BadConfigHandler) handler).setFailureMessage(entry.getIntFailureMsg());
        }
        return entry;
    }
}
