package org.dcache.services.httpd;

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.PrintWriter;
import java.net.URL;
import java.util.Map;
import java.util.NoSuchElementException;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.DomainContextAware;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.CommandInterpreter;

import org.dcache.services.httpd.handlers.HandlerDelegator;
import org.dcache.services.httpd.util.AliasEntry;
import org.dcache.util.Args;

public class HttpServiceCell extends CommandInterpreter
                             implements CellMessageReceiver,
                                        CellMessageSender,
                                        CellCommandListener,
                                        CellInfoProvider,
                                        DomainContextAware,
                                        EnvironmentAware
{
    private static final Logger logger = LoggerFactory.getLogger(HttpServiceCell.class);

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

    public String ac_set_alias_$_3_16(Args args) throws Exception {
        logger.debug("ac_set_alias_$_3_16 {}", args.toString());
        AliasEntry entry = AliasEntry.createEntry(args, this);
        logger.debug("putting {}, {}", entry.getName(), entry);
        delegator.addAlias(entry.getName(), entry);
        return entry.getStatusMessage();
    }

    public static final String hh_unset_alias = "<aliasName>";

    public String ac_unset_alias_$_1(Args args) {
        delegator.removeAlias(args.argv(0));
        return "Done";
    }

    public String getDefaultWebappsXml() {
        return defaultWebappsXml;
    }

    public Map<String, Object> getDomainContext() {
        return domainContext;
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
