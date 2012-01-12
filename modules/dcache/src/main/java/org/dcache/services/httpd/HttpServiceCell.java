package org.dcache.services.httpd;

import dmg.util.Args;
import dmg.util.HttpRequest;
import dmg.util.HttpException;
import dmg.util.HttpBasicAuthenticationException;
import dmg.util.HttpResponseEngine;
import dmg.util.CollectionFactory;
import dmg.cells.nucleus.EnvironmentAware;
import dmg.protocols.kerberos.Base64;
import java.util.Map;
import java.util.SortedMap;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.cells.AbstractCell;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class HttpServiceCell
    extends AbstractCell implements EnvironmentAware
{
    private final static Logger _log =
        LoggerFactory.getLogger(HttpServiceCell.class);

    private final static Splitter PATH_SPLITTER =
        Splitter.on('/').omitEmptyStrings();

    private final Map<String, AliasEntry> _aliasHash = Maps.newHashMap();
    private Map<String,Object> _context;
    private int _listenPort;
    private Server _jetty;
    private volatile Map<String,Object> _environment;

    private static final FileNameMap __mimeTypeMap =
        URLConnection.getFileNameMap();

    public HttpServiceCell(String name, String args)
        throws InterruptedException, ExecutionException
    {
        super(name, args);
        doInit();
    }

    @Override
    protected void init()
        throws Exception
    {
        Args args = getArgs();
        if (args.argc() < 1) {
            throw new IllegalArgumentException("USAGE : ... <listenPort>");
        }

        _context = getDomainContext();
        _listenPort = Integer.parseInt(args.argv(0));

        _jetty = new Server(_listenPort);
        _jetty.setHandler(new HttpServiceCellHandler());
        _jetty.start();
    }

    @Override
    public void cleanUp()
    {
        try {
            _jetty.stop();
        } catch (Exception e) {
            _log.error("Failed to stop Jetty: {}", e.getMessage());
        }
        super.cleanUp();
    }

    public class HttpServiceCellHandler extends AbstractHandler
    {
        @Override
        public void handle(String target, Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
            throws IOException
        {
            new HtmlService(request, response).run();
        }
    }

    private static class AliasEntry
    {
        private String _intFailureMsg = null;
        private String _type;
        private Object _obj;
        private String _spec;
        private String _onError = null;
        private String _overwrite = null;
        private Method _getInfo = null;

        public AliasEntry(String type, Object obj, String spec)
        {
            Class [] argClasses = { java.io.PrintWriter.class };
            _type = type;
            _obj = obj;
            _spec = spec;
            if (obj instanceof HttpResponseEngine) {
                try {
                    _getInfo = obj.getClass().getMethod("getInfo", argClasses);
                } catch (Exception e) {
                }
            }
        }

        public void getInfo(PrintWriter pw)
        {
            if (_getInfo == null) {
                pw.println(toString());
                return;
            }
            Object [] args = { pw };
            try {
                _getInfo.invoke(_obj, args);
            } catch (Exception e) {
                pw.println("Exception : " + e);
            }
        }

        public void setIntFailureMsg(String entry)
        {
            _intFailureMsg = entry;
        }

        public void setOnError(String entry)
        {
            _onError = entry;
        }

        public void setOverwrite(String entry)
        {
            _overwrite = entry;
        }

        public String getIntFailureMsg()
        {
            return _intFailureMsg;
        }

        public String getOnError()
        {
            return _onError;
        }

        public String getOverwrite()
        {
            return _overwrite;
        }

        public String getType()
        {
            return _type;
        }

        public Object getSpecific()
        {
            return _obj;
        }

        public String getSpecificString()
        {
            return _spec;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(_type).append("(").append(_spec).append(")");
            if(_onError != null)
                sb.append( " [onError=").append(_onError).append("]");
            if(_overwrite != null)
                sb.append(" [overwrite ").append(_overwrite).append("]");
            return sb.toString();
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        for (Map.Entry<String, AliasEntry> aliasEntry: _aliasHash.entrySet()) {
            pw.println("<<<<< " + aliasEntry.getKey() + " >>>>>>>>>");
            aliasEntry.getValue().getInfo(pw);
        }
    }
    public String hh_ls_alias = "[<alias>]";
    public String ac_ls_alias_$_0_1(Args args)
        throws Exception
    {
        AliasEntry entry = null;
        if (args.argc() == 0) {
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<String, AliasEntry> aliasEntry : _aliasHash.entrySet() ) {
                sb.append(aliasEntry.getKey()).append(" -> ").
                    append(aliasEntry.getValue()).append("\n");
            }
            return sb.toString();
        } else {
            entry = _aliasHash.get(args.argv(0));
            if (entry == null)
                throw new Exception("Alias not found : " + args.argv(0));
            return args.argv(0) + " -> " + entry;
        }
    }

    public String hh_unset_alias = "<aliasName>";
    public String ac_unset_alias_$_1(Args args)
    {
        _aliasHash.remove(args.argv(0));
        return "Done";
    }

    public String hh_set_alias =
        "<aliasName> directory|class|context <specification>";

    public String fh_set_alias =
        "set alias <alias>  <type> [<typeSpecific> <...>]\n"+
        "    <type>         <specific> \n"+
        "   directory    <fullDirectoryPath>\n"+
        "   file         <fullFilePath> <arguments> <...>\n"+
        "   class        <fullClassName>\n"+
        "   context      [options] <context> or  <contextNameStart>*\n" +
        "                  options : -overwrite=<alias> -onError=<alias>\n" +
        "       predefined alias : <home> =  default for http://host:port/ \n" +
        "                          <default> =  default for any type or error \n";

    public String ac_set_alias_$_3_16(Args args) throws Exception
    {
        String alias = args.argv(0);
        String type = args.argv(1);
        String spec = args.argv(2);

        if(type.equals("directory") ||
            type.equals("file")        ) {

            File dir = new File(spec);
            if((! dir.isDirectory()) &&
                (! dir.isFile()     )   )
                throw new
                    Exception("Directory/File not found : "+spec);

            _aliasHash.put( alias,
                             new AliasEntry("directory",
                                             dir,
                                             spec         ));
            return alias+" -> directory("+spec+")";

        }else if(type.equals("context")) {

            int pos = spec.indexOf("*");
            if(pos > -1)spec = spec.substring(0,pos);

            AliasEntry entry = new AliasEntry(type, spec, spec);

            String     tmp = args.getOpt("onError");
            if(tmp != null)entry.setOnError(tmp);

            tmp = args.getOpt("overwrite");
            if(tmp != null)entry.setOverwrite(tmp);


            _aliasHash.put(alias, entry);
            return alias+" -> context("+spec+")";

        }else if(type.equals("class")) {

            int    argcount = args.argc() - 3;
            String []   arg = new String[argcount];
            StringBuilder sb = new StringBuilder();
            sb.append("class="+spec);
            for(int i = 0; i < argcount; i++) {
                arg[i] = args.argv(3+i);
                sb.append(";").append(arg[i]);
            }

            HttpResponseEngine engine=null;
            String intFailureMsg=null, retMsg;

            try {
                engine = invokeHttpEngine(spec, arg);
                retMsg = alias+" -> class("+sb.toString()+")";
            } catch(ClassNotFoundException e) {
                type = "badconfig";
                intFailureMsg = "failed to load class "+spec;
                retMsg = alias+" -> class("+sb.toString()+")  FAILED TO LOAD CLASS";
            }

            AliasEntry aliasEntry = new AliasEntry(type, engine, sb.toString());
            _aliasHash.put(alias, aliasEntry);

            if(engine == null)
                aliasEntry.setIntFailureMsg(intFailureMsg);

            return retMsg;

        }else if(type.equals("cell")) {

            _aliasHash.put(alias, new AliasEntry(type, spec, spec));
            return "";

        }
        throw new Exception("Unknown type : "+type);
    }

    private HttpResponseEngine invokeHttpEngine(String className, String [] a)
        throws Exception
    {
        Class     c = Class.forName(className);
        Class  [] argsClass = null;
        Object [] args = null;
        Constructor constr = null;
        //
        // trying to find a contructor
        //   <init>(CellNucleus nucleus, String [] args)
        //   <init>(String [] args)
        //   <init>()
        //
        HttpResponseEngine engine = null;
        try{
            argsClass = new Class[2];
            argsClass[0] = dmg.cells.nucleus.CellNucleus.class;
            argsClass[1] = java.lang.String[].class;
            constr = c.getConstructor(argsClass);
            args = new Object[2];
            args[0] = getNucleus();
            args[1] = a;
            engine = (HttpResponseEngine)constr.newInstance(args);
        }catch(Exception e) {
            try{
                argsClass = new Class[1];
                argsClass[0] = java.lang.String[].class;
                constr = c.getConstructor(argsClass);
                args = new Object[1];
                args[0] = a;
                engine = (HttpResponseEngine)constr.newInstance(args);
            }catch(Exception ee) {
                argsClass = new Class[0];
                constr = c.getConstructor(argsClass);
                args = new Object[0];
                engine =  (HttpResponseEngine)constr.newInstance(args);
            }
        }
        addCommandListener(engine);
        if(engine instanceof EnvironmentAware) {
            ((EnvironmentAware) engine).setEnvironment(_environment);
        }
        return engine;
    }

    private class HtmlService
        implements HttpRequest
    {
        private InputStream _in;
        private OutputStream _out;
        private BufferedReader _br;
        private PrintWriter _pw;
        private HttpServletRequest _request;
        private HttpServletResponse _response;
        private Map<String,String> _map = CollectionFactory.newHashMap();
        private String[] _tokens;
        private int _tokenOffset = 1;
        private boolean _isDirectory = false;
        private String _userName;
        private String _password;
        private boolean _authDone = false;

        //
        // the HttpRequest interface
        //
        @Override
        public void setContentType(String type)
        {
            _response.setContentType(type);
        }

        @Override
        public Map<String,String> getRequestAttributes()
        {
            return _map;
        }

        @Override
        public OutputStream getOutputStream()
        {
            return _out;
        }

        @Override
        public PrintWriter getPrintWriter()
        {
            return _pw;
        }

        @Override
        public String[] getRequestTokens()
        {
            return _tokens;
        }

        @Override
        public int getRequestTokenOffset()
        {
            return _tokenOffset;
        }

        @Override
        public boolean isDirectory()
        {
            return _isDirectory;
        }

        @Override
        public String getParameter(String parameter) {
            return _request.getParameter(parameter);
        }

        private synchronized void doAuthorization()
        {
            if (_authDone) return;
            _authDone = true;
            String auth = _request.getHeader("Authorization");
            if (auth == null) return;
            StringTokenizer st = new StringTokenizer(auth);
            if (st.countTokens() < 2) return;
            if (!st.nextToken().equals("Basic")) return;
            auth = new String(Base64.decode(st.nextToken()));
            _log.info("Authentication : >{}<", auth);
            st = new StringTokenizer(auth, ":");
            if(st.countTokens() < 2)return;
            _userName = st.nextToken();
            _password = st.nextToken();
        }

        @Override
        public boolean isAuthenticated()
        {
            doAuthorization();
            return _userName != null;
        }

        @Override
        public String getUserName()
        {
            doAuthorization();
            return _userName;
        }

        @Override
        public String getPassword()
        {
            doAuthorization();
            return _password;
        }

        private HtmlService(HttpServletRequest request,
                            HttpServletResponse response) throws IOException
        {
            _request = request;
            _response = response;
            _in = request.getInputStream();
            _out = response.getOutputStream();
            _br = new BufferedReader(new InputStreamReader(_in));
            _pw = new PrintWriter(new OutputStreamWriter(_out));
            setContentType("text/html");
        }

        private String getContentTypeFor(String fileName)
        {
            if (fileName.endsWith(".html")) {
                return "text/html";
            } else if (fileName.endsWith(".css")) {
                return "text/css";
            } else {
                return __mimeTypeMap.getContentTypeFor(fileName);
            }
        }

        public void run()
        {
            try {
                Enumeration<String> names = _request.getHeaderNames();
                while (names.hasMoreElements()) {
                    String name = names.nextElement();
                    _map.put(name, _request.getHeader(name));
                }

                if (!_request.getMethod().equals("GET")) {
                    throw new HttpException(HttpServletResponse.SC_NOT_IMPLEMENTED, "Method not implemented");
                }

                splitUrl(_request.getRequestURI());

                AliasEntry  entry = null;
                String      alias = null;

                alias = _tokens.length == 0 ? "<home>" : _tokens[0];
                _tokenOffset = 1;
                try {
                    entry = _aliasHash.get(alias);
                    if(entry == null)
                        throw new HttpException(HttpServletResponse.SC_NOT_FOUND,
                                                "Alias not found : "+alias);

                    switchHttpType(entry);

                } catch (HttpException e) {
                    if (e.getErrorCode() != HttpServletResponse.SC_NOT_FOUND)
                        throw e;
                    entry = _aliasHash.get("<default>");
                    if (entry == null)
                        throw e;
                    switchHttpType(entry);
                }

                _pw.flush();
            } catch (HttpException e) {
                printHttpException(e);
                _log.warn("Problem with {}: {}",
                        _request.getRequestURI(),
                        e.getMessage());
            } catch (Exception e) {
                printHttpException(new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                                                     "Bad Request : " + e));
                _log.warn("Problem in HtmlService: {}", e);
            }
            _log.info("Finished");
        }

        private void switchHttpType(AliasEntry entry) throws Exception
        {
            String type = entry.getType();

            if (type.equals("badconfig")) {
                StringBuilder sb = new StringBuilder();

                sb.append("HTTP Server badly configured");
                if (entry.getIntFailureMsg() != null) {
                    sb.append(": ");
                    sb.append(entry.getIntFailureMsg());
                }
                sb.append(".");

                throw new HttpException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                                        sb.toString());
            } else if (type.equals("directory")) {
                sendFile((File)entry.getSpecific(), _tokens);
            } else if (type.equals("context")) {
                String aliasString;
                AliasEntry aliasEntry;
                //
                //  are we overwritten ?
                //
                if(((aliasString = entry.getOverwrite()) != null) &&
                    ((aliasEntry = _aliasHash.get(aliasString)) != null)) {

                    switchHttpType(aliasEntry);
                    return;

                }
                String html;
                String specificName = (String) entry.getSpecific();
                if((_tokens.length > 1) && (_tokens[1].equals("index.html"))) {
                    html = createContextDirectory();
                }else{
                    if(_tokens.length > 1) {

                        String contextName = _tokens[1];
                        if (!contextName.startsWith(specificName))
                            throw new HttpException(HttpServletResponse.SC_FORBIDDEN, "Forbidden");

                        specificName = contextName;
                    }
                    html = (String) _context.get(specificName);
                }
                if (html == null) {
                    if (((aliasString = entry.getOnError()) == null) ||
                        ((aliasEntry = _aliasHash.get(aliasString)) == null))
                        throw new HttpException(HttpServletResponse.SC_NOT_FOUND, "Not found : "+specificName);
                    switchHttpType(aliasEntry);
                    return;
                }

                _pw.println(html);
            } else if(type.equals("class")) {
                HttpResponseEngine engine =
                    (HttpResponseEngine)entry.getSpecific();
                try {
                    engine.queryUrl(this);
                } catch (HttpException e) {
                    throw e;
                } catch (Exception e) {
                    throw new HttpException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "HttpResponseEngine ("+engine.getClass().getCanonicalName()+") is broken, please report this to sysadmin.");
                }

            }
        }

        private String createContextDirectory()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("<html><title>Context directory</title>\n");
            sb.append("<body bgcolor=\"#0088dd\">\n");
            sb.append("<h1>Context Directory</h1>\n");
            sb.append("<blockquote>\n");
            sb.append("<center>\n");
            sb.append("<table border=1 cellspacing=0 cellpadding=4 width=\"%90\">\n");
            sb.append("<tr><th>Context Name</th><th>Class</th><th>Content</th></tr>\n");
            SortedMap<String,Object> map = CollectionFactory.newTreeMap();
            map.putAll(_context);
            for (Map.Entry<String,Object> e: map.entrySet()) {
                String key = e.getKey();
                Object o = e.getValue();
                String str = o.toString();
                str = str.substring(0,Math.min(str.length(),60)).trim();
                str = htmlToRegularString(str);
                sb.append("<tr><td>").
                    append(key).
                    append("</td><td>").
                    append(o.getClass().getName()).
                    append("</td><td>").
                    append(str.length()==0?"&nbsp;":str).
                    append("</td></tr>\n");
            }
            sb.append("</table></center>\n");
            sb.append("</blockquote>\n");
            sb.append("<hr>");
            sb.append("<address>Created : ").append(new Date()).append("</address>\n");
            sb.append("</body></html>");
            return sb.toString();
        }

        private String htmlToRegularString(String str)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0, n = str.length(); i < n; i++) {
                char c = str.charAt(i);
                switch(c) {
                case '<' : sb.append("&lt;"); break;
                case '>' : sb.append("&gt;"); break;
                case '\n' : sb.append("\\n"); break;
                default : sb.append(c);
                }
            }
            return sb.toString();
        }

        private void sendFile(File base, String [] tokens) throws Exception
        {
            String filename = null;
            if( tokens.length < 2) {
                filename = "index.html";
            }else{
                StringBuilder sb = new StringBuilder();
                sb.append(tokens[1]);
                for(int i = 2; i < tokens.length; i++)
                    sb.append("/").append(tokens[i]);
                filename = sb.toString();
            }
            File f = base.isFile() ? base : new File(base, filename);
            if(!f.getCanonicalFile().getAbsolutePath().startsWith(base.getCanonicalFile().getAbsolutePath()))
               throw new HttpException(HttpServletResponse.SC_FORBIDDEN, "Forbidden");

            if(! f.isFile())
                throw new HttpException(HttpServletResponse.SC_NOT_FOUND, "Not found : "+ filename);

            FileInputStream binary = new FileInputStream(f);
            try {
                int rc = 0;
                byte[] buffer = new byte[4 * 1024];
                setContentType(getContentTypeFor(filename));
                while ((rc = binary.read(buffer, 0, buffer.length)) > 0) {
                    _out.write(buffer, 0, rc);
                }
            } finally {
                _out.flush();
                try {
                    binary.close();
                } catch (IOException e) {
                }
            }
        }

        @Override
        public void printHttpHeader(int size)
        {
            if (size > 0) {
                _response.setContentLength(size);
            }
        }

        public void printHttpException(HttpException exception)
        {
            try {
                if (exception instanceof HttpBasicAuthenticationException) {
                    String realm =
                        ((HttpBasicAuthenticationException) exception).getRealm();
                    _response.setHeader("WWW-Authenticate",
                                        "Basic realm=\"" + realm + "\"");
                }

                _response.sendError(exception.getErrorCode(),
                                    exception.getMessage());
            } catch (IOException e) {
                _log.warn("Failed to send reply: {}", e.getMessage());
            }
        }

        private void splitUrl(String url)
            throws URISyntaxException
        {
            String path = new URI(url).getPath();
            _isDirectory = path.endsWith("/");
            _tokens =
                Iterables.toArray(PATH_SPLITTER.split(path), String.class);
        }
    }

	@Override
	public void setEnvironment(Map<String, Object> environment) {
		_environment = environment;
	}
}
