 package  dmg.cells.services ;

import   dmg.cells.nucleus.* ;
import   dmg.util.* ;
import   dmg.protocols.kerberos.Base64 ;
import java.util.* ;
import java.io.* ;
import java.net.* ;
import java.text.*;
import java.lang.reflect.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class      HttpServiceCell
       extends    CellAdapter
       implements Runnable      {

   private final static Logger _log =
       LoggerFactory.getLogger(HttpServiceCell.class);

   private ServerSocket _listener ;
   private CellNucleus  _nucleus ;
   private Args         _args ;
   private int          _listenPort ;
   private Thread       _listenThread ;
   private int          _serial    = 0 ;
   private final Map<String, AliasEntry> _aliasHash = CollectionFactory.newHashMap();
   private final Map<String,Object> _context;
   private SimpleDateFormat _dateFormat =
                new SimpleDateFormat( "EEE, dd MMM yyyy hh:mm:ss z");

   private static final FileNameMap __mimeTypeMap = URLConnection.getFileNameMap();

   public HttpServiceCell( String name , String args ) throws Exception {
       super( name , args , false ) ;

       _args    = getArgs() ;
       _nucleus = getNucleus() ;
       _context = getDomainContext() ;

       // The time *must* be in GMT
       _dateFormat.setTimeZone( TimeZone.getTimeZone("GMT"));

       try{
          if( _args.argc() < 1 )
              throw new
              IllegalArgumentException( "USAGE : ... <listenPort>" ) ;

          _listenPort   = Integer.parseInt( _args.argv(0) ) ;
          _listener     = new ServerSocket( _listenPort ) ;
          _listenThread = _nucleus.newThread( this , "Listener" ) ;
          _listenThread.start() ;

       }catch(Exception e ){
          start();
          kill() ;
          throw e ;
       }

       start() ;


   }

    @Override
    public void cleanUp()
    {
        if (_listener != null) {
            try {
                _listener.close();
            } catch (IOException e) {
                _log.warn(e.getMessage());
            }
        }

        if( _listenThread != null) {
            _listenThread.interrupt();
            try {
                _listenThread.join();
            } catch (InterruptedException e1) {
                _log.warn("Interrupted while waiting for listener thread to end");
            }
        }

        super.cleanUp();
    }

   private static class AliasEntry {
	  private String _intFailureMsg = null;
      private String _type ;
      private Object _obj  ;
      private String _spec ;
      private String _onError   = null ;
      private String _overwrite = null ;
      private Method _getInfo   = null ;
      public AliasEntry( String type  , Object obj , String spec ){
         Class [] argClasses = { java.io.PrintWriter.class } ;
         _type = type ;
         _obj  = obj ;
         _spec = spec ;
         if( obj instanceof HttpResponseEngine ){
            try{
              _getInfo = obj.getClass().getMethod("getInfo",argClasses);
            }catch(Exception ee){}
         }
      }
      public void getInfo( PrintWriter pw ){
         if( _getInfo == null ){
            pw.println(toString());
            return ;
         }
         Object [] args = { pw } ;
         try{
            _getInfo.invoke( _obj , args ) ;
         }catch(Exception ee ){
            pw.println("Exception : "+ee ) ;
         }
      }
      public void setIntFailureMsg( String entry){ _intFailureMsg = entry ; }
      public void setOnError( String entry ){ _onError = entry ; }
      public void setOverwrite( String entry ){ _overwrite = entry ; }
      public String getIntFailureMsg(){ return _intFailureMsg ;}
      public String getOnError(){ return _onError ;}
      public String getOverwrite(){ return _overwrite ;}
      public String getType(){ return _type ; }
      public Object getSpecific(){ return _obj ; }
      public String getSpecificString(){ return _spec ; }

      @Override
      public String toString(){
         StringBuffer sb = new StringBuffer() ;
         sb.append( _type ).append("(").append(_spec).append(")") ;
         if( _onError != null )
               sb.append(  " [onError=" ).append(_onError).append("]");
         if( _overwrite != null )
               sb.append( " [overwrite " ).append(_overwrite).append("]");
         return sb.toString();
      }
   }

   @Override
   public void getInfo( PrintWriter pw ){

      for( Map.Entry<String, AliasEntry> aliasEntry : _aliasHash.entrySet() ){

          pw.println("<<<<< "+aliasEntry.getKey()+" >>>>>>>>>");
          aliasEntry.getValue().getInfo(pw);
      }
   }
   public String hh_ls_alias = "[<alias>]" ;
   public String ac_ls_alias_$_0_1( Args args )throws Exception{
      AliasEntry entry = null ;
      if( args.argc() == 0 ){
          StringBuffer sb     = new StringBuffer() ;
          for( Map.Entry<String, AliasEntry> aliasEntry : _aliasHash.entrySet()  ){
              sb.append( aliasEntry.getKey() ).append( " -> " ).
                 append( aliasEntry.getValue() ).append( "\n" ) ;
          }
          return sb.toString() ;
      }else{
          entry = _aliasHash.get( args.argv(0) ) ;
          if( entry == null )
             throw new Exception( "Alias not found : "+args.argv(0) ) ;
          return args.argv(0)+" -> "+entry ;
      }
   }
   public String hh_unset_alias = "<aliasName>" ;
   public String ac_unset_alias_$_1( Args args ){

      _aliasHash.remove( args.argv(0) ) ;
      return "Done" ;
   }
   public String hh_set_alias =
        "<aliasName> directory|class|context <specification>" ;

   public String fh_set_alias =
      "set alias <alias>  <type> [<typeSpecific> <...>]\n"+
      "    <type>         <specific> \n"+
      "   directory    <fullDirectoryPath>\n"+
      "   file         <fullFilePath> <arguments> <...>\n"+
      "   class        <fullClassName>\n"+
      "   context      [options] <context> or  <contextNameStart>*\n" +
      "                  options : -overwrite=<alias> -onError=<alias>\n" +
      "       predefined alias : <home>     =  default for http://host:port/ \n" +
      "                          <default>  =  default for any type or error \n" ;

   public String ac_set_alias_$_3_16( Args args )throws Exception{

      String alias = args.argv(0) ;
      String type  = args.argv(1) ;
      String spec  = args.argv(2) ;


      if( type.equals("directory") ||
          type.equals("file")         ){

         File dir = new File( spec ) ;
         if( ( ! dir.isDirectory() ) &&
             ( ! dir.isFile()      )    )
            throw new
            Exception( "Directory/File not found : "+spec ) ;

         _aliasHash.put(  alias ,
                          new AliasEntry( "directory" ,
                                          dir ,
                                          spec          ) ) ;
         return alias+" -> directory("+spec+")" ;

      }else if( type.equals( "context" ) ){

         int pos = spec.indexOf("*") ;
         if( pos > -1 )spec = spec.substring(0,pos) ;

         AliasEntry entry = new AliasEntry( type , spec , spec ) ;

         String     tmp  = args.getOpt( "onError" ) ;
         if( tmp != null )entry.setOnError( tmp ) ;

         tmp  = args.getOpt( "overwrite" ) ;
         if( tmp != null )entry.setOverwrite( tmp ) ;


         _aliasHash.put( alias , entry ) ;
         return alias+" -> context("+spec+")" ;

      }else if( type.equals( "class" ) ){

         int    argcount = args.argc() - 3 ;
         String []   arg = new String[argcount] ;
         StringBuffer sb = new StringBuffer() ;
         sb.append( "class="+spec ) ;
         for( int i = 0 ; i < argcount ; i++ ){
             arg[i] = args.argv(3+i) ;
             sb.append( ";" ).append(arg[i]) ;
         }

         HttpResponseEngine engine=null;
         String intFailureMsg=null, retMsg;

         try {
        	 engine  = invokeHttpEngine( spec , arg ) ;
        	 retMsg = alias+" -> class("+sb.toString()+")";
         } catch( ClassNotFoundException e) {
        	 type = "badconfig";
        	 intFailureMsg = "failed to load class "+spec;
        	 retMsg = alias+" -> class("+sb.toString()+")  FAILED TO LOAD CLASS";
         }

         AliasEntry aliasEntry = new AliasEntry( type , engine , sb.toString());
         _aliasHash.put( alias , aliasEntry) ;

         if( engine == null)
        	 aliasEntry.setIntFailureMsg( intFailureMsg);

         return retMsg;

      }else if( type.equals( "cell" ) ){

         _aliasHash.put( alias , new AliasEntry( type , spec , spec ) ) ;
         return "" ;

      }
      throw new Exception( "Unknown type : "+type ) ;
   }
   private HttpResponseEngine invokeHttpEngine( String className , String [] a )
           throws Exception {

       Class     c         = Class.forName( className ) ;
       Class  [] argsClass = null ;
       Object [] args      = null ;
       Constructor constr  = null ;
       //
       // trying to find a contructor
       //   <init>( CellNucleus nucleus , String [] args )
       //   <init>( String [] args )
       //   <init>( )
       //
       HttpResponseEngine engine = null ;
       try{
          argsClass    = new Class[2] ;
          argsClass[0] = dmg.cells.nucleus.CellNucleus.class ;
          argsClass[1] = java.lang.String[].class ;
          constr  = c.getConstructor( argsClass ) ;
          args    = new Object[2] ;
          args[0] = getNucleus() ;
          args[1] = a ;
          engine = (HttpResponseEngine)constr.newInstance( args ) ;
       }catch( Exception e ){
          try{
             argsClass    = new Class[1] ;
             argsClass[0] = java.lang.String[].class ;
             constr  = c.getConstructor( argsClass ) ;
             args    = new Object[1] ;
             args[0] = a ;
             engine = (HttpResponseEngine)constr.newInstance( args ) ;
          }catch( Exception ee ){
             argsClass    = new Class[0] ;
             constr  = c.getConstructor( argsClass ) ;
             args    = new Object[0] ;
             engine =  (HttpResponseEngine)constr.newInstance( args ) ;
          }
       }
       addCommandListener(engine);
       return engine ;
   }

   @Override
   public void run(){
       Socket socket=null;
       try {
           while( true ){
               _serial ++ ;
               socket = _listener.accept();
               _log.info( "Connection ("+_serial+") from : "+socket ) ;

               new Thread(
                       new HtmlService( _serial , socket ) ,
                       socket.getInetAddress().toString()
               ).start() ;
           }
       } catch(SocketException e) {
           /* We get a SocketException when the ServerSocket closes, and that's the only
            * way to get out of the accept.  So we must ignore SocketExceptions.
            */
       } catch(IOException e) {
           _log.warn("Problem in connection from "+socket+" : " + e);
       }
       _log.info("Listener Done" );
   }

   private class HtmlService
           implements Runnable, HttpRequest {

       private InputStream    _in ;
       private OutputStream   _out ;
       private BufferedReader _br ;
       private PrintWriter    _pw ;
       private Socket         _socket  ;
       private int            _serial ;
       private Map<String,String> _map = CollectionFactory.newHashMap();
       private String    []   _tokens ;
       private int            _tokenOffset = 1 ;
       private boolean        _isDirectory = false ;
       private String         _contentType = "text/html" ;
       private String         _userName = null ;
       private String         _password = null ;
       private boolean        _authDone = false ;
       //
       // the HttpRequest interface
       //
       public void    setContentType( String type ){ _contentType = type ; }
       public Map<String,String> getRequestAttributes(){ return _map ; }
       public OutputStream getOutputStream(){ return _out ; }
       public PrintWriter  getPrintWriter(){ return _pw ; }
       public String []    getRequestTokens(){ return _tokens ; }
       public int          getRequestTokenOffset(){ return _tokenOffset ; }
       public boolean      isDirectory(){ return _isDirectory ; }
       private synchronized void      doAuthorization(){
          if( _authDone )return ;
          _authDone = true ;
          String auth = (String)_map.get("Authorization") ;
          if( auth == null )return ;
          StringTokenizer st = new StringTokenizer(auth) ;
          if( st.countTokens() < 2 )return ;
          if( ! st.nextToken().equals("Basic") )return ;
          auth = new String( Base64.decode( st.nextToken() ) ) ;
          _log.info( "Authentication : >"+auth+"<" ) ;
          st = new StringTokenizer(auth,":") ;
          if( st.countTokens() < 2 )return ;
          _userName = st.nextToken() ;
          _password = st.nextToken() ;
       }
       public boolean isAuthenticated(){
          doAuthorization() ;
          return _userName != null ;
       }
       public String getUserName(){ doAuthorization() ; return _userName ; }
       public String getPassword(){ doAuthorization() ; return _password ; }
       //
       //
       private HtmlService( int serial , Socket socket ) throws IOException {

          _socket = socket ;
          _serial = serial ;
          _in  = socket.getInputStream() ;
          _out = socket.getOutputStream() ;
          _br  = new BufferedReader(
                      new InputStreamReader( _in ) ) ;
          _pw  = new PrintWriter(
                      new OutputStreamWriter( _out ) ) ;
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

       public void run(){
          try{
             String request = _br.readLine() ;
             if( request == null ){
                _log.warn( "No request line found" ) ;
                throw new Exception( "Nothing requested" ) ;
             }
             _log.info( "Request line  : "+request ) ;
             //
             // split the request attributes
             //
             String x = null ;
             while( ( x = _br.readLine() ) != null ){
                _log.info( "Request line(x) : "+x ) ;
                if( x.length() == 0 )break ;
                int n = x.indexOf(':') ;
                if( n < 0 )continue ;
                String key = x.substring(0,n) ;
                String value = n == x.length() - 1 ? "" : x.substring(n+1).trim() ;
                _map.put(key,value);
             }

             StringTokenizer st = new StringTokenizer( request ) ;
             String direction   = st.nextToken() ;

             if( ! direction.equals( "GET" ) )
                  throw new
                  HttpException( HttpStatus.NOT_IMPLEMENTED, "Not Implemented : "+direction ) ;

             String destination = st.nextToken() ;

             splitUrl( destination  ) ;

             AliasEntry  entry  = null ;
             String      alias  = null ;

             alias = _tokens.length == 0 ? "<home>" : _tokens[0] ;
             _tokenOffset = 1 ;
             try{
                entry = _aliasHash.get( alias ) ;
                if( entry == null )
                    throw new
                    HttpException( HttpStatus.NOT_FOUND , "Alias not found : "+alias ) ;

                switchHttpType( entry ) ;

             }catch(HttpException ee){
                if( ee.getErrorCode() != HttpStatus.NOT_FOUND )throw ee ;
                entry = _aliasHash.get( "<default>" ) ;
                if( entry == null )throw ee ;
                switchHttpType( entry ) ;
             }

             _pw.flush() ;
          }catch( HttpException e ){
             printHttpException( e ) ;
             _log.warn( "Problem in HtmlService : "+e ) ;
          }catch( Exception ee ){
             printHttpException( new HttpException( HttpStatus.BAD_REQUEST , "Bad Request : "+ee ) ) ;
             _log.warn( "Problem in HtmlService : "+ee ) ;
          }finally{
            try{ _out.close() ; }catch( IOException eeee ){}
          }
          _log.info( "Finished" ) ;

       }
       private void switchHttpType( AliasEntry entry ) throws Exception {

          String type = entry.getType() ;

          if( type.equals( "badconfig")) {
        	  StringBuilder sb = new StringBuilder();

        	  sb.append( "HTTP Server badly configured");
        	  if( entry.getIntFailureMsg() != null) {
        		  sb.append( ": ");
        		  sb.append( entry.getIntFailureMsg());
        	  }
        	  sb.append( ".");

        	  throw new HttpException(  HttpStatus.INTERNAL_SERVER_ERROR , sb.toString());

          } else if( type.equals( "directory" ) ){

             sendFile( (File)entry.getSpecific() , _tokens ) ;

          }else if( type.equals( "context" ) ){

             String     aliasString = null ;
             AliasEntry aliasEntry  = null ;
             //
             //  are we overwritten ?
             //
             if( ( ( aliasString = entry.getOverwrite() ) != null ) &&
                 ( ( aliasEntry = _aliasHash.get( aliasString ) ) != null )){

                 switchHttpType( aliasEntry ) ;
                 return ;

             }
             String html = null ;
             String specificName = (String)entry.getSpecific() ;
             if( ( _tokens.length > 1 ) && ( _tokens[1].equals("index.html") ) ){
                 html = createContextDirectory() ;
             }else{
                 if( _tokens.length > 1 ){

                    String contextName = _tokens[1] ;
                    if( ! contextName.startsWith( specificName ) )
                       throw new
                       HttpException( HttpStatus.FORBIDDEN , "Forbidden" ) ;

                    specificName = contextName ;
                 }
                 html = (String)_context.get( specificName ) ;
             }
             if( html == null ){
                if( ( ( aliasString = entry.getOnError() ) == null ) ||
                    ( ( aliasEntry = _aliasHash.get( aliasString ) ) == null ) )
                 throw new
                 HttpException( HttpStatus.NOT_FOUND , "Not found : "+specificName);

                switchHttpType( aliasEntry ) ;
                return ;
             }

             printHttpHeader(0) ;
             _pw.println( html ) ;

          }else if( type.equals( "class" ) ){

             HttpResponseEngine engine =  (HttpResponseEngine)entry.getSpecific() ;

             try {
            	 engine.queryUrl( this ) ;
             } catch( HttpException e) {
            	 throw e;
             } catch( Exception e) {
            	 throw new HttpException( HttpStatus.INTERNAL_SERVER_ERROR, "HttpResponseEngine ("+engine.getClass().getCanonicalName()+") is broken, please report this to sysadmin.");
             }

          }
          return ;
       }
       private String createContextDirectory(){
           StringBuffer sb = new StringBuffer() ;
           sb.append("<html><title>Context directory</title>\n");
           sb.append("<body bgcolor=\"#0088dd\">\n");
           sb.append("<h1>Context Directory</h1>\n");
           sb.append("<blockquote>\n");
           sb.append("<center>\n");
           sb.append("<table border=1 cellspacing=0 cellpadding=4 width=\"%90\">\n");
           sb.append("<tr><th>Context Name</th><th>Class</th><th>Content</th></tr>\n");
           SortedMap<String,Object> map = CollectionFactory.newTreeMap();
           map.putAll(_context);
           for( Iterator n = map.entrySet().iterator() ; n.hasNext() ; ){
               Map.Entry e = (Map.Entry)n.next() ;
               String key  = (String)e.getKey()  ;
               Object o    = e.getValue() ;
               String str  = o.toString() ;
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
           sb.append("<address>Created : ").append(new Date().toString()).append("</address>\n");
           sb.append("</body></html>");
           return sb.toString();
       }
       private String htmlToRegularString( String str ){
           StringBuffer sb = new StringBuffer() ;
           for( int i = 0 , n = str.length() ; i<n; i++ ){
               char c = str.charAt(i);
               switch(c){
                   case '<' : sb.append("&lt;"); break ;
                   case '>' : sb.append("&gt;"); break ;
                   case '\n' : sb.append("\\n"); break ;
                   default : sb.append(c);
               }
           }
           return sb.toString();
       }
       private void sendFile( File base , String [] tokens )throws Exception{

           File f = null ;

           String filename = null ;
           if(  tokens.length < 2 ){
              filename = "index.html" ;
           }else{
              StringBuffer sb  = new StringBuffer() ;
              sb.append( tokens[1] ) ;
              for( int i = 2 ; i < tokens.length ; i++ )
                 sb.append( "/" ).append( tokens[i] ) ;
              filename = sb.toString() ;
           }
           f = base.isFile() ? base : new File( base , filename ) ;
           if( ! f.isFile() )
              throw new Exception( "Url Not found : "+f ) ;

           FileInputStream binary = new FileInputStream(f);
           try {
               int rc = 0;
               byte[] buffer = new byte[4 * 1024];
               printHttpHeader((int)f.length(), getContentTypeFor(filename));
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
       public void printHttpHeader( int size ){
          printHttpHeader( size , _contentType ) ;
       }
       public void printHttpHeader( int size , String contentType ){
          String dateString = _dateFormat.format(new Date());
          _pw.println( "HTTP/1.0 200 Document follows" );
          _pw.println( "MIME-Version: 1.0" ) ;
          _pw.println( "Server: Java Cell Server" ) ;
          _pw.println( "Date: "+dateString ) ;
          _pw.println( "Content-Type: "+contentType ) ;
          if( size > 0 )_pw.println( "Content-Length: "+size ) ;
          _pw.println( "Last-Modified: "+dateString ) ;
          _pw.println( "Expires: "+dateString ) ;
          _pw.println( "");
          _pw.flush() ;
       }
       public void printHttpException( HttpException exception ){
          _log.info( "Starting printHttpException");
          String dateString = _dateFormat.format(new Date());
          _pw.println( "HTTP/1.0 "+exception.getErrorCode()+" "+exception.getMessage() );
          _pw.println( "MIME-Version: 1.0" ) ;
          _pw.println( "Server: Java Cell Server" ) ;
          _pw.println( "Date: "+dateString ) ;
          _pw.println( "Content-Type: text/html" ) ;
          _pw.println( "Last-Modified: "+dateString) ;
          if( exception instanceof HttpBasicAuthenticationException ){
            _pw.println( "WWW-Authenticate: Basic realm=\""+
                      ((HttpBasicAuthenticationException)exception).getRealm()+
                      "\"" ) ;
          }
          _pw.println( "");
          _pw.println( "<html><head><title>Exception occured</title></head>");
          _pw.println( "<h1><font color=red>"+
                       "An internal error occured"+
                       "</font></h1>" ) ;
          _pw.println( "<h4>The Exception was : <font color=red>"+
                       exception.getErrorCode()+"</font></h4>" ) ;
          _pw.println( "<h4>The message was : </h4>" ) ;
          _pw.println( "<pre>" ) ;
          _pw.println( exception.getMessage() ) ;
          _pw.println( "</pre>" ) ;
          _pw.println( "</body></html>");
          _pw.flush() ;
          _log.info("printHttpException done");
       }
      private final static int S_IDLE = 0 ;
      private final static int S_COPY = 1 ;
      private final static int S_HEX  = 2 ;
      private void splitUrl( String url ){
         int state       = S_IDLE ;
         int hex_count   = 0 ;
         StringBuffer sb = new StringBuffer() ;
         StringBuffer hexsb = new StringBuffer() ;
         List<String>       v  = new ArrayList<String>() ;
         char c ;
         boolean isDirectory = false ;

         for( int pos = 0 ; pos <= url.length() ; pos ++ ){
            c = pos == url.length() ? '\0' : url.charAt(pos) ;
            switch( state ){

               case S_IDLE :
                  if( c == '/' ){
                    state = S_COPY ;
                  }else if( c == '%' ){
                    state = S_HEX ;
                    hex_count = 0 ;
                  }else if( c == '\0' ){
                    isDirectory = true ;
                    // nothing
                  }else {
                    sb.append( c ) ;
                    state = S_COPY ;
                  }
               break ;
               case S_COPY :
                  if( c == '/' ){
                    if( sb.length() > 0 ){
                       v.add( sb.toString() ) ;
                    }
                    sb.setLength(0) ;
                    state = S_IDLE ;
                  }else if( c == '%' ){
                    state = S_HEX ;
                    hex_count = 0 ;
                  }else if( c == '\0' ){
                    if( sb.length() > 0 ){
                       v.add( sb.toString() ) ;
                    }
                    sb.setLength(0) ;
                    state = S_IDLE ;
                  }else{
                     sb.append( c ) ;
                  }
               break ;
               case S_HEX :
                  hexsb.append( c ) ;
                  if( hex_count++ > 0 ){
                     int value = 0 ;
                     try{
                        value = Integer.parseInt( hexsb.toString() , 16 ) ;
                     }catch( NumberFormatException nfe ){
                        value = ' ' ;
                     }
                     sb.append( (char)value ) ;
                     hexsb.setLength(0) ;
                     state = S_COPY ;
                  }
               break ;

            }

         }

         _tokens      = v.toArray( new String[v.size()]) ;
         _isDirectory = isDirectory ;

         return  ;

      }
   }
   public static void printException33( PrintWriter pw , Exception ee ){
//      printDummyHttpHeader( pw ) ;
      pw.println( "<html><head><title>Exception occured</title></head>");
      pw.println( "<h1><font color=red>"+
                   "An internal error occured"+
                   "</font></h1>" ) ;
      pw.println( "<h4>The Exception was : <font color=red>"+
                   ee.getClass().getName()+"</font></h4>" ) ;
      pw.println( "<h4>The message was : </h4>" ) ;
      pw.println( "<pre>" ) ;
      pw.println( ee.getMessage() ) ;
      pw.println( "</pre>" ) ;
      pw.println( "</body></html>");
   }
   private void printTitlePage33( PrintWriter pw ){
//      printDummyHttpHeader( pw ) ;
      pw.println("<html>") ;
      pw.println("<head><title>Cell Http Server</title></head>") ;
      pw.println("<body bgcolor=green>" ) ;
      pw.println( "<center><h1>"+
                  "Welcome to the CellHttpServer 0.1.0"+
                  "</h1></center>" ) ;
      pw.println("</body></html>") ;
      pw.flush() ;
   }


}
