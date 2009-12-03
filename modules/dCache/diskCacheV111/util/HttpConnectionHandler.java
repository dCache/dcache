package diskCacheV111.util;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Hashtable;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

public class HttpConnectionHandler
{

  private static final Logger _log = Logger.getLogger(HttpConnectionHandler.class);

  public static final String HTTP09 = "HTTP/0.9";
  public static final String HTTP10 = "HTTP/1.0";
  public static final String HTTP11 = "HTTP/1.1";
  private Map<String, String> httpheaders;
  private Socket connection;
  private BufferedReader in;
  private OutputStream outstream;
  private BufferedWriter out;
  private String  method;
  private String request_url_string;
  private URL request_url;
  private String http_version;
  private int error = 0;
  private String error_string = null;
  private boolean connectionclosed = false;
  private boolean processed_header = false;

  public HttpConnectionHandler(Socket httpconnection) throws IOException
  {
    this( new BufferedReader(
                new InputStreamReader(httpconnection.getInputStream ())) ,
          httpconnection.getOutputStream() ,
          new BufferedWriter(
                new OutputStreamWriter(httpconnection.getOutputStream())));
    connection = httpconnection;
    _log.debug("HttpConnectionHandler created with Socket = "+httpconnection);
  }

  public HttpConnectionHandler(BufferedReader in,
                               OutputStream outstream,
                               BufferedWriter out) throws IOException
  {
    _log.debug("HttpConnectionHandler created with streams");
    this.in =in;
    this.outstream = outstream;
    this.out = out;
    //(this.timeout_thread = new Thread(this)).start();
    _log.debug("parsing ...");
    boolean parsed = parseRequest();
    _log.debug("parsed ...");
    if(!parsed)
    {
       _log.debug("parseRequest returned false,returning error header ...");
       returnErrorHeader();
    }
    else
    {
       _log.debug("parseRequest returned true");
    }
  }

  public String[] getHeaders()
  {
    if(isclosed())
    {
      return null;
    }
    int size = httpheaders.keySet ().size ();
    String[] headers = new String[size];
    return httpheaders.keySet().toArray(headers);
  }

  public String getHeaderValue(String header)
  {
    if(isclosed())
    {
      return null;
    }
    return httpheaders.get(header);
  }

  public synchronized String getHttpMethod()
  {
    if(isclosed())
    {
      return null;
    }
    return method;
  }

  public synchronized String getUrlString()
  {
    if(isclosed())
    {
      return null;
    }
    return request_url_string;
  }

  public synchronized URL getUrl()
  {
    if(isclosed())
    {
      return null;
    }
    return request_url;
  }

  public String getHttpVersion()
  {
    return http_version;
  }

  public synchronized void returnErrorHeader(String errorString) throws IOException
  {
    if(isclosed())
    {
      return;
    }
    StringBuffer sb = new StringBuffer();
    if(this.error == 0)
    {
      this.error = 400;
    }

    sb.append(HTTP11).append(' ').append(this.error);
    sb.append(' ').append(errorString).append('\n');
    sb.append("Connection: close");
    sb.append('\n').append('\n');
    out.write(sb.toString());
    out.flush();
    closeconnection();
  }

  public synchronized void closeconnection() throws IOException
  {
    if(isclosed() )
    {
      return;
    }
    in.close();
    out.close();
    if(connection != null)
    {
      connection.close();
    }
    in = null;
    out = null;
    outstream = null;
    connection = null;
    connectionclosed = true;
  }


  public synchronized boolean isclosed()
  {
    return connectionclosed;
  }

  public synchronized void returnRedirectHeader(String redirectURI) throws IOException
  {
    if(isclosed())
    {
      return;
    }
    StringBuffer sb = new StringBuffer();
    sb.append(HTTP11).append(" 302 Redirected\n");
    sb.append("Server: HttpDoor \n");
    sb.append("Location: ").append(redirectURI).append('\n');
    sb.append("Connection: close");
    sb.append('\n').append('\n');
    if(_log.isTraceEnabled()){
        _log.trace("returning header :"+sb.toString());
    }
    out.write(sb.toString());
    _log.trace("returned");
    out.flush();
    closeconnection();
  }


  private synchronized boolean parseRequest()
  {

    if(isclosed())
    {
      return false;
    }

    String line;

    try
    {
      // Read the first line of the request.
      line = in.readLine();
      _log.debug("parsing line "+line);
      if ( line == null || line.length() == 0 )
      {
        error = HttpURLConnection.HTTP_BAD_REQUEST;
        error_string = "Empty request";
        _log.warn("error: Empty request");
        return false;
      }

      StringTokenizer st = new StringTokenizer(line);
      int count = st.countTokens ();
      if( count <2 || count >3)
      {
        error = HttpURLConnection.HTTP_BAD_REQUEST;
        error_string = "bad request";
        _log.warn("error: bad request, count = "+count);
        return false;
      }
      this.method = st.nextToken();
      if(!method.equals("OPTIONS") &&
         !method.equals("GET")     &&
         !method.equals("HEAD")    &&
         !method.equals("POST")    &&
         !method.equals("PUT")     &&
         !method.equals("DELETE")  &&
         !method.equals("TRACE")   &&
         !method.equals("CONNECT")    )
      {
        error = HttpURLConnection.HTTP_BAD_REQUEST;
        error_string = "bad request, unknown method: "+method;
         _log.warn("bad request, unknown method: "+method);
        return false;
      }
      this.request_url_string = st.nextToken();

      if(count == 2)
      {
        http_version = HTTP09;
      }
      else
      {
        String ver = st.nextToken();
        if(ver.toUpperCase ().equals (HTTP10))
        {
          http_version = HTTP10;
        }
        else
        {
          http_version = HTTP11;
        }
      }

      this.httpheaders = new Hashtable<String, String>();

      while ( true )
      {
        line = in.readLine();
         _log.debug("parsing line: "+line);
        if ( line == null || line.length() == 0 )
        {
          break;
        }
        int colonBlank = line.indexOf( ':' );
        if ( colonBlank != -1 )
        {
          String name = line.substring( 0, colonBlank ).trim();
          String value = line.substring( colonBlank + 1 ).trim();
          httpheaders.put(name.toLowerCase(),value);
        }
      }

      // Check Host: header in HTTP/1.1 requests.
      if ( http_version.equals( "HTTP/1.1") )
      {
        String host = httpheaders.get( "host" );
        if ( host == null )
        {
          error = java.net.HttpURLConnection.HTTP_BAD_REQUEST;
          error_string = "bad request, host header is missing";
          _log.warn("error: bad request, host header is missing");
          return false;
        }
      }

      request_url_string = HttpConnectionHandler.decode( request_url_string );
      if(request_url_string.indexOf(':') <0)
      {
          request_url_string = "file:///".concat(request_url_string);
      }

      this.request_url = new URL(request_url_string);

      return true;
    } catch(MalformedURLException e) {
      _log.warn("Bad request: " + e.getMessage() );
      error = java.net.HttpURLConnection.HTTP_BAD_REQUEST;
      error_string = e.getMessage();
    } catch(IOException e) {
      _log.warn("Failed to read request: " + e.getMessage());
      error = java.net.HttpURLConnection.HTTP_BAD_REQUEST;
      error_string = e.getMessage();
    }
    finally
    {
      synchronized(this)
      {
        processed_header  = true;
        notify();
      }
    }

    return false;
  }


  private synchronized void returnErrorHeader() throws IOException
  {
    if(connectionclosed)
    {
      return;
    }
    StringBuffer sb = new StringBuffer();
    if(this.error == 0)
    {
      this.error = 400;
    }

    sb.append(HTTP11).append(' ').append(this.error);
    sb.append(' ').append(this.error_string).append('\n');
    sb.append("Connection: close");
    sb.append('\n').append('\n');
    out.write(sb.toString());
    out.flush();
    closeconnection();
  }



  private synchronized void returnFileHeader(RandomAccessFile diskFile) throws IOException
  {
    if(isclosed())
    {
      return;
    }
    StringBuffer sb = new StringBuffer();
    sb.append(HTTP11).append(" 200 OK\n");
    sb.append("Server: HttpDoor \n");
    sb.append("Connection: close\n");
    sb.append("Content-Length: ");
    sb.append(diskFile.length ()).append('\n');
    sb.append("Content-Type: application/octet-stream\n");
    sb.append('\n');
    out.write(sb.toString());
    out.flush();
  }

  private synchronized void returnPartialFileHeader(RandomAccessFile diskFile, long offset, long length) throws IOException
  {
    if(isclosed())
    {
      return;
    }
    StringBuffer sb = new StringBuffer();
    sb.append(HTTP11).append(" 206 Partial Content\n");
    sb.append("Server: HttpDoor \n");
    sb.append("Connection: close\n");
    sb.append("Content-Length: ");
    sb.append(length).append('\n');
    sb.append("Content-Range: ");
    sb.append("bytes ").append(offset).append('-').append(offset+length-1).append("/").append(diskFile.length()).append('\n');
    sb.append("Content-Type: application/octet-stream\n");
    sb.append('\n');
    out.write(sb.toString());
    out.flush();
  }

  volatile long read = 0;
  private long last_transfer_time    = System.currentTimeMillis() ;
  public synchronized void sendFile(RandomAccessFile diskFile) throws IOException
  {
    if(isclosed())
    {
      return;
    }
    returnFileHeader(diskFile);
    long length = diskFile.length ();
    transferFile(diskFile,0,length);
  }

  public synchronized void sendPartialFile(RandomAccessFile diskFile, long offset, long length) throws IOException
  {
     if(isclosed())
     {
       return;
     }
     returnPartialFileHeader(diskFile, offset, length);
     transferFile(diskFile, offset, length);
  }

  private void transferFile(RandomAccessFile diskFile, long offset, long length) throws IOException
  {
    byte[] b = new byte[1024];
    diskFile.seek(offset);
    while(read <length)
    {
      int readlen =(int)( length -read <1024?length -read:1024);
      readlen = diskFile.read(b,0,readlen);
      outstream.write(b,0,readlen);
      last_transfer_time    = System.currentTimeMillis() ;
      read += readlen;
    }
    outstream.flush();
    closeconnection();
  }

  public long transfered()
  {
      return read;
  }
  public long getLast_transfer_time()
  {
      return last_transfer_time;
  }

  private static String decode( String str )
  {
    StringBuffer sb = new StringBuffer();
    int length = str.length();
    for ( int i = 0; i < length; ++i )
    {
      char c = str.charAt( i );
      if ( c == '%' && i + 2 < length )
      {
        char c1 = str.charAt( i + 1 );
        char c2 = str.charAt( i + 2 );
        if ( isHexDigit( c1 ) && isHexDigit( c2 ) )
        {
          sb.append( (char) ( toHexDigit( c1 ) << 4 + toHexDigit( c2 ) ) );
          i += 2;
        }
        else
        {
          sb.append( c );
        }
      }
      else
      {
        sb.append( c );
      }
    }
    return sb.toString();
  }

  private static boolean isHexDigit( char c )
  {
    return (c >= '0' && c <= '9') ||
           (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
  }

  private static int toHexDigit( char c )
  {
    if ( c >= '0' && c <= '9' )
    {
        return c - '0';
    }
    else if ( c >= 'a' && c <= 'f' )
    {
        return c - 'a' + 10;
    }
    else if ( c >= 'A' && c <= 'F' )
    {
        return c - 'A' + 10;
    }
    else
    {
      throw new IllegalArgumentException(" character "+c+"is not a hex digit");
    }
  }

}
