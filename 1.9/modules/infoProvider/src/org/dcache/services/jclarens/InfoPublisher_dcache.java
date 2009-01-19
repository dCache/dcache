package org.dcache.services.jclarens;


import java.io.PrintWriter ;
import java.io.ObjectInputStream;
import java.net.URL;
import java.net.InetAddress;
import java.net.Socket;
import java.security.Security;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;

import dmg.cells.nucleus.* ;
import dmg.util.Args;
import rendezvous_pkg.*;
import org.dcache.services.infoCollector.GlueSchemaV1_2;


  /**InfoPublisher_dcache Cell.<br/>
   * This Cell publishes SRM information to a jClarens host.
   * @see org.dcache.services.infoCollector.InfoCollector
   **/
  public class InfoPublisher_dcache extends CellAdapter implements Runnable {

  /** Thread lauched by this Cell  **/
  private Thread  _publishThread = null ;

  /** run() enabler **/
  private boolean _continue = true ;

  /** Arguments specified in .batch file **/
  private Args _opt;

  /** The email address of the service administrator. **/
  private String _admin_email;

  /** The URL of the Clarens server where this publication will be sent. **/
  private String _clarens_url;

  /** The URL that client applications will use to contact this service. **/
  private String _service_url;

  /** The transport encoding (such as 'xmlrpc' or 'soap') that
  client applications will use to contact this service. **/
  private String _encoding;

  /** The name of this service, such as 'SRM'. **/
  private String _service_name;

  /** The VO that this service instance belongs to, such as
  'uscms'.  If it belongs to multiple VOs then you must
  register the service multiple times, once for each VO. **/
  private String _vo;

  /** The name of the facility that hosts the service. **/
  private String _site;

  /** The URL that points to the WSDL for this service. **/
  private String _wsdl_url;

  /** The Certificate subject for the server hosting this service. **/
  private String _server_dn;

  /** Describes the version number for the service. **/
  private String _version;

  /** The interval, in minutes, at which the service should
      be published to the Discovery Service. **/
  private long _interval = 5L;

  /** The length of time, in minutes, that
  this service should remain in the registry. If the service
  is not reregistered before this time then it will be
  automatically removed from the registry. Set by default
  to three times the registration interval.  **/
  private long _expiration = 3 * _interval;

  /** The full path to the file for the trusted certivicate authority.
  May contain a wildcard to specify several authorities. **/
  private String _sslCAFiles;

  /** The full path to the hostcert.pem host certificate file. **/
  private String _sslCertFile;

  /** The full path to the hostkey.pem host key file. **/
  private String _sslKey;

  /** If true, will ignore dynamic information. Default is false. **/
  private boolean _static_only;

  /** Support class for contacting publication service **/
  Rendezvous _rendezvous_port;

  /**
    * Constructor
    * This method creates a Cell by means the super Class
    * <code>CellAdapter</code>. It creates a Thread and starts it.<br>
    * 
    * @param name The name of the Cell, used in dCache
    * @param args The arguments passed when the Cell is instanceted
    * @throws Exception
  */

  public InfoPublisher_dcache( String name , String args )  throws Exception {

    super( name , args , false ) ;

    useInterpreter( true ) ;

    _opt = getArgs() ;


    try {wait(5000);} catch(Exception ie) {}


    try{

        /**
          *  USAGE :
          *              -admin_email=ADMIN_EMAIL
        */

      _admin_email = setParam("admin_email", _admin_email); //todo: use Opts instead.
      _clarens_url = setParam("clarens_url", _clarens_url);
      _service_url = setParam("service_url", _service_url);
      _encoding = setParam("encoding", _encoding);
      _service_name = setParam("service_name", _service_name);
      _vo = setParam("vo", _vo);
      _site = setParam("site", _site);
      _wsdl_url = setParam("wsdl_url", _wsdl_url);
      _server_dn = setParam("server_dn", _server_dn);
      _version = setParam("version", _version);
      _interval = setParam("interval", _interval);
      _expiration = setParam("expiration", _expiration);
      _sslCAFiles = setParam("sslCAFiles", _sslCAFiles);
      _sslCertFile = setParam("sslCertFile", _sslCertFile);
      _sslKey = setParam("sslKey", _sslKey);

      _static_only = setParam("static_only", new String()).equals("true") ? true : false;

      System.setProperty("axis.socketSecureFactory", "org.glite.security.trustmanager.axis.AXISSocketFactory");
      System.setProperty("sslCAFiles", _sslCAFiles);
      System.setProperty("sslCertFile", _sslCertFile);
      System.setProperty("sslKey", _sslKey);

      Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());


      RendezvousService rendezvous_service = new RendezvousServiceLocator();
     	// Now use the service to get a stub which implements the SDI.
      URL destination = new URL(_clarens_url);
	    _rendezvous_port = rendezvous_service.getrendezvous(destination);

      _publishThread  = new Thread( this ) ;
      _publishThread.start() ;

    } catch( Exception iae ){
      esay("InfoPublisher_dcache couldn't start due to "+iae);
      esay(iae);
      start() ;
      kill() ;
      throw iae ;
    }

    // Should be done with cell commands.
    setPrintoutLevel( CellNucleus.PRINT_CELL | CellNucleus.PRINT_ERROR_CELL );

    start() ;

	  say(" Constructor finished" ) ;
   }

   /**
     * is called if user types 'info'
     */
 public void getInfo( PrintWriter pw ){
   super.getInfo(pw);
   pw.println(" admin_email :  " + _admin_email);
   pw.println(" clarens_url :  " + _clarens_url);
   pw.println(" service_url :  " + _service_url);
   pw.println(" encoding :  " + _encoding);
   pw.println(" service_name :  " + _service_name);
   pw.println(" vo :  " + _vo);
   pw.println(" site :  " + _site);
   pw.println(" wsdl_url :  " + _wsdl_url);
   pw.println(" server_dn :  " + _server_dn);
   pw.println(" version :  " + _version);
   pw.println(" expiration :  " + _expiration);
   pw.println(" interval :  " + _interval);
   pw.println(" sslCAFiles :  " + _sslCAFiles);
   pw.println(" sslCertFile :  " + _sslCertFile);
   pw.println(" sslKey :  " + _sslKey);
 }

   /**
    * This method is called from finalize() in the CellAdapter
    * super Class to clean the actions made from this Cell.
    * It stops the Thread created.
    */
   public void cleanUp(){
  
      say(" Clean up called ... " ) ;
      synchronized( this ){ 
    	 _continue = false ;
         notifyAll() ; 
      }
      say( " Done" ) ;
   }
   
   /**
    * This method is invoked when a message arrives to this Cell
    * @param msg CellMessage
    */
   public synchronized void messageArrived( CellMessage msg ){
       
       UOID uoid  = msg.getLastUOID();
       Object obj = msg.getMessageObject() ;
       
   }
   
   public String hh_report = "" ;
   /**
    * Command 'report'.<br/>
    * Show all last srm information collected in a human-readable form.
    * @param args - any
    * @return report
    * @throws Exception
    */
   public String ac_report_$_0( Args args ) throws Exception {
       //return _inf.show();
       return "";
   }

   /** Set a parameter according to option specified in .batch config file **/
   private String setParam(String name, String target) {
    if(target==null) target = new String();
    String option = _opt.getOpt(name) ;
    if((option != null) && (option.length()>0)) target = option;
    say("Using " + name + " : " + target);
    return target;
   }

   /** Set a parameter according to option specified in .batch config file **/
   private long setParam(String name, long target) {
    String option = _opt.getOpt(name) ;
    if( ( option != null ) && ( option.length() > 0 ) ) {
      try{ target = Integer.parseInt(option); } catch(Exception e) {}
    }
    say("Using " + name + " : " + target);
    return target;
   }

   /**
    * This thread send periodically the commands needed to obtain
    * the information from the cells. 
    **/
  public void run(){

    /** Holds all of the service information for registration **/
    ServiceDescriptor serviceDescriptor;

    long interval = this._interval * 60 * 1000;  // Can come from .batch. "wait" is in milliseconds.
    long expiration = this._expiration * 60;     // Web service expire time is in minutes.

    if( Thread.currentThread() == _publishThread ){
      synchronized( this ) {
        while( _continue ) {

          GlueSchemaV1_2 schema = getGlueSchemaV1_2Object();

          if(_static_only || schema == null) {
            serviceDescriptor = getDefaultServiceDescriptor();
            registerService(serviceDescriptor, expiration);
          }

          if(!_static_only && schema != null && schema.se.access_protocol != null)
			    for(int i=0; i<schema.se.access_protocol.length; i++) {
            GlueSchemaV1_2.AccessProtocol ap = schema.se.access_protocol[i];
            serviceDescriptor = getAccessServiceDescriptor(ap);
            appendSEinfo(schema.se, serviceDescriptor);
            registerService(serviceDescriptor, expiration);
          }

          if(!_static_only && schema != null && schema.se.control_protocol != null)
			    for(int i=0; i<schema.se.control_protocol.length; i++) {
            GlueSchemaV1_2.ControlProtocol ap = schema.se.control_protocol[i];
            serviceDescriptor = getControlServiceDescriptor(ap);
            appendSEinfo(schema.se, serviceDescriptor);
            registerService(serviceDescriptor, expiration);
          }

          try {
              wait( interval ) ;
          } catch( InterruptedException ie ) {
            break ;
          }

        }
      }

    kill() ;
    }
  }

  /**
    * Connects to InfoExporter and reads the GlueSchemaV1_2 object.
  **/
  public GlueSchemaV1_2 getGlueSchemaV1_2Object() {

    GlueSchemaV1_2 schema = null;
		String port = setParam("port", "22111");
		//if( Opts.get("port")!=null ) port=Opts.get("port");
		try{
			//InetAddress addr = InetAddress.getByName(Opts.get("host"));   // will use localhost if host is not specified.
      String host = setParam("host", "localhost");
      InetAddress addr = InetAddress.getByName(host);
			Socket socket = new Socket(addr,Integer.parseInt(port));

			ObjectInputStream in =
				new ObjectInputStream(
						socket.getInputStream());

			Object obj = in.readObject();
			schema = (GlueSchemaV1_2) obj;

			in.close();
			socket.close();
		}catch(Exception e){
			e.printStackTrace(System.out);
		}

    return schema;
  }

  /**
    * Returns a ServiceDescriptor with info from settings in the batch file only.
  **/
  public ServiceDescriptor getDefaultServiceDescriptor() {

    ServiceDescriptor sd = new ServiceDescriptor();
    Provider endpt = new Provider();
    endpt.setUri(this._service_url);
    endpt.setEncoding(this._encoding);
    sd.setEndpoint(new Provider[] { endpt });

    sd.setAdmin(this._admin_email);
    sd.setName(this._service_name);
    sd.setVo(this._vo);
    sd.setSite(this._site);
    sd.setWsdl(this._wsdl_url);
    sd.setProvider_dn(this._server_dn);
    sd.setVersion(this._version);

    return sd;
  }

  /**
    * Returns a ServiceDescriptor with info from AccessProtocol of GlueSchemaV1_2
  **/
  public ServiceDescriptor getAccessServiceDescriptor(GlueSchemaV1_2.AccessProtocol ap) {

    ServiceDescriptor sd = new ServiceDescriptor();
    Provider endpt = new Provider();
    endpt.setUri(ap.endPoint);
    endpt.setEncoding(this._encoding);
    sd.setEndpoint(new Provider[] { endpt });

    sd.setAdmin(this._admin_email);
    sd.setName(ap.type);
    sd.setVo(this._vo);      //todo: Place VOs in item array
    sd.setSite(ap.localID);
    sd.setWsdl("");          //todo: refer to SRM wsdl ?
    sd.setProvider_dn(this._server_dn);  //todo: get DN from Security or other
    sd.setVersion(ap.version);

    return sd;
  }

  /**
    * Returns a ServiceDescriptor with info from a ControlProtocol of GlueSchemaV1_2
  **/
  public ServiceDescriptor getControlServiceDescriptor(GlueSchemaV1_2.ControlProtocol cp) {

    ServiceDescriptor sd = new ServiceDescriptor();
    Provider endpt = new Provider();
    endpt.setUri(cp.endPoint);
    endpt.setEncoding(this._encoding);
    sd.setEndpoint(new Provider[] { endpt });

    sd.setAdmin(this._admin_email);
    sd.setName(cp.type);
    sd.setVo(this._vo);      //todo: Place VOs in item array
    sd.setSite(cp.localID);
    sd.setWsdl("");          //todo: refer to SRM wsdl ?
    sd.setProvider_dn(this._server_dn);  //todo: get DN from Security or other
    sd.setVersion(cp.version);

    return sd;
  }

  /**
    * Adds infor from the SE fields of GlueSchemaV1_2 to a ServiceDescriptor
  **/
  public void appendSEinfo(GlueSchemaV1_2.StorageElement se, ServiceDescriptor sd) {
    if(se==null) return;
    ArrayList SDitemsArray;
    ServiceDescriptor_item[] sdItems = sd.getItem();
    SDitemsArray = (sdItems!=null) ? new ArrayList(Arrays.asList(sdItems)) : new ArrayList();

    addServiceDescriptorItem("Cache Size", se.sizeTotal, SDitemsArray);
    addServiceDescriptorItem("Available Size", se.sizeFree, SDitemsArray);

    sd.setItem((ServiceDescriptor_item[]) SDitemsArray.toArray(new ServiceDescriptor_item[1]));
  }

  /**
    * Fills a value in the arraylist of ServiceDescriptor_item's
  **/
  public void addServiceDescriptorItem(String key, String value, ArrayList SDitemsArray) {
      if(key==null || value==null) return;
      ServiceDescriptor_item sd_item;
      sd_item = new ServiceDescriptor_item();
      sd_item.setKey(key);
      sd_item.setValue(value);
      SDitemsArray.add(sd_item);
    }

  /**
    * Register the service described in the ServiceDescriptor using the Rendezvous service.
  **/
  public void registerService(ServiceDescriptor sd, long expiration) {
    say("try register" ) ;
    try{

      sd.setExpire(new Long(expiration + (new Date()).getTime() / 1000));

      int result = _rendezvous_port.register(new ServiceDescriptor[] {sd});
      if (result != 0) {
        System.err.println("Registration failed with code "+result);
      }
      } catch ( Exception ie ) {
        ie.printStackTrace();
    }

    say("done register" ) ;
  }

}
