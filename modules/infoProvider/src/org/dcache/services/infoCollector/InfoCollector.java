package org.dcache.services.infoCollector;


import java.io.PrintWriter ;
import dmg.cells.nucleus.* ;
import dmg.cells.services.login.*;
import dmg.util.Args;
import diskCacheV111.vehicles.* ;



/**Info-Collector Cell.<br/>
 * This Cell collects and keeps updated the dynamic information about
 * dCache resources and their state.
 * It periodically queries the other Cells to obtain the information.
 * When this cell need to obtain some information from the dCache, 
 * it sends a message to the cell that own them.
 * The cell queried replies at that message by another message returned
 * to the this Cell. So this cell recives the message and 
 * processes it to obtain the information. Finally it stores the 
 * information in a <code>InfoBase</code> object, that it istanciates.<br><br>
 * This class instantiates also an <code>InfoExporter</code> object to export
 * the information that it collects to some extern Information System.
 * 
 * @see dmg.cells.nucleus.CellAdapter
 * @see InfoBase
 **/
public class      InfoCollector 
       extends    CellAdapter 
       implements Runnable {

/** Thread lauched by this Cell  **/	
   private Thread   _sendThread   = null ;
/** Time interval between a message sending and the next**/
   private long     _interval     = 60000L ;   //1min 
/** run() enabler **/
   private boolean  _continue     = true ;

// Object Identifier references//   
/** Message's UOID in reply at 'xcm ls' command to PoolManager**/
   private UOID _id1;
/** Message's UOID in reply at 'psux ls pgroup' command to PoolManager**/
   private UOID _id2;
/** Array of message's UOID in reply at 'psux ls pgroup &lt;poolname&gt;' command 
 *  sent for each pool to PoolManager**/
   private UOID[] _id3;
/** Message's UOID in reply at 'ls -binary' command to LoginBroker **/
   private UOID _id1p;
/** Message's UOID in reply at 'ls -binary' command to srm-LoginBroker **/
   private UOID _id2p;
   
   
/** Point to InfoBase object (here created) where the information collected are organized **/   
   final private InfoBase _inf = new InfoBase();

/** Point to InfoBase object (here created) where the information collected are organized **/  
   private InfoExporter _exporter; 
   
/** This flag determines if the InfoExporter have to be created **/     
   private boolean _export = true;

/** This is the *ONLY* place to modify in the whole package when you decide
 *  to change the schema to adopt to export the information. It is the name of the
 *  Class (that extends Schema) used.
 **/    
   private String  _schema_type = "org.dcache.services.infoCollector.GlueSchemaV1_2";

/** A reference to link to the SchemaFiller specialization **/
   private SchemaFiller _schema_filler;   

   
   /**
    * Constructor
    * This method creates a Cell by means the super Class
    * <code>CellAdapter</code>. It creates a Thread and starts it.<br>
    * Here the <code>InfoExporter</code> and the <code>SchemaFiller</code> 
    * object is created. The <code>SchemaFiller</code> is created using
    * the RTTI so it doesn't change if the schema changes. 
    * 
    * @param name The name of the Cell, used in dCache
    * @param args The arguments passed when the Cell is instanceted
    * @throws Exception
    */
    public InfoCollector( String name , String args )  throws Exception {

        super( name , args , false ) ;

        useInterpreter( true ) ;

        try{

           /**
             *  USAGE :
             *              -schema=SCHEMA_TYPE
             *              -interval=SLEEP_IN_SECONDS   #  default = 60 seconds
             *              -listenPort=TCP_LISTEN_PORT  #  default = 22111 
             */
             
           Args opt = getArgs() ;
           String options = opt.getOpt("schema") ;
           if( ( options != null ) && ( options.length() > 0 ) )_schema_type = options ;

           say("Using schema type : "+_schema_type ) ;
           
           options = opt.getOpt("interval") ;
           if( ( options != null ) && ( options.length() > 0 ) ){
              try{
                 _interval = Integer.parseInt( options ) * 1000L ;
              }catch(Exception e){}
           }
           say("Using inteval : "+_interval+" msec");
           
           if(_export){
              _schema_filler = (SchemaFiller)Class.forName( _schema_type + "Filler" ).newInstance();
              _schema_filler.inf = _inf;  // InfoBase reference initialization in the SchemaFiller
	          _exporter = new InfoExporter(_schema_filler, this);
           }
           
           _sendThread  = new Thread( this ) ;
           _sendThread.start() ;

        /*
        }catch(ClassNotFoundException ce){  
            ce.printStackTrace(System.err);
        }catch(InstantiationException ie){
            ie.printStackTrace(System.err);
        }catch(IllegalAccessException ia){
            ia.printStackTrace(System.err);
        */
        }catch( Exception iae ){
           esay("InfoCollector couldn't start due to "+iae);
           esay(iae);
           start() ;
           kill() ;
           throw iae ;
        }

        /*
         * not needed, should be done with cell commands
         */
        setPrintoutLevel( CellNucleus.PRINT_CELL |
                          CellNucleus.PRINT_ERROR_CELL ) ;
        start() ;
	// say(" Constructor finished" ) ;
   }
   /**
     * is called if user types 'info'
     */
   public void getInfo( PrintWriter pw ){
        super.getInfo(pw);
        pw.println(" Schema Type :  "+_schema_type );
        pw.println(" Interval    :  "+_interval+" msec" );
   }
   /**
    * This method is called from finalize() in the CellAdapter
    * super Class to clean the actions made from this Cell.
    * It stops the Thread created.
    */
   public void cleanUp(){
  
      say(" Clean up called ... " ) ;
      if(_exporter != null) _exporter.cleanUp();
      synchronized( this ){ 
    	 _continue = false ;
         notifyAll() ; 
      }
      say( " Done" ) ;
   }
   
   /**
    * This method is invoked when a message arrives to this Cell
    * and it's not a command.
    * The messages that income to this cell it's aspected to be replies 
    * at messages from this same Cell, for this reason the incoming messages
    * are processed in base of the UOID of original message (if it's a reply).
    * As matter of facts, if we know why a message is sent (the command that it
    * carries in for a Cell), we know also we have to process the reply at that
    * message.  
    * The messages and the informations requested are:<ul>
    * <li>_idp1: an updated LoginBrokerInfo array for access protocols</li>
    * <li>_idp2: an updated LoginBrokerInfo array for srm protocols</li>
    * <li>_id1 : an updated PoolCostInfoByName object</li>
    * <li>_id2 : an array of every pool group's names</li>
    * <li>_id3 : an array of every pool and link's names of a given group</li> 
    * </ul>
    * If the message is an getPoolCostInfoByName object, the timestamp value in that 
    * object can be used to age the space information about the pools.
    * 
    * @see diskCacheV111.vehicles.CostModulePoolInfoTable
    * @see InfoBase
    * @see InfoExporter
    * @param msg CellMessage
    */
   public synchronized void messageArrived( CellMessage msg ){
       
       UOID uoid  = msg.getLastUOID();
       Object obj = msg.getMessageObject() ;
       
       /*
        * If the message riceved is the reply at 'ls -binary' to 
        * LoginBroker, it contains, as object, an updated array of 
        * LoginBrokerInfo object.
        */
       if( _id1p!=null && uoid.equals(_id1p) ){                              
    	   _inf.protocols = (LoginBrokerInfo[])obj;
  
       /*
        * If the message riceved is the reply at 'ls -binary' to 
        * srm-LoginBroker, it contains, as object, an updated array of 
        * LoginBrokerInfo object.
        */       
       }else if( _id2p!=null &&  uoid.equals(_id2p) ){                              
    	   _inf.srm = (LoginBrokerInfo[])obj;   
    	   
       /*
        * If the message riceved is the reply at 'xcm ls'
        * it contains, as object, an updated InfoPoolTable object.
        */
       }else if( _id1!=null && uoid.equals(_id1) ){                              // if( obj instanceof CostModulePoolInfoTable )
    	   _inf.pools = (CostModulePoolInfoTable)obj;
       
       /*
        * If the message riceved is the reply at 'psux ls pgroup'
        * it contains, as object, an array containing every pool 
        * group name. For each name in this array, a 'psux ls pgroup 
        * &lt;poolgroupname&gt;' message is sent, storing the UOID
        * produced in an array with the same dimension of the array
        * in the arrived message. 
        */
        }else if( _id2!=null && uoid.equals(_id2) ){
    	   _inf.pgroup_pools.clear();
    	   _inf.pgroup_links.clear();
    	   
    	   Object[] pg = (Object[])obj;
    	   _id3 = new UOID[pg.length];
    	   for(int i=0; i<pg.length; i++){ 
              try{
                 CellMessage msg3 = new CellMessage( new CellPath("PoolManager"), "psux ls pgroup "+pg[i].toString() ) ;
                 sendMessage( msg3 ) ;
                 _id3[i] = msg3.getUOID();
              }catch(Exception e ){
    	         esay( " Problem sending msg: "+e ) ;
              }
    	   } 
    	/*
    	 * If the message recived is the reply at 'psux ls pgroup &lt;poolgroupname&gt;' 
    	 * (ac_psux_ls_pgroup_$_0_1) message, it contains as object an Object[] that 
    	 * contains information about pools and links related to the specified pool group.
    	 * This informations are stored in two separated Hashtable in the InfoBase object.
    	 */   
       } else { 
    	   for(int i=0; i<_id3.length; i++)
              if( uoid.equals(_id3[i]) ){
                 Object[] result = (Object[])obj;
                 _inf.pgroup_pools.put((String)result[0], (Object[])result[1]);
                 _inf.pgroup_links.put((String)result[0], (Object[])result[2]);
              }
       }
       
   }
   
   public String hh_report = "" ;
   /**
    * Command 'report'.<br/>
    * Show all last dynamic information collected in a human-readable form.
    * @param args - any
    * @return report
    * @throws Exception
    */
   public String ac_report_$_0( Args args ) throws Exception {
       return _inf.show();
   }
  
   
   
   /**
    * This thread send periodically the commands needed to obtain
    * the information from the cells. 
    **/
   public void run(){
	   
       if( Thread.currentThread() == _sendThread ){
           
         synchronized( this ){            
            while( _continue ){
               
               try{
                   wait( _interval ) ;
               }catch( InterruptedException ie ){
                   break ;
               }
               
               try{
            	  // send 'ls -binary' to LoginBroker and save message UOID
             	  CellMessage msg1p = new CellMessage( new CellPath("LoginBroker"), "ls -binary" ) ;
                  sendMessage( msg1p ) ;
                  _id1p = msg1p.getUOID();
               }catch(Exception e ){
                   esay( " Problem sending msg: "+e ) ;
               }try{
                  // send 'ls -binary' to srm-LoginBroker and save message UOID
             	  CellMessage msg2p = new CellMessage( new CellPath("srm-LoginBroker"), "ls -binary" ) ;
                  sendMessage( msg2p ) ;
                  _id2p = msg2p.getUOID();
               }catch(Exception e ){
                   esay( " Problem sending msg: "+e ) ;
               }try{   
            	  // send 'xcm ls' and save message UOID
            	  CellMessage msg1 = new CellMessage( new CellPath("PoolManager"), "xcm ls" ) ;
                  sendMessage( msg1 ) ;
                  _id1 = msg1.getUOID();
               }catch(Exception e ){
                   esay( " Problem sending msg: "+e ) ;
               }try{  
                  // send 'psux ls pgroup' and save messageUOID
                  CellMessage msg2 = new CellMessage( new CellPath("PoolManager"), "psux ls pgroup" ) ;
                  sendMessage( msg2 ) ;
                  _id2 = msg2.getUOID();  
               }catch(Exception e ){
                  esay( " Problem sending msg: "+e ) ;
               }
                
            }
         }
      }
      kill() ;
   }

}
