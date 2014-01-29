package diskCacheV111.hsmControl ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.hsmControl.HsmControlGetBfDetailsMsg;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.SyncFifo2;

import org.dcache.util.Args;

public class HsmControlOsm extends CellAdapter implements Runnable {

    private final static Logger _log =
        LoggerFactory.getLogger(HsmControlOsm.class);

    private CellNucleus _nucleus ;
    private Args        _args ;
    private int         _requests;
    private int         _failed;
    private int         _outstandingRequests;
    private File        _database;
    private SyncFifo2   _fifo = new SyncFifo2() ;
    private SimpleDateFormat formatter
         = new SimpleDateFormat ("MM.dd hh:mm:ss");

    public HsmControlOsm( String name , String  args ) throws Exception {
       super( name , args , false ) ;
       _nucleus = getNucleus() ;
       _args    = getArgs() ;
       try{
          if( _args.argc() < 1 ) {
              throw new
                      IllegalArgumentException("Usage : ... <database>");
          }

          _database = new File( _args.argv(0) ) ;
          if( ! _database.isDirectory() ) {
              throw new
                      IllegalArgumentException("Not a directory : " + _database);
          }
       }catch(Exception e){
          start() ;
          kill() ;
          throw e ;
       }
       useInterpreter( true );
       _nucleus.newThread( this , "queueWatch").start() ;
       start();
       export();
    }

    @Override
    public void getInfo( PrintWriter pw ){
       pw.println("HsmControlOsm : [$Id: HsmControlOsm.java,v 1.4 2005-01-17 16:21:33 patrick Exp $]" ) ;
       pw.println("Requests    : "+_requests ) ;
       pw.println("Failed      : "+_failed ) ;
       pw.println("Outstanding : "+_fifo.size() ) ;
    }
    private int _maxQueueSize = 100 ;
    @Override
    public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       _requests ++ ;
       String error;
        if( ! ( obj instanceof Message ) ){
            error = "Illegal dCache message class : "+obj.getClass().getName();
        }else if( _fifo.size() > _maxQueueSize ){
            error = "Queue size exceeded "+_maxQueueSize+", Request rejected";
       }else{
          _fifo.push( msg ) ;
          return ;
       }
       _failed ++ ;
       _log.warn(error);
       ((Message)obj).setFailed( 33 , error ) ;
       msg.revertDirection() ;
       try{
          sendMessage( msg ) ;
       }catch(Exception ee ){
          _log.warn("Problem replying : "+ee ) ;
       }
    }
    @Override
    public void run(){
        // HsmControlGetBfDetailsMsg
        _log.info("Starting working thread");
        try{
            while( true ){
                CellMessage msg = (CellMessage)_fifo.pop() ;
                if( msg == null ){
                    _log.warn("fifo empty");
                    break ;
                }
                Message request = (Message)msg.getMessageObject() ;
                try{

                    if( request instanceof HsmControlGetBfDetailsMsg ){
                        getBfDetails( ((HsmControlGetBfDetailsMsg)request).getStorageInfo() );
                    }else{
                        throw new
                        IllegalArgumentException("Not supported "+request.getClass().getName());
                    }
                   request.setSucceeded() ;
                   msg.revertDirection() ;
                   try{
                      sendMessage( msg ) ;
                   }catch(Exception ee ){
                      _log.warn("Problem replying : "+ee ) ;
                   }
                }catch(Exception eee ){
                   _failed ++ ;
                   _log.warn(eee.toString(), eee);
                   request.setFailed( 34 , eee.toString() ) ;
                   msg.revertDirection() ;
                   try{
                      sendMessage( msg ) ;
                   }catch(Exception ee ){
                      _log.warn("Problem replying : "+ee ) ;
                   }
                }
            }
        }catch(Exception ee ){
            _log.warn("Got exception from run while : "+ee);
        }finally{
            _log.info("Working thread finished");
        }
    }
    private Map<String, Object[]> _driverMap = new HashMap<>() ;
    public static final String hh_check_osm = "<store> <bfid>";
    public String ac_check_osm_$_2( Args args )throws Exception {
       String store = args.argv(0);
       String bfid  = args.argv(1);
       StorageInfo si = new OSMStorageInfo( store , "" , bfid ) ;

       getBfDetails( si ) ;

       String result = si.getKey("hsm.details");

       return result == null ? "No Details" : result ;
    }
    public static final String hh_define_driver = "<hsm> <driverClass> [<options>]";
    public String ac_define_driver_$_2( Args args ) throws Exception {

        String hsm    = args.argv(0);
        String driver = args.argv(1);
        Class<?> c = Class.forName(driver);
        Class<?>[] classArgs  = { Args.class,Args.class } ;
        Object [] objectArgs = { getArgs() , args ,  this } ;

        Constructor<?> con = c.getConstructor( classArgs ) ;
        Object[] values = new Object[3];
        try{
            values[0] = driver ;
            values[1] = con.newInstance( objectArgs ) ;
            values[2] = args ;
        }catch(InvocationTargetException ite ){
            throw (Exception)ite.getTargetException();
        }
        if( ! ( values[1] instanceof HsmControllable ) ) {
            throw new
                    IllegalArgumentException("Not a HsmControllable : (" + hsm + ") " + driver);
        }

        _driverMap.put( hsm , values ) ;
        return hsm+" "+driver+" "+values[1].toString();
    }
    public static final String hh_ls_driver = "";
    public String ac_ls_driver( Args args ){
         StringBuilder sb = new StringBuilder() ;
        for (Map.Entry<String,Object[]> e : _driverMap.entrySet()) {
            String hsm = e.getKey();
            Object[] obj = e.getValue();

            sb.append(hsm).append(" ").
                    append(obj[0].toString()).append(" ").
                    append(obj[1].toString()).append("\n");

        }
         return sb.toString();
    }
    private void getBfDetails( StorageInfo storageInfo ) throws Exception {
        String hsm = storageInfo.getHsm() ;
        if( ( hsm == null ) || ( hsm.equals("") ) ) {
            throw new
                    IllegalArgumentException("Hsm not specified");
        }

        Object [] values = _driverMap.get( hsm );
        if( values == null ) {
            throw new
                    IllegalArgumentException("Driver not found for hsm=" + hsm);
        }

        HsmControllable hc = (HsmControllable)values[1] ;
        _log.info("Controller found for "+hsm+" -> "+values[0]);
        hc.getBfDetails( storageInfo );

    }
}
