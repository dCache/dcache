package diskCacheV111.hsmControl ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.hsmControl.HsmControlGetBfDetailsMsg;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;

import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkArgument;

public class HsmControlOsm extends CellAdapter implements Runnable {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(HsmControlOsm.class);

    private static final int MAX_QUEUE_SIZE = 100 ;

    private final CellNucleus _nucleus ;
    private int         _requests;
    private int         _failed;
    private int         _outstandingRequests;
    private final File        _database;

    private final BlockingQueue<CellMessage>  _fifo = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

    public HsmControlOsm(String name, String arguments)
    {
        super(name, arguments);
        _nucleus = getNucleus();
        Args args = getArgs();
        checkArgument(args.argc() >= 1, "Usage : ... <database>");
        _database = new File(args.argv(0));
    }

    @Override
    protected void starting() throws Exception
    {
        if (!_database.isDirectory()) {
            throw new IllegalArgumentException("Not a directory : " + _database);
        }
        useInterpreter( true );
        _nucleus.newThread( this , "queueWatch").start() ;
    }

    @Override
    public void getInfo( PrintWriter pw ){
       pw.println("HsmControlOsm : [$Id: HsmControlOsm.java,v 1.4 2005-01-17 16:21:33 patrick Exp $]" ) ;
       pw.println("Requests    : "+_requests ) ;
       pw.println("Failed      : "+_failed ) ;
       pw.println("Outstanding : "+_fifo.size() ) ;
    }

    @Override
    public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       _requests ++ ;
       String error;
        if( ! ( obj instanceof Message ) ){
            error = "Illegal dCache message class : "+obj.getClass().getName();
        }else if( _fifo.offer(msg) ){
            return;
        } else {
            error = "Queue size exceeded "+ MAX_QUEUE_SIZE + ", Request rejected";
        }
       _failed ++ ;
       LOGGER.warn(error);
       ((Message)obj).setFailed( 33 , error ) ;
       msg.revertDirection() ;
       try{
          sendMessage( msg ) ;
       }catch(RuntimeException ee ){
          LOGGER.warn("Problem replying : {}", ee.toString() ) ;
       }
    }
    @Override
    public void run(){
        // HsmControlGetBfDetailsMsg
        LOGGER.info("Starting working thread");
        try{
            while( true ){
                CellMessage msg = _fifo.poll();
                if( msg == null ){
                    LOGGER.warn("fifo empty");
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
                   }catch(RuntimeException ee ){
                      LOGGER.warn("Problem replying : {}", ee.toString() ) ;
                   }
                }catch(Exception eee ){
                   _failed ++ ;
                   LOGGER.warn(eee.toString(), eee);
                   request.setFailed( 34 , eee.toString() ) ;
                   msg.revertDirection() ;
                   try{
                      sendMessage( msg ) ;
                   }catch(RuntimeException ee ){
                      LOGGER.warn("Problem replying : {}", ee.toString() ) ;
                   }
                }
            }
        }catch(Exception ee ){
            LOGGER.warn("Got exception from run while : {}", ee.toString());
        }finally{
            LOGGER.info("Working thread finished");
        }
    }
    private final Map<String, Object[]> _driverMap = new HashMap<>() ;
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
        if( ( hsm == null ) || (hsm.isEmpty()) ) {
            throw new
                    IllegalArgumentException("Hsm not specified");
        }

        Object [] values = _driverMap.get( hsm );
        if( values == null ) {
            throw new
                    IllegalArgumentException("Driver not found for hsm=" + hsm);
        }

        HsmControllable hc = (HsmControllable)values[1] ;
        LOGGER.info("Controller found for {}  -> {}", hsm, values[0]);
        hc.getBfDetails( storageInfo );

    }
}
