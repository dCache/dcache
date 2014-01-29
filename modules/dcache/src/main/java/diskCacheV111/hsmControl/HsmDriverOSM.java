/*
 * HsmDriverOSM.java
 *
 * Created on January 17, 2005, 12:46 AM
 */

package diskCacheV111.hsmControl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringTokenizer;

import diskCacheV111.util.RunSystem;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.util.Args;

/**
 *
 * @author  patrick
 */
    public class HsmDriverOSM implements HsmControllable  {

        private Logger _log = LoggerFactory.getLogger(HsmDriverOSM.class);

        private Args _global;
        private Args _local;
        private String _command;

        public HsmDriverOSM( Args global , Args local )
        {
            _global = global ;
            _local  = local ;
            _command = local.getOpt("command");

            if( ( _command == null ) || ( _command.equals("") )) {
                throw new
                        IllegalArgumentException("command  option not found");
            }

        }
        @Override
        public void getBfDetails( StorageInfo storageInfo ) throws Exception {
            setBfDetails( storageInfo ) ;
            try{
                setVolumeDetails( storageInfo );
            }catch(Exception ee){
                _log.error("Can't get volume details "+ee);
            }

        }
        public String toString(){

            return "HSM control driver for OSM ("+_command+")";
        }
        private void setVolumeDetails( StorageInfo storageInfo )throws Exception {
            if( ! ( storageInfo instanceof OSMStorageInfo ) ) {
                throw new
                        IllegalArgumentException("not an OSM storage info " + storageInfo
                        .getClass().getName());
            }

             OSMStorageInfo osm = (OSMStorageInfo)storageInfo ;

             String tape  = osm.getKey( "hsm.osm.volumeName" ) ;
             String store = osm.getStore() ;

             if( ( tape == null )  || ( tape.equals("")  ) ||
                 ( store == null ) || ( store.equals("") )    ) {
                 throw new
                         IllegalArgumentException("Not enough info in storageInfo (volumeName)");
             }

             String command = _command+" -S "+store+" lsvol -l "+tape ;

             RunSystem system = new RunSystem( command , 1000, 10000L );
             system.go();

             int    rc     = system.getExitValue() ;
             String error  = system.getErrorString() ;
             String output = system.getOutputString() ;

             if( ( rc != 0 ) || ( error.length() != 0 )  || ( output.length() == 0 ) ) {
                 throw new
                         IllegalArgumentException(error == null ?
                         "Unknow error in responds to >" + command + "<" :
                         error);
             }

             _log.info("Output : "+output);

             String volNbf;
             String volStat;
             String volCap;
             String line;
             try{
                 StringTokenizer st = new StringTokenizer( output , "\n");
                 line = st.nextToken() ; line = st.nextToken() ;
                 st = new StringTokenizer( line );
                 for( int i = 0 ; i < 3 ; i++ ) {
                     st.nextToken();
                 }
                 volNbf = st.nextToken() ;
                 for( int i = 0 ; i < 3 ; i++ ) {
                     st.nextToken();
                 }
                 volCap  = st.nextToken() ;
                 volStat = st.nextToken() ;
             }catch(Exception ee ){
                throw new
                IllegalArgumentException("Format error in output of >"+command+"< : "+output);
             }
             String details = "volumeNbf="+volNbf+";volumeStatus="+volStat+";volumeCapacity="+volCap+";";
             String tmp = osm.getKey("hsm.details");
             osm.setKey("hsm.details" , tmp == null ? details : ( tmp+details ));
             osm.setKey("hsm.osm.volumeNbf",volNbf);
             osm.setKey("hsm.osm.volumeStatus",volStat);
             osm.setKey("hsm.osm.volumeCapacity",volCap );
        }
        private void setBfDetails( StorageInfo storageInfo )throws Exception {
            if( ! ( storageInfo instanceof OSMStorageInfo ) ) {
                throw new
                        IllegalArgumentException("not an OSM storage info " + storageInfo
                        .getClass().getName());
            }

             OSMStorageInfo osm = (OSMStorageInfo)storageInfo ;
             String store = osm.getStore() ;
             String bfid  = osm.getBitfileId() ;
             if( ( store == null ) || ( store.equals("") ) ||
                 ( bfid  == null ) || ( bfid.equals("")  )    ) {
                 throw new
                         IllegalArgumentException("Not enough info in storageInfo");
             }

             String command = _command+" -S "+store+" lsbf -a "+bfid ;

             RunSystem system = new RunSystem( command , 1000, 10000L );
             system.go();

             int    rc     = system.getExitValue() ;
             String error  = system.getErrorString() ;
             String output = system.getOutputString() ;

             if( ( rc != 0 ) || ( error.length() != 0 )  || ( output.length() == 0 ) ) {
                 throw new
                         IllegalArgumentException(error == null ?
                         "Unknow error in responds to >" + command + "<" :
                         error);
             }

             _log.info("Output : "+output);

             String tape;
             String status;
             String line;
             try{
                 StringTokenizer st = new StringTokenizer( output , "\n");
                 line = st.nextToken() ; line = st.nextToken() ;
                 st = new StringTokenizer( line );
                 for( int i = 0 ; i < 15 ; i++ ) {
                     st.nextToken();
                 }
                 tape = st.nextToken() ;
                 for( int i = 0 ; i < 6 ; i++ ) {
                     st.nextToken();
                 }
                 status = st.nextToken() ;
             }catch(Exception ee ){
                throw new
                IllegalArgumentException("Format error in output of >"+command+"< : "+output);
             }
             osm.setKey("hsm.details","volumeName="+tape+";bfStatus="+status+";");
             osm.setKey("hsm.osm.volumeName",tape);
             osm.setKey("hsm.osm.bfStatus",status);
        }
    }
