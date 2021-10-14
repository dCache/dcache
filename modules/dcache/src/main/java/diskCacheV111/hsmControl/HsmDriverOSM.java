/*
 * HsmDriverOSM.java
 *
 * Created on January 17, 2005, 12:46 AM
 */

package diskCacheV111.hsmControl;

import diskCacheV111.util.RunSystem;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;
import java.util.StringTokenizer;
import org.dcache.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author patrick
 */
public class HsmDriverOSM implements HsmControllable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HsmDriverOSM.class);

    private final Args _global;
    private final Args _local;
    private final String _command;

    public HsmDriverOSM(Args global, Args local) {
        _global = global;
        _local = local;
        _command = local.getOpt("command");

        if ((_command == null) || (_command.isEmpty())) {
            throw new
                  IllegalArgumentException("command  option not found");
        }

    }

    @Override
    public void getBfDetails(StorageInfo storageInfo) throws Exception {
        setBfDetails(storageInfo);
        try {
            setVolumeDetails(storageInfo);
        } catch (Exception ee) {
            LOGGER.error("Can't get volume details {}", ee.toString());
        }

    }

    public String toString() {

        return "HSM control driver for OSM (" + _command + ")";
    }

    private void setVolumeDetails(StorageInfo storageInfo) throws Exception {
        if (!(storageInfo instanceof OSMStorageInfo)) {
            throw new
                  IllegalArgumentException("not an OSM storage info " + storageInfo
                  .getClass().getName());
        }

        OSMStorageInfo osm = (OSMStorageInfo) storageInfo;

        String tape = osm.getKey("hsm.osm.volumeName");
        String store = osm.getStore();

        if ((tape == null) || (tape.isEmpty()) ||
              (store == null) || (store.isEmpty())) {
            throw new
                  IllegalArgumentException("Not enough info in storageInfo (volumeName)");
        }

        RunSystem system = new RunSystem(1000, 10000L, _command, "-S", store, "lsvol", "-l",
              "tape");
        system.go();

        int rc = system.getExitValue();
        String error = system.getErrorString();
        String output = system.getOutputString();

        if ((rc != 0) || (!error.isEmpty()) || (output.isEmpty())) {
            throw new
                  IllegalArgumentException(error == null ?
                  "Unknow error in responds to >" + _command + "-S" + store + "lsvol" + "-l"
                        + "tape" + "<" :
                  error);
        }

        LOGGER.info("Output : {}", output);

        String volNbf;
        String volStat;
        String volCap;
        String line;
        try {
            StringTokenizer st = new StringTokenizer(output, "\n");
            line = st.nextToken();
            line = st.nextToken();
            st = new StringTokenizer(line);
            for (int i = 0; i < 3; i++) {
                st.nextToken();
            }
            volNbf = st.nextToken();
            for (int i = 0; i < 3; i++) {
                st.nextToken();
            }
            volCap = st.nextToken();
            volStat = st.nextToken();
        } catch (Exception ee) {
            throw new
                  IllegalArgumentException(
                  "Format error in output of >" + _command + "-S" + store + "lsvol" + "-l" + "tape"
                        + "< : " + output);
        }
        String details =
              "volumeNbf=" + volNbf + ";volumeStatus=" + volStat + ";volumeCapacity=" + volCap
                    + ";";
        String tmp = osm.getKey("hsm.details");
        osm.setKey("hsm.details", tmp == null ? details : (tmp + details));
        osm.setKey("hsm.osm.volumeNbf", volNbf);
        osm.setKey("hsm.osm.volumeStatus", volStat);
        osm.setKey("hsm.osm.volumeCapacity", volCap);
    }

    private void setBfDetails(StorageInfo storageInfo) throws Exception {
        if (!(storageInfo instanceof OSMStorageInfo)) {
            throw new
                  IllegalArgumentException("not an OSM storage info " + storageInfo
                  .getClass().getName());
        }

        OSMStorageInfo osm = (OSMStorageInfo) storageInfo;
        String store = osm.getStore();
        String bfid = osm.getBitfileId();
        if ((store == null) || (store.isEmpty()) ||
              (bfid == null) || (bfid.isEmpty())) {
            throw new
                  IllegalArgumentException("Not enough info in storageInfo");
        }

        RunSystem system = new RunSystem(1000, 10000L, _command, "-S", store, "lsbf", "-a", bfid);
        system.go();

        int rc = system.getExitValue();
        String error = system.getErrorString();
        String output = system.getOutputString();

        if ((rc != 0) || (!error.isEmpty()) || (output.isEmpty())) {
            throw new
                  IllegalArgumentException(error == null ?
                  "Unknow error in responds to >" + _command + "-S" + store + "lsbf" + "-a" + bfid
                        + "<" :
                  error);
        }

        LOGGER.info("Output : {}", output);

        String tape;
        String status;
        String line;
        try {
            StringTokenizer st = new StringTokenizer(output, "\n");
            line = st.nextToken();
            line = st.nextToken();
            st = new StringTokenizer(line);
            for (int i = 0; i < 15; i++) {
                st.nextToken();
            }
            tape = st.nextToken();
            for (int i = 0; i < 6; i++) {
                st.nextToken();
            }
            status = st.nextToken();
        } catch (Exception ee) {
            throw new
                  IllegalArgumentException(
                  "Format error in output of >" + _command + "-S" + store + "lsbf" + "-a" + bfid
                        + "< : " + output);
        }
        osm.setKey("hsm.details", "volumeName=" + tape + ";bfStatus=" + status + ";");
        osm.setKey("hsm.osm.volumeName", tape);
        osm.setKey("hsm.osm.bfStatus", status);
    }
}
