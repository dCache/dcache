package org.dcache.chimera.migration;

import java.io.PrintStream;
import java.util.Map;

import org.dcache.auth.Subjects;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

public class StorageInfoComparator implements PnfsIdValidator {

    final private PrintStream _pw;
    final private NameSpaceProvider _nsp1;
    final private String _nsp1Name;
    final private NameSpaceProvider _nsp2;
    final private String _nsp2Name;

    /**
     * Compare the StorageInfo returned by two NameSpaceProvider objects. No
     * output is emitted.
     */
    public StorageInfoComparator( NameSpaceProvider nsp1, NameSpaceProvider nsp2) {
        this( null, nsp1, nsp2);
    }

    /**
     * Compare the StorageInfo returned by two NameSpaceProvider objects. Any
     * discrepancies are recorded in the supplied PrintStream object.
     */
    public StorageInfoComparator( PrintStream pw, NameSpaceProvider nsp1,
                                  NameSpaceProvider nsp2) {
        this( pw, nsp1, nsp1.getClass().getSimpleName(), nsp2, nsp2.getClass()
                .getSimpleName());
    }

    /**
     * Compare the StorageInfo returned by two NameSpaceProvider objects. Any
     * discrepancies are recorded in the supplied PrintStream object.
     * NameSpaceProviders nsp1 and nsp2 are identified by the labels name1
     * and name2 respectively.
     */
    public StorageInfoComparator( PrintStream pw, NameSpaceProvider nsp1,
                                  String name1, NameSpaceProvider nsp2,
                                  String name2) {
        _pw = pw;
        _nsp1 = nsp1;
        _nsp1Name = name1;
        _nsp2 = nsp2;
        _nsp2Name = name2;
    }

    @Override
    public boolean isOK( PnfsId pnfsId) {

        StorageInfo si1, si2;

        try {
            si1 = _nsp1.getStorageInfo( Subjects.ROOT, pnfsId);
        } catch (IllegalArgumentException e) {
            if( _pw != null)
                _pw.println( "Failed to obtain StorageInfo from " + _nsp1Name +
                             ": " + e.getMessage());
            return false;
        } catch (CacheException e) {
            if( _pw != null)
                _pw.println( "Failed to obtain StorageInfo from " + _nsp1Name +
                             ": " + e.getMessage());
            return false;
        }

        try {
            si2 = _nsp2.getStorageInfo( Subjects.ROOT, pnfsId);
        } catch (IllegalArgumentException e) {
            if( _pw != null)
                _pw.println( "Failed to obtain StorageInfo from " + _nsp2Name +
                             ": " + e.getMessage());
            return false;

        } catch (CacheException e) {
            if( _pw != null)
                _pw.println( "Failed to obtain StorageInfo from " + _nsp2Name +
                             ": " + e.getMessage());
            return false;
        }

        if( si1 == null && si2 == null)
            return true;

        if( si1 == null) {
            if( _pw != null)
                _pw.println( "ID " + pnfsId + " has null " + _nsp1Name +
                             " StorageInfo");
            return false;
        }


        /*
         *  PNFS parses the various "keys" in a file's level-2 metadata to obtain
         *  a set of keyword-value pairs.  These values are then evaluated to
         *  obtain information like the file's checksum(s), AccessLatency and
         *  Retention policy information.  Chimera has no level-2 metadata, so
         *  current does not create any flag entries; moreover, with PNFS,
         *  whether a file's AccessLatency and RetentionPolicy is in the flags
         *  depends on whether this information was taken from the file's level-2
         *  metadata or whether it came from the directory (or dCache defaults).
         *  Therefore it is hard for Chimera to emulate PNFS's behaviour accurately.
         *
         *  The (Generic)StorageInfo class equals method is sensitive to the flags.
         *  So, as a work-around, we remove all key/flag entries before checking
         *  equality.
         */
        removeAllKeys( si1);
        removeAllKeys( si2);

        if( si1.equals( si2))
            return true;

        if( _pw != null) {
            _pw.println( "ID " + pnfsId +
                         " failed consistency check for Storage Info.");
            _pw.println( "\t" + _nsp1Name + ":");
            _pw.println( "\t\t" + si1);
            _pw.println( "\t" + _nsp2Name + ":");
            _pw.println( "\t\t" + si2);
        }

        return false;
    }


    private void removeAllKeys( StorageInfo si) {
        Map<String,String> keyMap = si.getMap();

        for( String key : keyMap.keySet()) {
            si.setKey( key, null);
        }
    }

}
