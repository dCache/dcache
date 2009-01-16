package org.dcache.chimera.migration;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.StorageInfoProvider;
import diskCacheV111.namespace.provider.BasicNameSpaceProvider;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import dmg.util.Args;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.dcache.chimera.namespace.ChimeraNameSpaceProvider;

public class Comperator {

    private static void pass(String msg, boolean pass) {
        if (!pass) {
            throw new IllegalStateException(msg);
        }
    }

    public static boolean compare(StorageInfo chimeraStorageInfo, StorageInfo pnfsStorageInfo) {
        return chimeraStorageInfo.equals(pnfsStorageInfo) &&
            chimeraStorageInfo.hashCode() == pnfsStorageInfo.hashCode();
    }

    public static boolean compare(FileMetaData chimeraMetaData, FileMetaData pnfsMetaData) {
        return chimeraMetaData.equals(pnfsMetaData) &&
            chimeraMetaData.hashCode() == pnfsMetaData.hashCode();
    }

    public static void main(String args[]) throws Exception {


        if (args.length != 1) {
            System.err.println("");
            System.err.println("    Usage Comperator <file>");
            System.err.println("");
            System.err.println(" where file is list of pnfsid to verify.");
        }


        String file = args[0];


        String pnfsArgs = "diskCacheV111.util.OsmInfoExtractor";
        String chimeraArge = "org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor " +
            "-chimeraConfig=/opt/d-cache/config/chimera-config.xml";


        NameSpaceProvider _chimeraNamespace = new ChimeraNameSpaceProvider(new Args(chimeraArge),
            null);
        NameSpaceProvider _pnfsNamespace = new BasicNameSpaceProvider(new Args(pnfsArgs),
            null);

        StorageInfoProvider _chimeraStorageInto = new ChimeraNameSpaceProvider(new Args(chimeraArge),
            null);
        StorageInfoProvider _pnfsStorageInfo = new BasicNameSpaceProvider(new Args(pnfsArgs),
            null);

        BufferedReader br = new BufferedReader(new FileReader(new File(file)));
        try {            

            while (true) {

                String line = br.readLine();
                PnfsId pnfsid = new PnfsId(line);

                FileMetaData chimeraMetaData = _chimeraNamespace.getFileMetaData(pnfsid);
                FileMetaData pnfsFileMetaData = _pnfsNamespace.getFileMetaData(pnfsid);
                pass(pnfsid.toString(), compare(chimeraMetaData, pnfsFileMetaData));

                StorageInfo chimeraStorageInfo = _chimeraStorageInto.getStorageInfo(pnfsid);
                StorageInfo pnfsStorageInfo = _pnfsStorageInfo.getStorageInfo(pnfsid);
                pass(pnfsid.toString(), compare(chimeraStorageInfo, pnfsStorageInfo));

            }
        } finally {
            br.close();
        }

    }
}
