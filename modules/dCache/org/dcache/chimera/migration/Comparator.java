package org.dcache.chimera.migration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.dcache.chimera.namespace.ChimeraNameSpaceProvider;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.provider.BasicNameSpaceProvider;
import diskCacheV111.util.PnfsId;
import dmg.util.Args;

public class Comparator {
    
    private static final String OPTION_CHIMIERA_CONFIG = "config";
    private static final String OPTION_PNFS_MOUNT = "pnfsMount";
    
    private static final String FILENAME_CHIMERA_CONFIG_DEFAULT = "/opt/d-cache/config/chimera-config.xml";

	public static void main(String args[]) throws Exception {

	    Args parsedArgs = new Args( args);

        if( parsedArgs.argc() != 1 || parsedArgs.isOneCharOption( 'h')) {
			System.err.println("");
			System.err.println("    Usage Comparator [-k] [-" + OPTION_CHIMIERA_CONFIG + "=<file>] <file>");
			System.err.println("");
			System.err.println("where <file> contains a list of PNFS IDs of files to verify.");
            System.err.println( "-k continue when an inconsistency is discovered.\n");
            System.err.println( "-" + OPTION_CHIMIERA_CONFIG + "=<file> is the Chimera XML configuration file.\n");
            System.err.println( "     ("+FILENAME_CHIMERA_CONFIG_DEFAULT + " by default)\n");
			System.exit(2);
		}
        
        String suppliedConfigFilename = parsedArgs.getOpt( OPTION_CHIMIERA_CONFIG);
        String chimeraConfigFilename = suppliedConfigFilename != null ?
                suppliedConfigFilename :
                FILENAME_CHIMERA_CONFIG_DEFAULT;
        
        String pnfsMount = parsedArgs.getOpt( OPTION_PNFS_MOUNT);

		String file = parsedArgs.argv( 0);
		boolean showAllErrors = parsedArgs.isOneCharOption( 'k');

		String pnfsArgs = "diskCacheV111.util.GenericInfoExtractor "
				+ "-delete-registration=dummyLocation -delete-registration-jdbcDrv=foo "
				+ "-delete-registration-dbUser=dummyUser -delete-registration-dbPass=dummyPass "
				+ (pnfsMount != null ? "-pnfs="+pnfsMount : "");
		String chimeraArgs = "org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor "
				+ "-chimeraConfig=" + chimeraConfigFilename;

		NameSpaceProvider chimeraNamespace = new ChimeraNameSpaceProvider(
				new Args(chimeraArgs), null);
		NameSpaceProvider pnfsNamespace = new BasicNameSpaceProvider(new Args(
				pnfsArgs), null);

		PnfsIdValidator checkFileMetaData = new FileMetaDataComparator( System.err, chimeraNamespace, pnfsNamespace);
        PnfsIdValidator checkStorageInfo = new StorageInfoComparator( System.err, chimeraNamespace, pnfsNamespace);
		PnfsIdValidator combinedComparator = new CombinedComparator( checkFileMetaData, checkStorageInfo);

		BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(new File(file)));
        } catch (FileNotFoundException e1) {
            System.out.println("\nCouldn't find file " + file);
            System.exit(2);
        }

        int errorCount = 0;

        TerminableBlockingQueue<PnfsId> queue = new TerminableBlockingQueueWrapper<PnfsId>( 
        		new ArrayBlockingQueue<PnfsId>( 1, true),
        		new PnfsId("0000000000000000000000"), 1);

        PnfsIdProducer producer = new PnfsIdProducer( br, queue);
        
        producer.start();
        
        
        PnfsId id;
        while( (id = queue.take()) != queue.getPoisonItem()) {
        	if( !combinedComparator.isOK( id)) {
        		errorCount++;
        		if( !showAllErrors)
        			break;
        	}
        }

        producer.emitFinalStatistics();

		System.out.println((errorCount == 0 ? "All OK." : (errorCount + " failures.")));
	}
}
