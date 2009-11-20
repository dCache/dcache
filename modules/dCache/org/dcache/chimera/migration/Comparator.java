package org.dcache.chimera.migration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.dcache.chimera.namespace.ChimeraNameSpaceProvider;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.provider.BasicNameSpaceProvider;
import diskCacheV111.util.PnfsId;
import dmg.util.Args;

public class Comparator {

	public static void main(String args[]) throws Exception {

	    Args parsedArgs = new Args( args);

        if( parsedArgs.argc() != 1 || parsedArgs.optc() > 1 ||
	            (parsedArgs.optc() == 1 && !parsedArgs.optv( 0).equals( "k"))) {
			System.err.println("");
			System.err.println("    Usage Comparator [-k] <file>");
			System.err.println("");
			System.err.println("where <file> contains a list of PNFS IDs of files to verify.");
                        System.err.println( "-k continue when an inconsistency is discovered.\n");
			System.exit(2);
		}

		String file = parsedArgs.argv( 0);
		boolean showAllErrors = parsedArgs.isOneCharOption( 'k');

		String pnfsArgs = "diskCacheV111.util.GenericInfoExtractor "
				+ "-delete-registration=dummyLocation -delete-registration-jdbcDrv=foo "
				+ "-delete-registration-dbUser=dummyUser -delete-registration-dbPass=dummyPass ";
		String chimeraArgs = "org.dcache.chimera.namespace.ChimeraOsmStorageInfoExtractor "
				+ "-chimeraConfig=/opt/d-cache/config/chimera-config.xml";

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

        int idCount = 0, errorCount = 0;

		try {
			String rawLine;

			while( (rawLine = br.readLine()) != null) {

			    String trimmedLine = rawLine.trim();

                if( trimmedLine.startsWith( "#"))
                    continue;

                if( trimmedLine.length() == 0)
                    continue;

				if (idCount % 1000 == 0)
					System.out.print("\nChecked " + idCount + " files: ");
				else if (idCount % 20 == 0)
					System.out.print(".");
				System.out.flush();

				idCount++;

				PnfsId pnfsid = new PnfsId(trimmedLine);

				if( !combinedComparator.isOK( pnfsid)) {
				    errorCount++;
				    if( !showAllErrors)
				        break;
				}
			}
		} catch (IOException e) {
			System.err.println("Cannot read file: " + e.getMessage());
			System.exit(2);
		} finally {
			try {
			    br.close();
			} catch (IOException e) {
				// Ignore this one: it doesn't really matter and shouldn't happen with local files, right?
			}
		}

		System.out.println("\n" + idCount + " IDs verified, " +
		                   (errorCount == 0 ? "all OK." : (errorCount + " failures.")));
	}
}
