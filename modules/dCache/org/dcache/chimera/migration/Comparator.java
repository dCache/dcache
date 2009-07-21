package org.dcache.chimera.migration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.dcache.chimera.namespace.ChimeraNameSpaceProvider;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.provider.BasicNameSpaceProvider;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import dmg.util.Args;

public class Comparator {

	/**
	 * An Exception thrown when a mismatch is discovered.
	 */
	public static class MismatchException extends Exception {
		private static final long serialVersionUID = 1L;

		public MismatchException(String msg) {
			super(msg);
		}
	}

	/**
	 * Check whether two StorageInfo objects (one supplied by Chimera, the other by PNFS) are equal.
	 *
	 * @param pnfsId  The string describing the PNFS ID.  This is used only to create a suitable message for Exceptions
	 * @param chimeraStorageInfo the StorageInfo from Chimera
	 * @param pnfsStorageInfo the StorageInfo from PNFS
	 * @throws MismatchException if StorageInfos are not equal.
	 */
	public static void assertEquals(String pnfsId,
			StorageInfo chimeraStorageInfo, StorageInfo pnfsStorageInfo)
			throws MismatchException {

		if( chimeraStorageInfo == null && pnfsStorageInfo == null)
			return;

		if( chimeraStorageInfo == null)
			throw new MismatchException( "ID "+pnfsId+" has null Chimera StorageInfo");

		if( pnfsStorageInfo == null)
			throw new MismatchException( "ID "+pnfsId+" has null PNFS StorageInfo");

		if ( chimeraStorageInfo.equals(pnfsStorageInfo))
			return;

		StringBuilder sb = new StringBuilder();
		sb.append("ID " + pnfsId
				+ " failed consistency check for Storage Info.\n");
		sb.append("\tPNFS:\n");
		sb.append("\t\t" + pnfsStorageInfo.toString() + "\n");
		sb.append("\tChimera:\n");
		sb.append("\t\t" + chimeraStorageInfo.toString() + "\n");

		throw new MismatchException(sb.toString());
	}

	/**
	 * Check whether two FileMetaData objects (one supplied by Chimera, the other by PNFS) are equal.
	 *
	 * @param pnfsId The string describing the PNFS ID.  This is used only to create a suitable message for Exceptions.
	 * @param chimeraMetaData the FileMetaData from Chimera
	 * @param pnfsMetaData the FileMetaData from PNFS
	 * @throws MismatchException if FileMetaData are not equal.
	 */
	public static void assertEquals(String pnfsId,
			FileMetaData chimeraMetaData, FileMetaData pnfsMetaData)
			throws MismatchException {

		if( chimeraMetaData == null && pnfsMetaData == null)
			return;

		if( chimeraMetaData == null)
			throw new MismatchException( "ID "+pnfsId+" has null Chimera FileMetaData");

		if( pnfsMetaData == null)
			throw new MismatchException( "ID "+pnfsId+" has null PNFS FileMetaData");

		if ( chimeraMetaData.equals(pnfsMetaData))
			return;

		StringBuilder sb = new StringBuilder();
		sb.append("ID " + pnfsId
				+ " failed consistency check for File Metadata.\n");
		sb.append("\tPNFS:\n");
		sb.append("\t\t" + pnfsMetaData.toString() + "\n");
		sb.append("\tChimera:\n");
		sb.append("\t\t" + chimeraMetaData.toString() + "\n");

		/**
		 * Here we explain the differences between the two FileMetaData objects.
		 */
		sb.append("\nDifferences:\n");
		if (pnfsMetaData.getGid() != chimeraMetaData.getGid())
			sb.append("\tgid: " + pnfsMetaData.getGid() + " != "
					+ chimeraMetaData.getGid() + "\n");

		if (pnfsMetaData.getUid() != chimeraMetaData.getUid())
			sb.append("\tuid: " + pnfsMetaData.getUid() + " != "
					+ chimeraMetaData.getUid() + "\n");

		if (pnfsMetaData.isDirectory() != chimeraMetaData.isDirectory())
			sb.append("\tdirectory: " + pnfsMetaData.isDirectory() + " != "
					+ chimeraMetaData.isDirectory() + "\n");

		if (pnfsMetaData.isSymbolicLink() != chimeraMetaData.isSymbolicLink())
			sb.append("\tsym-link: " + pnfsMetaData.isSymbolicLink() + " != "
					+ chimeraMetaData.isSymbolicLink() + "\n");

		if (pnfsMetaData.isRegularFile() != chimeraMetaData.isRegularFile())
			sb.append("\tregular file: " + pnfsMetaData.isRegularFile()
					+ " != " + chimeraMetaData.isRegularFile() + "\n");

		if (pnfsMetaData.getFileSize() != chimeraMetaData.getFileSize())
			sb.append("\tsize: " + pnfsMetaData.getFileSize() + " != "
					+ chimeraMetaData.getFileSize() + "\n");

		if (!pnfsMetaData.getUserPermissions().equals(
				chimeraMetaData.getUserPermissions()))
			sb.append("\tuser permissions: "
					+ pnfsMetaData.getUserPermissions() + " != "
					+ chimeraMetaData.getUserPermissions() + "\n");

		if (!pnfsMetaData.getGroupPermissions().equals(
				chimeraMetaData.getGroupPermissions()))
			sb.append("\tgroup permissions: "
					+ pnfsMetaData.getGroupPermissions() + " != "
					+ chimeraMetaData.getGroupPermissions() + "\n");

		if (!pnfsMetaData.getWorldPermissions().equals(
				chimeraMetaData.getWorldPermissions()))
			sb.append("\tworld permissions: "
					+ pnfsMetaData.getWorldPermissions() + " != "
					+ chimeraMetaData.getWorldPermissions() + "\n");

		if (pnfsMetaData.getCreationTime() != chimeraMetaData.getCreationTime())
			sb.append("\tcreation-time: " + pnfsMetaData.getCreationTime()
					+ " != " + chimeraMetaData.getCreationTime() + "\n");

		if (pnfsMetaData.getLastAccessedTime() != chimeraMetaData
				.getLastAccessedTime())
			sb.append("\tlast-accessed: " + pnfsMetaData.getLastAccessedTime()
					+ " != " + chimeraMetaData.getLastAccessedTime() + "\n");

		if (pnfsMetaData.getLastModifiedTime() != chimeraMetaData
				.getLastModifiedTime())
			sb.append("\tlast-modified: " + pnfsMetaData.getLastModifiedTime()
					+ " != " + chimeraMetaData.getLastModifiedTime() + "\n");

		throw new MismatchException(sb.toString());
	}

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

		NameSpaceProvider chimeraStorageInfoProvider = new ChimeraNameSpaceProvider(
				new Args(chimeraArgs), null);
		NameSpaceProvider pnfsStorageInfoProvider = new BasicNameSpaceProvider(
				new Args(pnfsArgs), null);

		int idCount = 0, idErrCount = 0;


		BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(new File(file)));
        } catch (FileNotFoundException e1) {
            System.out.println("\nCouldn't find file " + file);
            System.exit(2);
        }

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

                try {
                    FileMetaData chimeraMetaData = chimeraNamespace.getFileMetaData( pnfsid);
                    FileMetaData pnfsFileMetaData = pnfsNamespace.getFileMetaData( pnfsid);
                    assertEquals( pnfsid.toString(), chimeraMetaData, pnfsFileMetaData);
                } catch (MismatchException e) {
                    idErrCount++;
                    if( showAllErrors)
                        System.out.println( "\n" + e.getMessage());
                    else
                        throw e;
                }

                try {
                    StorageInfo chimeraStorageInfo = chimeraStorageInfoProvider.getStorageInfo( pnfsid);
                    StorageInfo pnfsStorageInfo = pnfsStorageInfoProvider.getStorageInfo( pnfsid);
                    assertEquals( pnfsid.toString(), chimeraStorageInfo, pnfsStorageInfo);
                } catch (MismatchException e) {
                    if( showAllErrors)
                        System.out.println( "\n" + e.getMessage());
                    else
                        throw e;
                }
			}
		} catch (IOException e) {
			System.out.println("\nCannot read file: " + e.getMessage());
			System.exit(2);
		} catch (MismatchException e) {
			System.out.println("\n" + e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			System.out.println("");
			e.printStackTrace();
			System.exit(2);
		} finally {
			try {
			    br.close();
			} catch (IOException e) {
				// Ignore this one: it doesn't really matter and shouldn't happen with local files, right?
			}
		}

		System.out.println("\n\n" + idCount + " IDs verified, " +
		                   (idErrCount == 0 ? "all OK." : (idErrCount + " failures.")));
	}
}
