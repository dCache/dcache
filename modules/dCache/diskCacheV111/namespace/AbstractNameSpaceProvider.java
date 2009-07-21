package diskCacheV111.namespace;

import java.util.Set;
import java.util.List;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.util.Checksum;

public class AbstractNameSpaceProvider
    implements NameSpaceProvider
{
    @Override
    public void setFileMetaData(PnfsId pnfsId, FileMetaData metaData)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileMetaData getFileMetaData(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PnfsId createEntry(String path, FileMetaData metaData, boolean isDirectory)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteEntry(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteEntry(String path)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameEntry(PnfsId pnfsId, String newName)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String pnfsidToPath(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PnfsId pathToPnfsid(String path, boolean followLinks)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PnfsId getParentOf(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getFileAttributeList(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getFileAttribute(PnfsId pnfsId, String attribute)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeFileAttribute(PnfsId pnfsId, String attribute)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFileAttribute(PnfsId pnfsId, String attribute, Object data)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addChecksum(PnfsId pnfsId, int type, String value)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getChecksum(PnfsId pnfsId, int type)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeChecksum(PnfsId pnfsId, int type)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] listChecksumTypes(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Checksum> getChecksums(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageInfo getStorageInfo(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStorageInfo(PnfsId pnfsId, StorageInfo storageInfo, int mode)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addCacheLocation(PnfsId pnfsId, String cacheLocation)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getCacheLocation(PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }
}