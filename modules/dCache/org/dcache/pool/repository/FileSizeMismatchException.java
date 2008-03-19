package org.dcache.pool.repository;

import org.dcache.pool.repository.v3.RepositoryException;

public class FileSizeMismatchException extends RepositoryException
{
    public FileSizeMismatchException(String s)
    {
        super(s);
    }
}
