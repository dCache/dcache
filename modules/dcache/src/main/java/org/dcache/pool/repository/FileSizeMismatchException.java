package org.dcache.pool.repository;

import org.dcache.pool.repository.v3.RepositoryException;

public class FileSizeMismatchException extends RepositoryException
{
    private static final long serialVersionUID = -5255866857461687962L;

    public FileSizeMismatchException(String s)
    {
        super(s);
    }
}
