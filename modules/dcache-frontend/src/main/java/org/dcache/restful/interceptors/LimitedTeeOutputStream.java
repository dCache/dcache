/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.restful.interceptors;

import org.apache.commons.io.output.ProxyOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A TeeOutputStream that limits the {@literal branch} OutputStream to
 * a maximum size.  The {@link #isBranchTruncated()} method describes whether
 * the branch OutputStream has been truncated.
 * @see org.apache.commons.io.output.TeeOutputStream
 */
public class LimitedTeeOutputStream extends ProxyOutputStream
{
    private final long limit;
    private boolean isBranchTruncated;
    protected final OutputStream branch;
    private long count;
    private int writeToBranch;

    public LimitedTeeOutputStream(OutputStream out, OutputStream branch, long limit)
    {
        super(out);
        this.branch = branch;
        checkArgument(limit > 0, "Limit must be a positive value");
        this.limit = limit;
    }

    @Override
    public synchronized void write(byte[] b) throws IOException
    {
        super.write(b);
        if (writeToBranch > 0) {
            branch.write(b, 0, writeToBranch);
        }
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException
    {
        super.write(b, off, len);
        if (writeToBranch > 0) {
            branch.write(b, off, writeToBranch);
        }
    }

    @Override
    public synchronized void write(int b) throws IOException
    {
        super.write(b);
        if (writeToBranch > 0) {
            branch.write(b);
        }
    }

    @Override
    public void flush() throws IOException
    {
        super.flush();
        branch.flush();
    }

    @Override
    public void close() throws IOException
    {
        try {
            super.close();
        } finally {
            this.branch.close();
        }
    }

    @Override
    protected void beforeWrite(int n)
    {
        writeToBranch = (int) Math.min(limit-count, n);

        if (writeToBranch < n) {
            isBranchTruncated = true;
        }
    }

    @Override
    protected void afterWrite(int n) throws IOException
    {
        count += writeToBranch;
    }

    /**
     * Whether the copied output to branch has been limited.
     */
    public boolean isBranchTruncated()
    {
        return isBranchTruncated;
    }
}

