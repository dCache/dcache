package org.dcache.gplazma.htpasswd;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;

class FileMultableInputSupplier implements MutableInputSupplier<Reader>
{
    private final File file;
    private final Charset charset;

    public FileMultableInputSupplier(File file, Charset charset)
    {
        this.file = file;
        this.charset = charset;
    }

    @Override
    public long lastModified()
    {
        return file.lastModified();
    }

    @Override
    public Reader getInput() throws IOException
    {
        return Files.newReader(file, charset);
    }
}
