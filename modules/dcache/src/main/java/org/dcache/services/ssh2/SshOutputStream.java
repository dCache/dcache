package org.dcache.services.ssh2;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

class SshOutputStream extends FilterOutputStream {
    public SshOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(int c) throws IOException {
        if (c == '\n') {
            super.write(0xa);
            super.write(0xd);
        } else {
            super.write(c);
        }
    }
}
