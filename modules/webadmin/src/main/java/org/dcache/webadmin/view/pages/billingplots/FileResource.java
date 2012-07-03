package org.dcache.webadmin.view.pages.billingplots;

import java.io.File;

import org.apache.wicket.markup.html.WebResource;
import org.apache.wicket.util.resource.FileResourceStream;
import org.apache.wicket.util.resource.IResourceStream;

public class FileResource extends WebResource {

    private static final long serialVersionUID = 1L;
    private File file;

    public FileResource(File file) {
        this.file = file;
    }

    @Override
    public IResourceStream getResourceStream() {
        return new FileResourceStream(file);
    }
}
