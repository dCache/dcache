package org.dcache.srm.unixfs;

import org.w3c.dom.Document;
import org.w3c.dom.Element;


/**
 * Configuration class that is based on a standard set of SRM configuration
 * but that adds support for UnixFS-specific configuration options.
 */
public class Configuration extends org.dcache.srm.util.Configuration
{
    private String _kpwdfile="../conf/dcache.kpwd";

    public Configuration()
    {
    }

    public Configuration(String path) throws Exception
    {
        super(path);
    }

    /** Getter for property kpwdFile.
     * @return Value of property kpwdFile.
     */
    public String getKpwdfile() {
        return _kpwdfile;
    }

    /** Setter for property kpwdFile.
     * @param kpwdfile New value of property kpwdFile.
     */
    public void setKpwdfile(String kpwdfile) {
        this._kpwdfile = kpwdfile;
    }


    @Override
    protected void set(String name, String value)
    {
        if(name.equals("kpwdfile")) {
            _kpwdfile = value;
        } else {
            super.set(name, value);
        }
    }

    @Override
    protected void write(Document document, Element root)
    {
        super.write(document, root);
        put(document,root, "kpwdfile", _kpwdfile,
                "kpwdfile, a dcache authorization database ");
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("\n\tkpwdfile=").append(this._kpwdfile);
        return sb.toString();
    }
}
