package org.dcache.gplazma;

/**
 * Session attribute information to be implemented by concrete attribute
 * implementations (e.g. home-directory, root-directory)
 */
public interface SessionAttribute {
    public String getName();
    public Object getValue();
}
