package org.dcache.services.info.base;

/**
 * The GuidedVerifyingVisitor is a VerifyingVisitor that visits only the
 * subtree allowed by the supplied StateGuide.
 * <p>
 * No effort is made to correlate the parts of the subtree that the
 * StateGuide allows with the state the VerifyingVisitor is expecting to
 * encounter.
 */
public class GuidedVerifyingVisitor extends VerifyingVisitor {
    private final StateGuide _guide;

    public GuidedVerifyingVisitor( StateGuide guide) {
        _guide = guide;
    }

    @Override
    public boolean isVisitable( StatePath path) {
        return _guide.isVisitable( path);
    }
}
