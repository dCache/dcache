package org.apache.curator.framework.imps;

public abstract  class PublicDelegatingCuratorFramework extends DelegatingCuratorFramework {

    public PublicDelegatingCuratorFramework(CuratorFrameworkImpl delegate) {
        super(delegate);
    }
}
