package dmg.cells.zookeeper;

import com.google.common.base.Throwables;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;

public class LmPersistentNode<T> extends PersistentNode {

    private LmPersistentNode(CuratorFramework client,
          String basePath,
          T data,
          Function<T, byte[]> f) {
        super(client, CreateMode.EPHEMERAL, false, basePath, f.apply(data));
    }

    public static <T> LmPersistentNode<T> createOrUpdate(CuratorFramework client,
          String basePath,
          T data,
          Function<T, byte[]> f,
          LmPersistentNode lmPersistentNode) throws PersistentNodeException {
        if (lmPersistentNode == null) {
            LmPersistentNode<T> node = new LmPersistentNode<>(client, basePath, data, f);
            node.start();
            return node;
        } else {
            return lmPersistentNode.update(data, f, basePath);
        }
    }

    public LmPersistentNode<T> update(T data, Function<T, byte[]> f, String basePath)
          throws PersistentNodeException {
        try {
            waitForInitialCreate(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            setData(f.apply(data));
            return this;
        } catch (Exception e) {
            Throwables.propagateIfPossible(e, PersistentNodeException.class);
            throw new PersistentNodeException("Failed upating ZK Node " + basePath + " to " + data);
        }
    }

    public static class PersistentNodeException extends Exception {

        public PersistentNodeException() {
        }

        public PersistentNodeException(String message) {
            super(message);
        }
    }
}
