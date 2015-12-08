/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package dmg.cells.zookeeper;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.ACLCreateModeBackgroundPathAndBytesable;
import org.apache.curator.framework.api.ACLCreateModePathAndBytesable;
import org.apache.curator.framework.api.ACLPathAndBytesable;
import org.apache.curator.framework.api.ACLable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.BackgroundVersionable;
import org.apache.curator.framework.api.ChildrenDeletable;
import org.apache.curator.framework.api.CreateBackgroundModeACLable;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.ExistsBuilderMain;
import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.GetDataWatchBackgroundStatable;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.curator.framework.api.SetDataBackgroundVersionable;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.SyncBuilder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.VersionPathAndBytesable;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.TransactionCheckBuilder;
import org.apache.curator.framework.api.transaction.TransactionCreateBuilder;
import org.apache.curator.framework.api.transaction.TransactionDeleteBuilder;
import org.apache.curator.framework.api.transaction.TransactionSetDataBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CDC;

import org.dcache.util.SequentialExecutor;

/**
 * Wrapper for CuratorFramework that maintains the CDC as well as injects a default
 * executor for callbacks.
 */
public class CellCuratorFramework implements CuratorFramework
{
    private final CuratorFramework inner;
    private final Executor executor;

    public CellCuratorFramework(CuratorFramework inner, Executor executor)
    {
        this.inner = inner;
        this.executor = new SequentialExecutor(executor);
    }

    protected static BackgroundCallback wrap(BackgroundCallback callback)
    {
        CDC cdc = new CDC();
        return (client, event) -> {
            try (CDC ignore = cdc.restore()) {
                callback.processResult(client, event);
            }
        };
    }

    @Override
    public void start()
    {
        inner.start();
    }

    @Override
    public void close()
    {
        inner.close();
    }

    @Override
    public CuratorFrameworkState getState()
    {
        return inner.getState();
    }

    @Override
    @Deprecated
    public boolean isStarted()
    {
        return inner.isStarted();
    }

    @Override
    public CreateBuilder create()
    {
        return new CreateBuilderDecorator(inner.create());
    }

    @Override
    public DeleteBuilder delete()
    {
        return new DeleteBuilderDecorator(inner.delete());
    }

    @Override
    public ExistsBuilder checkExists()
    {
        return new ExistsBuilderDecorator(inner.checkExists());
    }

    @Override
    public GetDataBuilder getData()
    {
        return new GetDataBuilderDecorator(inner.getData());
    }

    @Override
    public SetDataBuilder setData()
    {
        return new SetDataBuilderDecorator(inner.setData());
    }

    @Override
    public GetChildrenBuilder getChildren()
    {
        return new GetChildrenBuilderDecorator(inner.getChildren());
    }

    @Override
    public GetACLBuilder getACL()
    {
        return new GetACLBuilderDecorator(inner.getACL());
    }

    @Override
    public SetACLBuilder setACL()
    {
        return new SetACLBuilderDecorator(inner.setACL());
    }

    @Override
    public CuratorTransaction inTransaction()
    {
        return new CuratorTransactionDecorator(inner.inTransaction());
    }

    @Override
    @Deprecated
    public void sync(String path, Object backgroundContextObject)
    {
        inner.sync(path, backgroundContextObject);
    }

    @Override
    public void createContainers(String path) throws Exception
    {
        inner.createContainers(path);
    }

    @Override
    public SyncBuilder sync()
    {
        return new SyncBuilderDecorator(inner.sync());
    }

    @Override
    public Listenable<ConnectionStateListener> getConnectionStateListenable()
    {
        return new ListenableDecorator<>(inner.getConnectionStateListenable(), executor);
    }

    @Override
    public Listenable<CuratorListener> getCuratorListenable()
    {
        return new ListenableDecorator<>(inner.getCuratorListenable(), executor);
    }

    @Override
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable()
    {
        return new ListenableDecorator<>(inner.getUnhandledErrorListenable(), executor);
    }

    @Override
    @Deprecated
    public CuratorFramework nonNamespaceView()
    {
        return new CellCuratorFramework(inner.nonNamespaceView(), executor);
    }

    @Override
    public CuratorFramework usingNamespace(String newNamespace)
    {
        return new CellCuratorFramework(inner.usingNamespace(newNamespace), executor);
    }

    @Override
    public String getNamespace()
    {
        return inner.getNamespace();
    }

    @Override
    public CuratorZookeeperClient getZookeeperClient()
    {
        /* WARNING: The client is not decorated! */
        return inner.getZookeeperClient();
    }

    @Override
    @Deprecated
    public EnsurePath newNamespaceAwareEnsurePath(String path)
    {
        return inner.newNamespaceAwareEnsurePath(path);
    }

    @Override
    public void clearWatcherReferences(Watcher watcher)
    {
        inner.clearWatcherReferences(watcher);
    }

    @Override
    public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException
    {
        return inner.blockUntilConnected(maxWaitTime, units);
    }

    @Override
    public void blockUntilConnected() throws InterruptedException
    {
        inner.blockUntilConnected();
    }

    private static class ListenableDecorator<T> implements Listenable<T>
    {
        private final Listenable<T> listenable;
        private final Executor executor;

        public ListenableDecorator(Listenable<T> listenable, Executor executor)
        {
            this.listenable = listenable;
            this.executor = executor;
        }

        @Override
        public void addListener(T listener)
        {
            addListener(listener, executor);
        }

        @Override
        public void addListener(T listener, Executor executor)
        {
            CDC cdc = new CDC();
            listenable.addListener(listener, r -> executor.execute(() -> cdc.execute(r)));
        }

        @Override
        public void removeListener(T listener)
        {
            listenable.removeListener(listener);
        }
    }

    private class CreateBuilderDecorator implements CreateBuilder
    {
        private final CreateBuilder inner;

        public CreateBuilderDecorator(CreateBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public ProtectACLCreateModePathAndBytesable<String> creatingParentsIfNeeded()
        {
            return new ProtectACLCreateModePathAndBytesableDecorator<>(inner.creatingParentsIfNeeded());
        }

        @Override
        public ProtectACLCreateModePathAndBytesable<String> creatingParentContainersIfNeeded()
        {
            return new ProtectACLCreateModePathAndBytesableDecorator<>(inner.creatingParentContainersIfNeeded());
        }

        @Override
        public ACLPathAndBytesable<String> withProtectedEphemeralSequential()
        {
            return new ACLPathAndBytesableDecorator<>(inner.withProtectedEphemeralSequential());
        }

        @Override
        public ACLCreateModeBackgroundPathAndBytesable<String> withProtection()
        {
            return new ACLCreateModeBackgroundPathAndBytesableDecorator<>(inner.withProtection());
        }

        @Override
        public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList)
        {
            return new BackgroundPathAndBytesableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public PathAndBytesable<String> inBackground()
        {
            return new PathAndBytesableDecorator<>(inner.inBackground());
        }

        @Override
        public PathAndBytesable<String> inBackground(Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(context));
        }

        @Override
        public PathAndBytesable<String> inBackground(BackgroundCallback callback)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public PathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public CreateBackgroundModeACLable compressed()
        {
            return new CreateBackgroundModeACLableDecorator(inner.compressed());
        }

        @Override
        public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode)
        {
            return new ACLBackgroundPathAndBytesableDecorator<>(inner.withMode(mode));
        }

        @Override
        public String forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public String forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class DeleteBuilderDecorator implements DeleteBuilder
    {
        private final DeleteBuilder inner;

        private DeleteBuilderDecorator(DeleteBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public BackgroundVersionable deletingChildrenIfNeeded()
        {
            return new BackgroundVersionableDecorator(inner.deletingChildrenIfNeeded());
        }

        @Override
        public ChildrenDeletable guaranteed()
        {
            return new ChildrenDeletableDecorator(inner.guaranteed());
        }

        @Override
        public Pathable<Void> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<Void> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Void forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public BackgroundPathable<Void> withVersion(int version)
        {
            return inner.withVersion(version);
        }
    }

    private class ExistsBuilderMainDecorator implements ExistsBuilderMain
    {
        protected final ExistsBuilderMain inner;

        public ExistsBuilderMainDecorator(ExistsBuilderMain inner)
        {
            this.inner = inner;
        }

        @Override
        public Pathable<Stat> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<Stat> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<Stat> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Stat> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<Stat> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Stat> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Stat forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public BackgroundPathable<Stat> watched()
        {
            return new BackgroundPathableDecorator<>(inner.watched());
        }

        @Override
        public BackgroundPathable<Stat> usingWatcher(Watcher watcher)
        {
            return new BackgroundPathableDecorator<>(inner.usingWatcher(watcher));
        }

        @Override
        public BackgroundPathable<Stat> usingWatcher(CuratorWatcher watcher)
        {
            return new BackgroundPathableDecorator<>(inner.usingWatcher(watcher));
        }
    }

    private class ExistsBuilderDecorator extends ExistsBuilderMainDecorator implements ExistsBuilder
    {
        public ExistsBuilderDecorator(ExistsBuilder inner)
        {
            super(inner);
        }

        @Override
        public ExistsBuilderMain creatingParentContainersIfNeeded()
        {
            return new ExistsBuilderMainDecorator(((ExistsBuilder) inner).creatingParentContainersIfNeeded());
        }
    }

    private class GetDataBuilderDecorator implements GetDataBuilder
    {
        private final GetDataBuilder inner;

        public GetDataBuilderDecorator(GetDataBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public Pathable<byte[]> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<byte[]> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<byte[]> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<byte[]> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<byte[]> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<byte[]> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public GetDataWatchBackgroundStatable decompressed()
        {
            return new GetDataWatchBackgroundStatableDecorator(inner.decompressed());
        }

        @Override
        public byte[] forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public WatchPathable<byte[]> storingStatIn(Stat stat)
        {
            return new WatchPathableDecorator<>(inner.storingStatIn(stat));
        }

        @Override
        public BackgroundPathable<byte[]> watched()
        {
            return new BackgroundPathableDecorator<>(inner.watched());
        }

        @Override
        public BackgroundPathable<byte[]> usingWatcher(Watcher watcher)
        {
            return new BackgroundPathableDecorator<>(inner.usingWatcher(watcher));
        }

        @Override
        public BackgroundPathable<byte[]> usingWatcher(CuratorWatcher watcher)
        {
            return new BackgroundPathableDecorator<>(inner.usingWatcher(watcher));
        }
    }

    private class SetDataBuilderDecorator implements SetDataBuilder
    {
        private final SetDataBuilder inner;

        public SetDataBuilderDecorator(SetDataBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public PathAndBytesable<Stat> inBackground()
        {
            return new PathAndBytesableDecorator<>(inner.inBackground());
        }

        @Override
        public PathAndBytesable<Stat> inBackground(Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(context));
        }

        @Override
        public PathAndBytesable<Stat> inBackground(BackgroundCallback callback)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<Stat> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public PathAndBytesable<Stat> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<Stat> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public SetDataBackgroundVersionable compressed()
        {
            return new SetDataBackgroundVersionableDecorator(inner.compressed());
        }

        @Override
        public Stat forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public Stat forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public BackgroundPathAndBytesable<Stat> withVersion(int version)
        {
            return new BackgroundPathAndBytesableDecorator<>(inner.withVersion(version));
        }
    }

    private class GetChildrenBuilderDecorator implements GetChildrenBuilder
    {
        private final GetChildrenBuilder inner;

        public GetChildrenBuilderDecorator(GetChildrenBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public Pathable<List<String>> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<List<String>> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<List<String>> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<List<String>> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<List<String>> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<List<String>> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public List<String> forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public WatchPathable<List<String>> storingStatIn(Stat stat)
        {
            return new WatchPathableDecorator<>(inner.storingStatIn(stat));
        }

        @Override
        public BackgroundPathable<List<String>> watched()
        {
            return new BackgroundPathableDecorator<>(inner.watched());
        }

        @Override
        public BackgroundPathable<List<String>> usingWatcher(Watcher watcher)
        {
            return new BackgroundPathableDecorator<>(inner.usingWatcher(watcher));
        }

        @Override
        public BackgroundPathable<List<String>> usingWatcher(CuratorWatcher watcher)
        {
            return new BackgroundPathableDecorator<>(inner.usingWatcher(watcher));
        }
    }

    private class GetACLBuilderDecorator implements GetACLBuilder
    {
        private final GetACLBuilder inner;

        public GetACLBuilderDecorator(GetACLBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public Pathable<List<ACL>> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<List<ACL>> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<List<ACL>> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<List<ACL>> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<List<ACL>> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<List<ACL>> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public List<ACL> forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public Pathable<List<ACL>> storingStatIn(Stat stat)
        {
            return new PathableDecorator<>(inner.storingStatIn(stat));
        }
    }

    private class SetACLBuilderDecorator implements SetACLBuilder
    {
        private final SetACLBuilder inner;

        public SetACLBuilderDecorator(SetACLBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public BackgroundPathable<Stat> withACL(List<ACL> aclList)
        {
            return new BackgroundPathableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public ACLable<BackgroundPathable<Stat>> withVersion(int version)
        {
            return new ACLableBackgroundPathableDecorator<>(inner.withVersion(version));
        }
    }

    private class CuratorTransactionDecorator implements CuratorTransaction
    {
        private final CuratorTransaction inner;

        public CuratorTransactionDecorator(CuratorTransaction inner)
        {
            this.inner = inner;
        }

        @Override
        public TransactionCreateBuilder create()
        {
            return new TransactionCreateBuilderDecorator(inner.create());
        }

        @Override
        public TransactionDeleteBuilder delete()
        {
            return new TransactionDeleteBuilderDecorator(inner.delete());
        }

        @Override
        public TransactionSetDataBuilder setData()
        {
            return new TransactionSetDataBuilderDecorator(inner.setData());
        }

        @Override
        public TransactionCheckBuilder check()
        {
            return new TransactionCheckBuilderDecorator(inner.check());
        }
    }

    private class TransactionCreateBuilderDecorator implements TransactionCreateBuilder
    {
        private final TransactionCreateBuilder inner;

        public TransactionCreateBuilderDecorator(TransactionCreateBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public PathAndBytesable<CuratorTransactionBridge> withACL(List<ACL> aclList)
        {
            return new PathAndBytesableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public ACLCreateModePathAndBytesable<CuratorTransactionBridge> compressed()
        {
            return new ACLCreateModePathAndBytesableDecorator<>(inner.compressed());
        }

        @Override
        public ACLPathAndBytesable<CuratorTransactionBridge> withMode(CreateMode mode)
        {
            return new ACLPathAndBytesableDecorator<>(inner.withMode(mode));
        }

        @Override
        public CuratorTransactionBridge forPath(String path, byte[] data) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path, data));
        }

        @Override
        public CuratorTransactionBridge forPath(String path) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path));
        }
    }

    private class TransactionDeleteBuilderDecorator implements TransactionDeleteBuilder
    {
        private final TransactionDeleteBuilder inner;

        public TransactionDeleteBuilderDecorator(TransactionDeleteBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public CuratorTransactionBridge forPath(String path) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path));
        }

        @Override
        public Pathable<CuratorTransactionBridge> withVersion(int version)
        {
            return new PathableCuratorTransactionBridgeDecorator(inner.withVersion(version));
        }
    }

    private class TransactionSetDataBuilderDecorator implements TransactionSetDataBuilder
    {
        private final TransactionSetDataBuilder inner;

        public TransactionSetDataBuilderDecorator(TransactionSetDataBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public VersionPathAndBytesable<CuratorTransactionBridge> compressed()
        {
            return new VersionPathAndBytesableCuratorTransactionBridgeDecorator(inner.compressed());
        }

        @Override
        public CuratorTransactionBridge forPath(String path, byte[] data) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path, data));
        }

        @Override
        public CuratorTransactionBridge forPath(String path) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path));
        }

        @Override
        public PathAndBytesable<CuratorTransactionBridge> withVersion(int version)
        {
            return new PathAndBytesableCuratorTransactionBridgeDecorator(inner.withVersion(version));
        }
    }

    private class TransactionCheckBuilderDecorator implements TransactionCheckBuilder
    {
        private final TransactionCheckBuilder inner;

        public TransactionCheckBuilderDecorator(TransactionCheckBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public CuratorTransactionBridge forPath(String path) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path));
        }

        @Override
        public Pathable<CuratorTransactionBridge> withVersion(int version)
        {
            return new PathableCuratorTransactionBridgeDecorator(inner.withVersion(version));
        }
    }

    private class CuratorTransactionBridgeDecorator implements CuratorTransactionBridge
    {
        private final CuratorTransactionBridge inner;

        public CuratorTransactionBridgeDecorator(CuratorTransactionBridge inner)
        {
            this.inner = inner;
        }

        @Override
        public CuratorTransactionFinal and()
        {
            return new CuratorTransactionFinalDecorator(inner.and());
        }
    }

    private class CuratorTransactionFinalDecorator implements CuratorTransactionFinal
    {
        private final CuratorTransactionFinal inner;

        public CuratorTransactionFinalDecorator(CuratorTransactionFinal inner)
        {
            this.inner = inner;
        }

        @Override
        public Collection<CuratorTransactionResult> commit() throws Exception
        {
            return inner.commit();
        }

        @Override
        public TransactionCreateBuilder create()
        {
            return new TransactionCreateBuilderDecorator(inner.create());
        }

        @Override
        public TransactionDeleteBuilder delete()
        {
            return new TransactionDeleteBuilderDecorator(inner.delete());
        }

        @Override
        public TransactionSetDataBuilder setData()
        {
            return new TransactionSetDataBuilderDecorator(inner.setData());
        }

        @Override
        public TransactionCheckBuilder check()
        {
            return new TransactionCheckBuilderDecorator(inner.check());
        }
    }

    private class SyncBuilderDecorator implements SyncBuilder
    {
        private final SyncBuilder inner;

        public SyncBuilderDecorator(SyncBuilder inner)
        {
            this.inner = inner;
        }

        @Override
        public Pathable<Void> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<Void> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Void forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class ProtectACLCreateModePathAndBytesableDecorator<T> implements ProtectACLCreateModePathAndBytesable<T>
    {
        private final ProtectACLCreateModePathAndBytesable<T> inner;

        public ProtectACLCreateModePathAndBytesableDecorator(ProtectACLCreateModePathAndBytesable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public ACLCreateModeBackgroundPathAndBytesable<String> withProtection()
        {
            return new ACLCreateModeBackgroundPathAndBytesableDecorator<>(inner.withProtection());
        }

        @Override
        public BackgroundPathAndBytesable<T> withACL(List<ACL> aclList)
        {
            return new BackgroundPathAndBytesableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public PathAndBytesable<T> inBackground()
        {
            return new PathAndBytesableDecorator<T>(inner.inBackground());
        }

        @Override
        public PathAndBytesable<T> inBackground(Object context)
        {
            return new PathAndBytesableDecorator<T>(inner.inBackground(context));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback)
        {
            return new PathAndBytesableDecorator<T>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathAndBytesableDecorator<T>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathAndBytesableDecorator<T>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Object context,
                                                Executor executor)
        {
            return new PathAndBytesableDecorator<T>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public ACLBackgroundPathAndBytesable<T> withMode(CreateMode mode)
        {
            return new ACLBackgroundPathAndBytesableDecorator<T>(inner.withMode(mode));
        }

        @Override
        public T forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private static class ACLPathAndBytesableDecorator<T> implements ACLPathAndBytesable<T>
    {
        private final ACLPathAndBytesable<T> inner;

        public ACLPathAndBytesableDecorator(ACLPathAndBytesable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public PathAndBytesable<T> withACL(List<ACL> aclList)
        {
            return new PathAndBytesableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public T forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class ACLCreateModeBackgroundPathAndBytesableDecorator<T> implements ACLCreateModeBackgroundPathAndBytesable<T>
    {
        private final ACLCreateModeBackgroundPathAndBytesable<T> inner;

        public ACLCreateModeBackgroundPathAndBytesableDecorator(ACLCreateModeBackgroundPathAndBytesable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public BackgroundPathAndBytesable<T> withACL(List<ACL> aclList)
        {
            return new BackgroundPathAndBytesableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public PathAndBytesable<T> inBackground()
        {
            return new PathAndBytesableDecorator<>(inner.inBackground());
        }

        @Override
        public PathAndBytesable<T> inBackground(Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(context));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public ACLBackgroundPathAndBytesable<T> withMode(CreateMode mode)
        {
            return new ACLBackgroundPathAndBytesableDecorator<>(inner.withMode(mode));
        }

        @Override
        public T forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class BackgroundPathAndBytesableDecorator<T> implements BackgroundPathAndBytesable<T>
    {
        private final BackgroundPathAndBytesable<T> inner;

        public BackgroundPathAndBytesableDecorator(BackgroundPathAndBytesable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public PathAndBytesable<T> inBackground()
        {
            return new PathAndBytesableDecorator<>(inner.inBackground());
        }

        @Override
        public PathAndBytesable<T> inBackground(Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(context));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public T forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private static class PathAndBytesableDecorator<T> implements PathAndBytesable<T>
    {
        private final PathAndBytesable<T> inner;

        public PathAndBytesableDecorator(PathAndBytesable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public T forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class PathAndBytesableCuratorTransactionBridgeDecorator implements PathAndBytesable<CuratorTransactionBridge>
    {
        private final PathAndBytesable<CuratorTransactionBridge> inner;

        public PathAndBytesableCuratorTransactionBridgeDecorator(
                PathAndBytesable<CuratorTransactionBridge> inner)
        {
            this.inner = inner;
        }

        @Override
        public CuratorTransactionBridge forPath(String path, byte[] data) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path, data));
        }

        @Override
        public CuratorTransactionBridge forPath(String path) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path));
        }
    }

    private class CreateBackgroundModeACLableDecorator implements CreateBackgroundModeACLable
    {
        private final CreateBackgroundModeACLable inner;

        public CreateBackgroundModeACLableDecorator(CreateBackgroundModeACLable inner)
        {
            this.inner = inner;
        }

        @Override
        public ACLCreateModePathAndBytesable<String> creatingParentsIfNeeded()
        {
            return new ACLCreateModePathAndBytesableDecorator<>(inner.creatingParentsIfNeeded());
        }

        @Override
        public ACLCreateModePathAndBytesable<String> creatingParentContainersIfNeeded()
        {
            return new ACLCreateModePathAndBytesableDecorator<>(inner.creatingParentContainersIfNeeded());
        }

        @Override
        public ACLPathAndBytesable<String> withProtectedEphemeralSequential()
        {
            return new ACLPathAndBytesableDecorator<>(inner.withProtectedEphemeralSequential());
        }

        @Override
        public BackgroundPathAndBytesable<String> withACL(List<ACL> aclList)
        {
            return new BackgroundPathAndBytesableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public PathAndBytesable<String> inBackground()
        {
            return new PathAndBytesableDecorator<>(inner.inBackground());
        }

        @Override
        public PathAndBytesable<String> inBackground(Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(context));
        }

        @Override
        public PathAndBytesable<String> inBackground(BackgroundCallback callback)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public PathAndBytesable<String> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<String> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public ACLBackgroundPathAndBytesable<String> withMode(CreateMode mode)
        {
            return new ACLBackgroundPathAndBytesableDecorator<>(inner.withMode(mode));
        }

        @Override
        public String forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public String forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class ACLBackgroundPathAndBytesableDecorator<T> implements ACLBackgroundPathAndBytesable<T>
    {
        private final ACLBackgroundPathAndBytesable<T> inner;

        public ACLBackgroundPathAndBytesableDecorator(ACLBackgroundPathAndBytesable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public BackgroundPathAndBytesable<T> withACL(List<ACL> aclList)
        {
            return new BackgroundPathAndBytesableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public PathAndBytesable<T> inBackground()
        {
            return new PathAndBytesableDecorator<>(inner.inBackground());
        }

        @Override
        public PathAndBytesable<T> inBackground(Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(context));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<T> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public T forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private static class ACLCreateModePathAndBytesableDecorator<T> implements ACLCreateModePathAndBytesable<T>
    {
        private final ACLCreateModePathAndBytesable<T> inner;

        public ACLCreateModePathAndBytesableDecorator(ACLCreateModePathAndBytesable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public PathAndBytesable<T> withACL(List<ACL> aclList)
        {
            return new PathAndBytesableDecorator<>(inner.withACL(aclList));
        }

        @Override
        public ACLPathAndBytesable<T> withMode(CreateMode mode)
        {
            return new ACLPathAndBytesableDecorator<>(inner.withMode(mode));
        }

        @Override
        public T forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class BackgroundVersionableDecorator implements BackgroundVersionable
    {
        private final BackgroundVersionable inner;

        private BackgroundVersionableDecorator(BackgroundVersionable inner)
        {
            this.inner = inner;
        }

        @Override
        public Pathable<Void> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<Void> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Void forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public BackgroundPathable<Void> withVersion(int version)
        {
            return inner.withVersion(version);
        }
    }

    private class ChildrenDeletableDecorator implements ChildrenDeletable
    {
        private final ChildrenDeletable inner;

        private ChildrenDeletableDecorator(ChildrenDeletable inner)
        {
            this.inner = inner;
        }

        @Override
        public BackgroundVersionable deletingChildrenIfNeeded()
        {
            return new BackgroundVersionableDecorator(inner.deletingChildrenIfNeeded());
        }

        @Override
        public Pathable<Void> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<Void> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<Void> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Void forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public BackgroundPathable<Void> withVersion(int version)
        {
            return inner.withVersion(version);
        }
    }

    private static class PathableDecorator<T> implements Pathable<T>
    {
        private final Pathable<T> inner;

        public PathableDecorator(Pathable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class PathableCuratorTransactionBridgeDecorator implements Pathable<CuratorTransactionBridge>
    {
        private final Pathable<CuratorTransactionBridge> inner;

        public PathableCuratorTransactionBridgeDecorator(
                Pathable<CuratorTransactionBridge> inner)
        {
            this.inner = inner;
        }

        @Override
        public CuratorTransactionBridge forPath(String path) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(path));
        }
    }

    private class BackgroundPathableDecorator<T> implements BackgroundPathable<T>
    {
        private final BackgroundPathable<T> inner;

        public BackgroundPathableDecorator(BackgroundPathable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public Pathable<T> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<T> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<T> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<T> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<T> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<T> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }
    }

    private class GetDataWatchBackgroundStatableDecorator implements GetDataWatchBackgroundStatable
    {
        private final GetDataWatchBackgroundStatable inner;

        public GetDataWatchBackgroundStatableDecorator(GetDataWatchBackgroundStatable inner)
        {
            this.inner = inner;
        }

        @Override
        public Pathable<byte[]> inBackground()
        {
            return new PathableDecorator<>(inner.inBackground());
        }

        @Override
        public Pathable<byte[]> inBackground(Object context)
        {
            return new PathableDecorator<>(inner.inBackground(context));
        }

        @Override
        public Pathable<byte[]> inBackground(BackgroundCallback callback)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<byte[]> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Pathable<byte[]> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public Pathable<byte[]> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public byte[] forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public WatchPathable<byte[]> storingStatIn(Stat stat)
        {
            return new WatchPathableDecorator<>(inner.storingStatIn(stat));
        }

        @Override
        public BackgroundPathable<byte[]> watched()
        {
            return new BackgroundPathableDecorator<>(inner.watched());
        }

        @Override
        public BackgroundPathable<byte[]> usingWatcher(Watcher watcher)
        {
            return new BackgroundPathableDecorator<>(inner.usingWatcher(watcher));
        }

        @Override
        public BackgroundPathable<byte[]> usingWatcher(CuratorWatcher watcher)
        {
            return new BackgroundPathableDecorator<>(inner.usingWatcher(watcher));
        }
    }

    private static class WatchPathableDecorator<T> implements WatchPathable<T>
    {
        private final WatchPathable<T> inner;

        public WatchPathableDecorator(WatchPathable<T> inner)
        {
            this.inner = inner;
        }

        @Override
        public T forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public Pathable<T> watched()
        {
            return new PathableDecorator<>(inner.watched());
        }

        @Override
        public Pathable<T> usingWatcher(Watcher watcher)
        {
            return new PathableDecorator<>(inner.usingWatcher(watcher));
        }

        @Override
        public Pathable<T> usingWatcher(CuratorWatcher watcher)
        {
            return new PathableDecorator<>(inner.usingWatcher(watcher));
        }
    }

    private class SetDataBackgroundVersionableDecorator implements SetDataBackgroundVersionable
    {
        private final SetDataBackgroundVersionable inner;

        public SetDataBackgroundVersionableDecorator(SetDataBackgroundVersionable inner)
        {
            this.inner = inner;
        }

        @Override
        public PathAndBytesable<Stat> inBackground()
        {
            return new PathAndBytesableDecorator<>(inner.inBackground());
        }

        @Override
        public PathAndBytesable<Stat> inBackground(Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(context));
        }

        @Override
        public PathAndBytesable<Stat> inBackground(BackgroundCallback callback)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<Stat> inBackground(BackgroundCallback callback, Object context)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public PathAndBytesable<Stat> inBackground(BackgroundCallback callback, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), executor));
        }

        @Override
        public PathAndBytesable<Stat> inBackground(BackgroundCallback callback, Object context, Executor executor)
        {
            return new PathAndBytesableDecorator<>(inner.inBackground(wrap(callback), context, executor));
        }

        @Override
        public Stat forPath(String path, byte[] data) throws Exception
        {
            return inner.forPath(path, data);
        }

        @Override
        public Stat forPath(String path) throws Exception
        {
            return inner.forPath(path);
        }

        @Override
        public BackgroundPathAndBytesable<Stat> withVersion(int version)
        {
            return new BackgroundPathAndBytesableDecorator<>(inner.withVersion(version));
        }
    }

    private class ACLableBackgroundPathableDecorator<T> implements ACLable<BackgroundPathable<T>>
    {
        private final ACLable<BackgroundPathable<T>> inner;

        public ACLableBackgroundPathableDecorator(ACLable<BackgroundPathable<T>> inner)
        {
            this.inner = inner;
        }

        @Override
        public BackgroundPathable<T> withACL(List<ACL> aclList)
        {
            return new BackgroundPathableDecorator<>(inner.withACL(aclList));
        }
    }

    private class VersionPathAndBytesableCuratorTransactionBridgeDecorator implements VersionPathAndBytesable<CuratorTransactionBridge>
    {
        private final VersionPathAndBytesable<CuratorTransactionBridge> inner;

        public VersionPathAndBytesableCuratorTransactionBridgeDecorator(VersionPathAndBytesable<CuratorTransactionBridge> inner)
        {
            this.inner = inner;
        }

        @Override
        public CuratorTransactionBridge forPath(String s, byte[] bytes) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(s, bytes));
        }

        @Override
        public CuratorTransactionBridge forPath(String s) throws Exception
        {
            return new CuratorTransactionBridgeDecorator(inner.forPath(s));
        }

        @Override
        public PathAndBytesable<CuratorTransactionBridge> withVersion(int i)
        {
            return new PathAndBytesableDecorator<>(inner.withVersion(i));
        }
    }
}
