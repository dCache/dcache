# NFSv4.1/pNFS Door

## Set of sequence diagrams of pNFS flow

### Positive path
```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Door
    participant poolA
    participant PoolManager
    Client->>Door: OPEN(file), LAYOUTGET 
    Door->>PoolManager: Select Pool
    PoolManager-->>Door: 'poolA'
    Door->>poolA: Start Mover (state)
    poolA-->>Door: Ready(id)
    Door-->>Client: 'open state', 'layout'
    loop Application IO
        Client->>poolA: READ(file, state, offset, len)
        poolA-->>Client: 'bytes'
    end
    Client->>Door: LAYOUT_RETURN, CLOSE
    Door->>poolA: Kill Mover(id)
    poolA->>poolA: state invalidated
    poolA-->>Door: Trnaster Finish
    Door->>Door: Invalidate state
    Door-->>Client: OK
```

### File is offline
```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Door
    participant PoolManager
    Client->>Door: OPEN(file), LAYOUTGET 
    Door->>PoolManager: Select Pool
    PoolManager-->>Door: 'No Pools'
    Door-->>Client:  NFS4ERR_LAYOUTTRYLATER
```

### No disk and tape locations
```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Door
    participant PoolManager
    Client->>Door: OPEN(file), LAYOUTGET 
    Door-->>Client:  NFS4ERR_IO
```

### PoolManager is offline
```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Door
    participant PoolManager
    Client->>Door: OPEN(file), LAYOUTGET 
    Door--XPoolManager: Select Pool
    Door-->>Client:  NFS4ERR_DELAY
```

### Two opens
```mermaid
sequenceDiagram
    autonumber
    box HostA
    actor ClientA as App1
    actor ClientB as App2
    end
    participant Door
    participant poolA
    participant PoolManager
    ClientA->>Door: OPEN(file), LAYOUTGET 
    Door->>PoolManager: Select Pool
    PoolManager-->>Door: 'poolA'
    Door->>poolA: Start Mover (state1)
    poolA-->>Door: Ready(id)
    Door-->>ClientA: 'open state1', 'layout'

    ClientB->>Door: OPEN(file), LAYOUTGET 
    Note right of Door: By same open owner stateid and layouts are shared
    Door-->>ClientB: 'open state1', 'layout'

    loop Application IO
        ClientA->>poolA: READ(file, state1, offset, len)
        poolA-->>ClientA: 'bytes'
    end
    loop Application IO
        ClientB->>poolA: READ(file, state1, offset, len)
        poolA-->>ClientB: 'bytes'
    end
    
    ClientA->>Door: LAYOUT_RETURN, CLOSE
    Door->>poolA: Kill Mover(id)
    poolA->>poolA: state invalidated
    poolA-->>Door: Trnaster Finish
    Door->>Door: Invalidate state
    Note right of Door: Single close for all previous opens
    Door-->>ClientA: OK
```

### Competing opens (race condition)

(fixed by commit 421e1dc932c73186d52901431ec8b822fdadb508)
```mermaid
sequenceDiagram
    autonumber
    box HostA
    actor ClientA as App1
    actor ClientB as App2
    end
    participant Door
    participant poolA
    participant PoolManager
    ClientA->>Door: OPEN(file), LAYOUTGET 
    Door->>PoolManager: Select Pool
    PoolManager-->>Door: 'poolA'
    Door->>poolA: Start Mover (state1)
    poolA-->>Door: Ready(id)
    Door-->>ClientA: 'open state1', 'layout'

    loop Application IO
        ClientA->>poolA: READ(file, state1, offset, len)
        poolA-->>ClientA: 'bytes'
    end
    
    ClientA->>Door: LAYOUT_RETURN, CLOSE
    Door->>poolA: Kill Mover(id)
    
    ClientB->>Door: OPEN(file), LAYOUTGET  
    Door-->>ClientB: 'open state1', 'layout'
    
    poolA->>poolA: state invalidated
    poolA-->>Door: Trnaster Finish
    Door--xDoor: older verion of stateid is used. File stays open
    Door-->>ClientA: NFS4ERR_BAD_SEQID
    
    loop Application IO
        ClientB->>poolA: READ(file, state1, offset, len)
        poolA-->>ClientB: NFS4ERR_BAD_STATEID
        Note right of PoolA: Mover is aleady gone
    end
```

### Competing opens

(fixed by commit 421e1dc932c73186d52901431ec8b822fdadb508)
```mermaid
sequenceDiagram
    autonumber
    box HostA
    actor ClientA as App1
    actor ClientB as App2
    end
    participant Door
    participant poolA
    participant PoolManager
    ClientA->>Door: OPEN(file), LAYOUTGET
    Door->>PoolManager: Select Pool
    PoolManager-->>Door: 'poolA'
    Door->>poolA: Start Mover (state1)
    poolA-->>Door: Ready(id)
    Door-->>ClientA: 'open state1', 'layout'

    loop Application IO
        ClientA->>poolA: READ(file, state1, offset, len)
        poolA-->>ClientA: 'bytes'
    end
    
    ClientA->>Door: LAYOUT_RETURN, CLOSE
    Door->>poolA: Kill Mover(id)

    ClientB->>Door: OPEN(file), LAYOUTGET
    Door-->>ClientB: NFS4ERR_DELAY

    poolA->>poolA: state invalidated
    poolA-->>Door: Trnaster Finish
    Door--xDoor: older verion of stateid is used. File stays open
    Door-->>ClientA: NFS4ERR_BAD_SEQID

    ClientB->>Door: LAYOUTGET
    Note right of Door: Processed as regular LAYOUTGET
```