flowchart TB
  subgraph EDGE["Edge"]
    C[Client Apps / CLI]
    LB[Load Balancer / Reverse Proxy]
    C --> LB
  end

  subgraph API["API Layer (KV Server)"]
    S[HTTP Server<br/>(cpp-httplib)]
    R[Request Router & Validator]
    W[Worker Pool (optional)]
    M[Metrics & Logger]
    LB --> S
    S --> R
    R --> W
    R --> M
  end

  subgraph CACHE["Cache Layer"]
    LRU[In-process LRU Cache<br/>(unordered_map + list)]
    REDIS[(Optional: Redis)]
    S --> LRU
    S --> REDIS
  end

  subgraph STORAGE["Storage Layer"]
    POOL[DB Connection Pool (PgBouncer)]
    PG[(PostgreSQL<br/>libpqxx)]
    S --> POOL
    POOL --> PG
  end

  subgraph SUPPORT["Infra & Observability"]
    PROM[Prometheus / Grafana]
    LOG[Logging Stack (ELK / Loki)]
    SECRETS[Secrets Manager]
    S --> PROM
    S --> LOG
    S --> SECRETS
  end

  %% Cache fallback arrows
  LRU -- hit --> S
  LRU -- miss --> PG
