# Spring Framework 6.x Upgrade Feasibility

Analysis of what would be required to upgrade dCache from Spring Framework 5.3.x to the
latest Spring 6.x series.

_Analysis date: September 2025. Spring target version at time of writing: 6.2.11._

---

## Current State

| Item | Version |
|---|---|
| Spring Framework | 5.3.39 |
| Java | 17 (meets Spring 6.x minimum) |
| Jersey | 2.41 (with `jersey-spring5`) |
| Spring Integration Kafka | 5.5.11 |
| Spring Kafka | 2.9.11 |

### Spring Modules in Use

- spring-beans, spring-context, spring-core, spring-expression
- spring-jdbc, spring-orm, spring-web, spring-aspects, spring-tx
- spring-integration-kafka, spring-kafka
- Jersey Spring integration (`jersey-spring5`)
- `spring-plugin-core`

---

## Key Compatibility Issue: javax → jakarta Migration

Spring 6.x adopts **Jakarta EE** specifications and requires all `javax.*` web/servlet APIs to
be replaced with `jakarta.*` equivalents. This is the most disruptive change.

### Import changes required

```java
// Before (javax)
import javax.servlet.*;
import javax.ws.rs.*;
import javax.annotation.*;
import javax.inject.*;
import javax.validation.*;

// After (jakarta)
import jakarta.servlet.*;
import jakarta.ws.rs.*;
import jakarta.annotation.*;
import jakarta.inject.*;
import jakarta.validation.*;
```

---

## Dependency Updates Required

### Core framework

```xml
<version.spring>6.2.11</version.spring>
<version.spring-integration-kafka>6.3.7</version.spring-integration-kafka>
<version.spring_kafka>3.3.3</version.spring_kafka>
```

### Jersey

Jersey 2.x only supports `javax.*` APIs. Spring 6.x requires Jersey 3.x:

```xml
<!-- Current -->
<version.jersey>2.41</version.jersey>
<artifactId>jersey-spring5</artifactId>
<artifactId>swagger-jersey2-jaxrs</artifactId>

<!-- Required -->
<version.jersey>3.1.9</version.jersey>
<artifactId>jersey-spring6</artifactId>
<artifactId>swagger-jersey3-jaxrs</artifactId>
```

> **Note:** `jersey-spring6` does not exist in Jersey 2.x and requires the Jersey 3.x migration.

### Other dependencies to review

- Jackson: consider 2.15+ for Jakarta EE support
- Jetty: may need upgrade for Jakarta servlet API
- All `javax.*` web dependencies → `jakarta.*`

---

## Affected Modules

| Module | Impact |
|---|---|
| `modules/dcache-frontend` | `jersey-spring5` → `jersey-spring6`; all JAX-RS annotations |
| `modules/dcache-webdav` | Servlet/JAX-RS annotation migration |
| `modules/dcache-srm` | SRM web services; servlet migration |
| `modules/dcache-chimera` | spring-tx, spring-jdbc (low risk) |
| `modules/dcache-resilience` | spring-tx (low risk) |
| `modules/chimera` | spring-tx, spring-jdbc (low risk) |
| `modules/cells` | spring-context (low risk) |
| `modules/gplazma2-fermi` | spring-beans (low risk) |
| `modules/dcache` | All Spring modules (core impact) |

---

## Risk Assessment

### High

1. **javax → jakarta migration** — breaking change across multiple modules
2. **Jersey 3.x upgrade** — significant JAX-RS API changes
3. **REST endpoint compatibility** — all existing REST APIs need regression testing
4. **Third-party consumers** — external systems using dCache HTTP/REST APIs need notice

### Medium

1. **Spring 6.x behavioural changes** — transaction management, AOP
2. **Build pipeline** — Maven plugin and dependency compatibility
3. **Test framework** — Jersey Test Framework migration

### Low

1. **Core Spring IoC/DI** — basic container usage is stable across versions
2. **Database operations** — Spring JDBC/ORM APIs are stable
3. **Internal business logic** — limited direct Spring Framework surface area

---

## Recommended Migration Strategy

### Phase 1 — Foundation

1. Audit full dependency tree for transitive `javax.*` consumers
2. Migrate `modules/dcache-frontend`: Jersey 3.x + `javax.*` → `jakarta.*`
3. Migrate `modules/dcache-webdav` and `modules/dcache-srm`
4. Validate all REST endpoints

### Phase 2 — Spring Framework Update

1. Update Spring to 6.2.x
2. Update Spring Integration Kafka and Spring Kafka
3. Validate Spring context initialisation
4. Validate transaction management and AOP

### Phase 3 — Integration & Testing

1. Full unit and integration test pass
2. REST API endpoint regression testing
3. Performance benchmarking
4. Update API documentation

### Phase 4 — Deployment Preparation

1. CI/CD pipeline adjustments
2. Docker image updates
3. Communication to downstream API consumers

---

## Conclusion

The upgrade is **technically feasible** given the Java 17 baseline, but requires substantial
effort due to the `javax` → `jakarta` namespace migration, which cascades through web
modules and their dependencies (Jersey, Jetty, Jackson). The largest single risk item is the
Jersey 2.x → 3.x migration in `dcache-frontend`.

The safest approach is to perform the Jersey/Jakarta migration first on a dedicated branch,
fully validate the REST API, and then layer the Spring 6.x core update on top.
