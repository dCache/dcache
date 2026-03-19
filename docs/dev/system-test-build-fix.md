# System-Test First-Run Build Fix

## Problem

On a clean build (empty local Maven repository), the `packages/system-test` module could fail
to resolve `org.dcache:dcache-plugin-hsqldb:zip` because the HSQLDB plugin's assembly ZIP was
built but not _attached_ to the Maven project artifact set.

The `packages/system-test` assembly unpacks the plugin ZIP:

```xml
<dependencySet>
  <includes>
    <include>org.dcache:dcache-plugin-hsqldb:zip:*</include>
  </includes>
  <outputDirectory>plugins</outputDirectory>
  <unpack>true</unpack>
</dependencySet>
```

Without the ZIP being attached, the in-reactor dependency resolver could not find it during a
first-pass `package` run. A prior `mvn install` worked around the issue by populating the
local repository first, but that should not be required.

## Fix

**File:** `plugins/hsqldb/pom.xml`

Add `<attach>true</attach>` to the `maven-assembly-plugin` configuration so the ZIP is
attached to the project artifacts and is immediately available to downstream modules in the
same reactor build:

```xml
<plugin>
  <artifactId>maven-assembly-plugin</artifactId>
  <configuration>
    <formats>
      <format>zip</format>
    </formats>
    <appendAssemblyId>false</appendAssemblyId>
    <attach>true</attach>   <!-- added: makes ZIP available in-reactor -->
  </configuration>
  <executions>
    <execution>
      <id>make-assembly</id>
      <phase>package</phase>
      <goals>
        <goal>single</goal>
      </goals>
    </execution>
  </executions>
</plugin>
```

## Validation

After the fix, a fresh build with an empty local repository succeeds:

```bash
mvn -Dmaven.repo.local=/tmp/m2-empty-repo -pl packages/system-test -am -T1C clean package
```

The ZIP is resolved via the attached in-reactor artifact; no prior `mvn install` is needed.

## Notes

- During the `package` phase, `system-test` runs `bin/populate` which initialises HSQLDB via
  Liquibase. The verbose Liquibase output is normal.
- A `failsafe-summary.xml` with result 254 and 0 tests may appear; this is expected because
  `system-test` has no JUnit integration tests wired into Failsafe.

## Recommended Build Command

```bash
mvn -pl packages/system-test -am clean package
```

## Affected Files

| File | Change |
|---|---|
| `plugins/hsqldb/pom.xml` | Added `<attach>true</attach>` to `maven-assembly-plugin` |
