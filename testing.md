# Testing guide (Bento / CCDI Portal WebService)

This document describes where tests live, how the build separates unit vs integration runs, and practical steps for adding new tests.

---

## Folder structure

Tests follow the standard Maven layout and mirror production packages under `src/main/java`.

```
src/test/
├── java/
│   └── gov/
│       └── nih/
│           └── nci/
│               ├── bento/                      # Core framework tests (e.g. controllers)
│               │   └── IndexControllerTest.java
│               ├── bento_ri/                   # Application-specific code tests (mirror `gov.nih.nci.bento_ri`)
│               │   └── service/
│               │       └── InventoryESServiceBuildFacetFilterQueryTest.java
│               └── integration/                # Integration-style tests (Failsafe naming conventions)
│                   └── OpenSearchIntegrationTest.java
└── resources/
    ├── application-test.properties             # Optional overrides for `@SpringBootTest` / test profiles
    └── application-integration.properties
```

| Location | Purpose |
|----------|---------|
| `src/test/java/.../bento/` | Unit tests for shared Bento components (e.g. web layer) when not tied to `bento_ri` only. |
| `src/test/java/.../bento_ri/...` | Unit tests for `bento_ri` packages; keep the same subpackages as `src/main` (`service`, `model`, etc.). |
| `src/test/java/.../integration/` | Tests that need external services, real HTTP, or a live OpenSearch; use Failsafe naming (see below). |
| `src/test/resources/` | `application-*.properties`, fixtures, and other classpath resources for tests. |

---

## How Maven runs tests

The project uses two plugins with different purposes:

| Plugin | Phase (typical) | What runs | How to run |
|--------|-----------------|-----------|------------|
| **maven-surefire-plugin** | `test` | **Unit tests** | `./mvnw test` |
| **maven-failsafe-plugin** | `verify` | **Integration tests** | `./mvnw verify` (integration tests run after the package is built) |

**Surefire** is configured to **exclude** classes that look like integration tests:

- `**/*IntegrationTest.java`
- `**/*IT.java`

Those names are reserved for **Failsafe** so they are not executed during `./mvnw test` (fast local/CI unit runs). Failsafe **includes** the same patterns so they run on `verify`.

**Naming rules (important):**

- **Unit test class names:** `*Test.java` (e.g. `InventoryESServiceBuildFacetFilterQueryTest.java`) — run by Surefire, **not** ending in `IT` or `IntegrationTest` unless you intend Failsafe-only.
- **Integration test class names:** `*IntegrationTest.java` or `*IT.java` — run by Failsafe, excluded from default `test` phase.

---

## Standard procedures

### 1. Plain JUnit 5 unit test (no Spring)

Use for pure functions, builders, and logic that does not need a container.

1. Create a class under the package that mirrors the class under test, e.g. `src/test/java/gov/nih/nci/bento_ri/service/...`.
2. Name the class `SomethingTest` (ends with `Test`, not `Tests` required by default Surefire in this project—Surefire includes `*Test` by default).
3. Use JUnit Jupiter: `@Test` from `org.junit.jupiter.api`, assertions from `org.junit.jupiter.api.Assertions`.
4. Prefer testing **outputs** (maps, strings, JSON) rather than implementation details when building OpenSearch DSL—serialize with Gson if production code does.

```java
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MyUtilityTest {
    @Test
    void computesExpectedValue() {
        assertEquals(42, MyUtility.answer());
    }
}
```

Run: `./mvnw test -Dtest=MyUtilityTest`

---

### 2. Unit test with Mockito

Use when collaborating beans or infrastructure must be stubbed (no network).

1. Add `@ExtendWith(MockitoExtension.class)` on the test class (JUnit 5 + Mockito integration).
2. Annotate dependencies with `@Mock` and the subject with `@InjectMocks`, **or** construct mocks manually if the class under test uses a non-standard constructor (see §5).

Run a single class: `./mvnw test -Dtest=gov.nih.nci.bento.MyServiceTest`

---

### 3. Spring MVC slice test (`@WebMvcTest`) — optional

Use when exercising controllers with `MockMvc` and a limited Spring context.

Existing pattern in `IndexControllerTest`: **standalone** `MockMvc` without loading the full application (`MockMvcBuilders.standaloneSetup(...)`). That keeps tests fast and avoids pulling Neo4j/ES configuration.

If you need `@WebMvcTest`, place properties under `src/test/resources` and activate profiles as needed.

---

### 4. Spring Boot integration test (`@SpringBootTest`)

Use sparingly for full application context (slow). Typical steps:

1. Add or reuse `src/test/resources/application-test.properties` with safe URLs/credentials (or mocks).
2. Annotate the test class with `@SpringBootTest` and optionally `@ActiveProfiles("test")` if you introduce a `test` profile.
3. Prefer narrowing scope (`@DataJpaTest`, `@JsonTest`) when possible instead of full boot.

Run: `./mvnw test` (or a dedicated class with `-Dtest=...`).

---

### 5. Testing a Spring `@Service` with a private constructor or heavy client setup

Some services (e.g. `InventoryESService`) build real `RestClient` instances in the constructor. For **unit** tests of methods that only build in-memory structures:

1. **Mock** `ConfigurationDAO` (or the minimum config the constructor reads) and point ES to `localhost` with `isEsSignRequests() == false` if that path avoids AWS signing.
2. If the constructor is not visible, use **reflection** `getDeclaredConstructor(...).setAccessible(true)` to instantiate, then **close** the client in `@AfterEach` to avoid leaking resources (see `InventoryESServiceBuildFacetFilterQueryTest`).

This keeps tests in the **unit** phase without a real OpenSearch.

---

### 6. Integration test (OpenSearch, HTTP, real services)

1. Place the class under `src/test/java/.../integration/` (or any package; location is convention only).
2. Name the class `*IntegrationTest.java` or `*IT.java` so **Surefire excludes it** and **Failsafe runs it** on `verify`.
3. Read connection settings from **environment variables** (or test properties) so CI (e.g. GitHub Actions) can inject `ES_HOST`, `ES_PORT`, `ES_SCHEME` as in `OpenSearchIntegrationTest`.
4. Open and close `RestClient` (or other clients) in `@BeforeEach` / `@AfterEach`.

Run integration tests only:

```bash
./mvnw verify
```

To run a single Failsafe test class (after compile):

```bash
./mvnw verify -Dit.test=OpenSearchIntegrationTest
```

(Exact property names may vary by Failsafe version; use `-Dit.test=ClassName` for a single IT class.)

---

## Conventions checklist

- **Package alignment:** `gov.nih.nci.bento_ri.service.Foo` → `gov.nih.nci.bento_ri.service.FooTest` under `src/test/java`.
- **One conceptual behavior per test method** where practical; use descriptive method names (`participants_noFilters_yieldsMatchAll`).
- **Avoid** naming integration-style classes `*Test.java` without exclusion—they would run on every `./mvnw test` and slow or break CI without services.
- **JSON / OpenSearch bodies:** After building `Map<String, Object>` query DSL, round-trip with the same **Gson** settings as production (`serializeNulls` if applicable) to ensure the payload is valid JSON.

---

## Quick reference commands

| Goal | Command |
|------|---------|
| All unit tests (Surefire) | `./mvnw test` |
| One unit test class | `./mvnw test -Dtest=FullyQualifiedOrSimpleClassName` |
| Unit tests + JaCoCo report | `./mvnw verify` (JaCoCo report is bound to `verify` in this project) |
| Include integration tests | `./mvnw verify` |

Surefire reports: `target/surefire-reports/`.  
Failsafe reports: `target/failsafe-reports/` when integration tests run.

---

## Related production paths

- Query bodies built for OpenSearch are serialized with Gson where requests are sent (e.g. count/search helpers on `ESService` / `InventoryESService`).
- Main OpenSearch query construction for inventory facets: `InventoryESService.buildFacetFilterQuery(...)`.

When extending coverage, add **unit** tests under `src/test/java` mirroring the production package, and reserve **`IntegrationTest` / `IT` suffixes** for tests that require a running cluster or external dependencies.
