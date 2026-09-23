# Repository Guidelines

## Project Overview

This is the **LMAX Disruptor** — a high-performance inter-thread messaging library for Java, version `4.0.0-SNAPSHOT` (group `com.lmax`, artifact `disruptor`). It provides a lock-free ring buffer (`RingBuffer`) with single- and multi-producer sequencers, pluggable wait strategies, and a fluent DSL for wiring event processors into consumer dependency graphs. This repository is a fork of `LMAX-Exchange/disruptor`; the upstream docs at https://lmax-exchange.github.io/disruptor/ remain the canonical user reference.

## Project Structure & Module Organization

Single-module Java library built with Gradle (Groovy DSL). Key locations:

- `src/main/java/com/lmax/disruptor` — production code. The root package holds the core (`RingBuffer`, `Sequence`, `Sequencer` implementations, wait strategies, batch event processors, rewind support); `dsl/` contains the fluent consumer API (`Disruptor`, `EventHandlerGroup`, `ConsumerRepository`); `util/` contains low-level helpers (`Util`, `DaemonThreadFactory`, `ThreadHints`).
- `src/test/java` — JUnit 5 unit and stress tests mirroring the main packages, plus `support/` test fixtures and `alternatives/`.
- `src/examples/java` — runnable usage examples (extra source set compiled against `main`).
- `src/perftest/java` — custom throughput/latency performance tests (uses HdrHistogram); packaged via `perfJar`.
- `src/jmh/java` — JMH microbenchmarks (JMH 1.35, plus OpenHFT affinity).
- `src/jcstress/java` — JCStress concurrency correctness probes (jcstress-core pinned to 0.11 on purpose — see `gradle/jcstress.gradle` for why).
- `src/docs/asciidoc/en` — AsciiDoc user/developer guide and changelog sources; `CHANGELOG.adoc` at the root just includes `src/docs/asciidoc/en/changelog.adoc`.
- `gradle/` — reusable build scripts (`maven.gradle`, `perf.gradle`, `jmh.gradle`, `asciidoc.gradle`, `jcstress.gradle`) applied from the root `build.gradle`.
- `config/checkstyle/` — Checkstyle rules (`checkstyle.xml`) and suppressions (`suppress.xml`).
- `.github/workflows/` — CI (see below); `.githooks/pre-commit` — the repo pre-commit hook.
- Treat `build/` and `.gradle/` as generated output.

Key configuration files: `build.gradle`, `settings.gradle` (Foojay toolchain resolver plugin), `gradle/wrapper/` (pinned Gradle 9.7.1), `.editorconfig`, `config/checkstyle/checkstyle.xml`.

## Technology Stack & Runtime Architecture

- Pure Java library with **zero runtime dependencies** (the published jar imports no packages — see the bnd `Import-Package: '!*'` configuration in `build.gradle`); Apache 2.0 licensed.
- Bytecode targets **Java 11** (`--release 11` on all compile tasks) via a Gradle toolchain; Gradle itself requires **JDK 17+** to run. Default toolchain is Java 11; override with `-Pdisruptor.javaToolchain=<n>` to compile and test on another JDK while keeping Java 11 bytecode. Missing JDKs are auto-provisioned by the Foojay resolver.
- Build plugins: `java-library`, `maven-publish`, `signing`, `checkstyle` (Checkstyle 10.4), `me.champeau.jmh` 0.7.3, `io.github.reyerizo.gradle.jcstress` 1.0.0, `org.asciidoctor.jvm.convert` 4.0.5, `biz.aQute.bnd.builder` 7.4.0 (OSGi bundle metadata).
- The jar carries `Automatic-Module-Name: com.lmax.disruptor` and reproducible-build settings (no `Built-By` manifest attribute, fixed Javadoc copyright year).

## Build, Test, and Development Commands

Always use the checked-in wrapper (`./gradlew`), not a system Gradle.

- `./gradlew clean build` — full build: compile, Checkstyle, tests, Javadoc/sources jars.
- `./gradlew test` — run the unit suite (JUnit Platform, parallel execution enabled, forks capped at half the available CPUs).
- `./gradlew check` — verification tasks including Checkstyle.
- `./gradlew jcstress -Pmode=quick` — concurrency probes; use stronger modes (`-Pmode=default`, etc.) for release-level validation.
- `./gradlew jmh` — JMH benchmarks; JSON results land in `build/reports/jmh/result.json`. `./gradlew jmhProfilers` lists available profilers.
- `./gradlew perfJar` — build an executable-ish jar of the custom performance tests (`archiveAppendix: 'perf'`).
- `./gradlew asciidoctor` — render the user/developer documentation (also bundles Javadoc and `src/docs/resources` + `src/docs/files`).
- `./gradlew setUpGitHooks` — point `core.hooksPath` at `.githooks/`; the pre-commit hook stashes unstaged changes and runs `./gradlew check test -q`.
- `./gradlew publish` — publish to Sonatype OSSRH (requires `sonatypeUsername`/`sonatypePassword` properties and GPG signing; signing is skipped for local builds and `publishToMavenLocal`).

## Coding Style & Naming Conventions

Follow `.editorconfig`: UTF-8, LF endings, four-space indentation, trimmed trailing whitespace, **Allman-style braces** (opening brace on the next line for classes, methods, and blocks), and `insert_final_newline = false`. Keep lines near 120 characters; Checkstyle permits up to 200. Use standard Java naming (`UpperCamelCase` types, `lowerCamelCase` members — Checkstyle enforces `^[a-z][a-zA-Z0-9_]*$` for members — `UPPER_SNAKE_CASE` constants), single-class imports only (no wildcards), and Javadoc on public APIs. This is a mechanically sympathetic, allocation-sensitive codebase: preserve cache-line padding (`Sequence`, `RingBuffer` fields), avoid allocations on hot paths, and justify any change to sequencing or memory-visibility patterns with benchmarks.

## Testing Guidelines

Tests use JUnit Jupiter 5.9 and Hamcrest 2.2. Name test classes `*Test` and place them beside the corresponding package path under `src/test/java`. The suite runs in parallel (`junit.jupiter.execution.parallel.*` system properties are set in the `test` task), so tests must be thread-safe and independent. Add focused regression tests for behavior changes; there is no coverage threshold — correctness, race safety, and performance evidence matter more than percentages. Run `./gradlew build` before submitting, plus `./gradlew jcstress -Pmode=quick` or JMH when touching sequencing, memory visibility, wait strategies, or other hot paths.

## CI, Release & Security Considerations

- GitHub Actions: `gradle-build.yml` builds on push/PR against a Java 11/17 matrix (passing `-Pdisruptor.javaToolchain` and toolchain paths); `jcstress-quick.yml` runs quick JCStress, `jcstress-manual.yml` runs heavier modes on demand; `codeql-analysis.yml` performs CodeQL scanning; `asciidoc.yml` / `asciidoc-build-only.yml` handle docs; `gradle-wrapper-validation.yml` validates the wrapper.
- Releases are published to Maven Central via Sonatype OSSRH (`gradle/maven.gradle`), signed with GPG only for real remote publishes. The version is defined by the `Version` class in `build.gradle` (currently `4.0.0-SNAPSHOT`).
- Keep the library dependency-free: adding a runtime dependency breaks the `Import-Package: '!*'` OSGi contract and the project's zero-dependency promise — treat that as a deliberate, reviewed decision.
- Never commit credentials; Sonatype credentials and GPG keys are supplied via Gradle properties/environment at release time.
- Update the AsciiDoc docs and `src/docs/asciidoc/en/changelog.adoc` when public behavior or APIs change.

## Commit & Pull Request Guidelines

Git history is present; recent commits use short, imperative, intent-first subjects (e.g. "Upgrade build to Gradle 9.7.1"). Pull requests should explain the problem and approach, link relevant issues, list the verification commands run, and include benchmark comparisons for performance-sensitive changes.
