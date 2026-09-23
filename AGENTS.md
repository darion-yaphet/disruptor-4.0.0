# Repository Guidelines

## Project Structure & Module Organization

This is a single-module Java library built with Gradle. Production code lives in `src/main/java/com/lmax/disruptor`; the `dsl/` package contains the fluent consumer API and `util/` contains low-level helpers. Unit tests mirror those packages under `src/test/java`. Runnable examples are in `src/examples/java`. Performance work is separated into `src/perftest/java` and JMH benchmarks in `src/jmh/java`; concurrency correctness probes live in `src/jcstress/java`. AsciiDoc sources and images are under `src/docs/`. Checkstyle rules are in `config/checkstyle/`, while reusable build logic is in `gradle/`. Treat `build/` and `.gradle/` as generated output.

## Build, Test, and Development Commands

- `./gradlew clean build` — compile Java 11-compatible sources, run checks and tests, and create JARs.
- `./gradlew test` — run the parallel JUnit 5 unit suite.
- `./gradlew check` — run verification tasks, including Checkstyle.
- `./gradlew jcstress -Pmode=quick` — exercise concurrency guarantees; use stronger modes for release-level validation.
- `./gradlew jmh` — run JMH benchmarks and write JSON results under `build/reports/jmh/`.
- `./gradlew perfJar` — package custom performance tests for individual execution.
- `./gradlew asciidoctor` — render developer and user documentation.

Use the checked-in wrapper rather than a system Gradle installation. `./gradlew setUpGitHooks` installs the repository pre-commit hook.

## Coding Style & Naming Conventions

Follow `.editorconfig`: UTF-8, LF endings, four-space indentation, trimmed trailing whitespace, and Allman-style braces. Keep lines near 120 characters when practical; Checkstyle permits up to 200. Use standard Java naming (`UpperCamelCase` types, `lowerCamelCase` methods/fields, `UPPER_SNAKE_CASE` constants), avoid wildcard imports, and add Javadoc to public APIs. Preserve allocation-sensitive and cache-line-aware patterns unless benchmarks justify a change.

## Testing Guidelines

Tests use JUnit Jupiter 5.9 and Hamcrest. Name test classes `*Test` and place them beside the corresponding package path. Add focused regression tests for behavior changes. There is no configured coverage threshold; correctness, race safety, and performance evidence matter more than a raw percentage. Run `./gradlew build` before submitting, plus `jcstress` or JMH when touching sequencing, memory visibility, wait strategies, or hot paths.

## Commit & Pull Request Guidelines

This snapshot has no `.git` history, so project-specific commit patterns cannot be verified. Use a short, imperative, intent-first subject; for substantial decisions add trailers such as `Tested:`, `Confidence:`, and `Scope-risk:`. Pull requests should explain the problem and approach, link relevant issues, list verification commands, and include benchmark comparisons for performance-sensitive changes. Update AsciiDoc and changelog material when public behavior or APIs change.
