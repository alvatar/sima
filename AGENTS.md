# PROJECT_RULES

## Communication (read first)

- Never open a reply with "You are right", "You're absolutely right", or any affirmation of the user. Just answer. Do not use the expression "load-bearing" either.
- No sycophancy. Do not praise, flatter, or validate. State findings flat.
- Never use the words "honest", "honestly", "honesty", "brutal", or "to be frank".  If something is a caveat or a risk, name the caveat or risk directly — do not frame it as an act of candor. NEVER USE THOSE WORDS.
- Dry, direct, professional. Lead with the substance. The most important point goes first and in full, never buried as a "side note".
- Never narrate your own tone or stance. No meta-commentary about how you are speaking — no "I'll say this flatly", "not defending it", "to be clear", "naming this directly", "stating plainly", or any phrase that describes the manner of your reply instead of just delivering it. Say the thing; do not announce how you are saying it.
- Define things by what they are, not by what they are not. A negative ("this is not X", "rather than Y") bloats without adding clarity; state the positive fact. Applies to prose, code comments, and docs.
- Do not invent ad-hoc hyphenated terms as shorthand ("line-framed", "no-replace"); use plain, established wording and spell the mechanism out. Applies to prose, code comments, and docs.
- Professional register in everything written — PRs, docs, comments, reports. No jokes, metaphors, or playful phrasing ("the exit exam", "sails past").
- Write for a senior engineer, not a domain expert: explain domain-specific concepts (cryptography, scheduling theory) at that level, and skip explanations of general engineering.
- Reading takes time. Include only what changes the reader's understanding or decision; cut everything else. PR descriptions cover what changed and why, in the conventional shape of a PR; they leave out ancillary material such as the tests performed or the process followed.

## General engineering rules

- No hacks, no speculative architecture, no demo-shaped dead ends
- Before introducing backwards compatibility, always ask. Err on the side of no backwards compatibility: it adds complexity and we do not need it.
- work/ is never committed
- Do a commit for every meaningful block of work, and push it. Small, meaningful commits that are easy to bisect later.
- Docs and comments are timeless: they state the current state and its rationale. Never record decision dates, timestamps, milestone numbers, or narrations of how something came to be — anywhere in the repo (docs, code comments, test comments, artifact pages). History lives in git. The roadmap file (`TODO.md`) is the one exemption, since tracking progression is its function.
- No AI attribution anywhere: no "Generated with Claude Code" footers, no `Co-Authored-By: Claude` lines, no session links — not in commits, PR bodies, PR comments, issues, or docs.

## Testing layer

- Rust logic is tested in Rust tests, next to the type or function it covers.
- Cross-crate and end-to-end behavior is tested in integration tests under the consuming crate's `tests/` directory (workspace: each crate owns its integration tests).
- End-to-end tests of real domains through the full spine live in `crates/sima-integration`.

## Test-code structure

- Rust tests live in the same file as the code they cover, in a `#[cfg(test)] mod tests` at the end of the file.
- A separate test file is justified only for integration tests spanning multiple files/modules.
- Test-only failure-injection state is consolidated into a single `#[cfg(test)]` seams struct per type, never scattered across the type's fields.
- Test-only accessors are `#[cfg(test)]`-gated and may sit beside the state they expose.

## TODOs

- File should go to <root>/work/TODO-<topic>.md
- Must elaborate every single task required to execute a feature end-to-end. Make sure decisions are made before starting it. A high-level document is otherwise a PLAN
- Must be fully self-contained: they must have all the information to execute everything by just reading it
- Must use markdown checkboxes so they progress can be tracked
- For every task in a TODO, each of these subtasks must always be added:
  - Identify the best way to implement it preserving or improving the architecture. Prepare the approach by respecting the rules of the project and making sure the architecture is well understood
  - Add regressions tests for the features that will be touched, specifically for the aspects of it that we don't want that change
  - Red/green TDD: For the new features: add red tests. We must ensure those tests fail. Do not move to next step until this is done exhaustively
  - Implement the feature. This is the core step in the TODO
  - Red/green TDD: Ensure the red tests are now green. Testing must be thorough, deep and rigorous. Do not skip this.
  - Review the test and the code to test code together
  - Do a supervision verification, with tough and high standards of quality:
    - All the project rules are respected
    - No code smells or hacks introduced
    - DRY
    - Architecture is respected
    - Is it deeply and thoroughly tested?
    - We are not introducing drifting in any previous aspect of the code
    - Every piece of code is placed in the most sensible location. Not just where is immediately convenient, but where it is most clean and reasonable.
    - Performance
  - Review must be thorough. Ask the question yourself: did I do my best?
  - Iterate implementation until the supervision step is satisfactory

## Architecture

- `README.md` is the design document; the near-term work list is `TODO.md`. Rust; local execution first, distributed by design.
- Invariants below are settled in discussion before being recorded here; new ones are added the same way.

Settled invariants:
- Execution backends are implementation crates under `crates/toolkits/` (`sima-toolkit-*`), depending on `sima-core` (and `sima-contracts` when needed); `sima-domains` depends on the toolkits its domains use, and each toolkit isolates its own dependency set.
- The store is the only durable state. Queues, schedulers, and orchestrators are ephemeral; a task source derives the currently-runnable frontier from (config, store state) — static batches and segment chains are two implementations of that one interface. Resume, crash-recovery, and re-run are one code path: re-derive the frontier, continue.
- One orchestrator per run — the `sima run` process itself, no daemon; single-writer enforced by an OS file lock the kernel releases when the holder exits, so no staleness protocol exists; the lock file's content (pid, hostname) is diagnostic only. Workers are stateless leaseholders.
- Executors are pure compute: they receive (spec, params, seed, env) and return artifacts + stats, never touching the store. Workers commit results through the catalog. The trust boundary lives on this seam.
- Candidates are opaque at the infrastructure layer: a spec is (format id, opaque bytes), content-addressed. Domains interpret specs; "genome" is domain vocabulary. Run parameters are a second opaque content-addressed blob (params): generators produce specs, config produces params, and the spec's format id governs the interpretation of both — so one candidate stays addressable across evaluation stages and the generator contract never carries evaluation policy.
- Two serialization worlds: identity-bearing bytes (anything hashed) go through the canonical `Enc`/`Dec` encoding exclusively; human-readable artifacts are serde and never identity-bearing.
- Reproducibility is declared per domain across two tiers (README, Determinism), not assumed uniform. The infrastructure guarantees run identity regardless: manifests are canonicalized so run hashes are independent of worker completion order, and journals are observational, excluded from equality criteria.

Principles:
- Clean, pristine architecture: clear spine, truthful boundaries, no split brain.
- No unjustified repeated code; justify file count / splits; deliberate naming.
- No bootstrapping garbage in the active path; isolate platform-specific code cleanly.
- Maintain a clear, data-driven flow of information.
- Every milestone serves the real search substrate, optimizing for correctness and architecture, not demos.

## Documentation structure

Applies to `docs/` and long-form comments:
- Enumerations are written as bullet lists or subsections, never buried inside prose paragraphs.
- Long paragraphs are broken at idea boundaries.
- Bold is used for genuinely key short phrases that aid scanning.
- Equations use proper math notation (GitHub-flavored LaTeX in markdown: `$...$` inline, `$$...$$` display), not ASCII character equations.

## Code quality

- Prefer minimal clean module boundaries over giant files
- Module layout rule: use a directory module only when it represents a real semantic namespace; if a module has no submodules, prefer a single `name.rs` file. `mod.rs` may contain module docs, submodule declarations, and small curated re-exports that define the namespace surface. Do not put substantive implementation logic in `mod.rs`; that code should go into its own file/module. Exception: integration-test helpers live in `tests/common/mod.rs` and may hold the helper logic directly, because Cargo compiles every top-level file under a crate's `tests/` directory as its own test crate, so shared code must sit in a subdirectory.
- Naming rule: if a module primarily exists to hold one major type, the file name should match the type name clearly (e.g. `RenderState` -> `render_state.rs`)
- Execution-toolkit naming: crates under `crates/toolkits/` are named by the developer-facing contract, not the runtime that powers them (`sima-toolkit-wgsl`, not its ash/wgpu backend); the runtime enters the name only if it leaks into the domain-facing API (`sima-toolkit-ash-wgsl`)
- Add inline comments for key operations / tricky logic
- Comment placement follows the level of the idea, to avoid drift: type-level and high-level doc comments carry the big and key ideas only; algorithmic details are commented inline, directly where they happen. When a detail changes, the adjacent inline comment is updated in the same edit — a detail described far from its code (in a type doc, a module doc, another function) goes stale silently.
- Comments should not have historical references to the previous versions of the code. They should be explain what the code does now, exclusively.
- Add doc comments for functions / important APIs, in all languages and parts of the project
- Use the project `Error` / `Result<T>` types for fallible project code; do not introduce ad-hoc string error returns
- Accessors returning a value by identity drop the `get_` prefix and read as nouns (`record(&key)`, `manifest(&run)`), per the Rust API guideline C-GETTER. `get_` is reserved for nothing here.
