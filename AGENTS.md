# PROJECT_RULES

## Communication (read first)

- Never open a reply with "You are right", "You're absolutely right", or any affirmation of the user. Just answer.
- No sycophancy. Do not praise, flatter, or validate. State findings flat.
- Never use the words "honest", "honestly", "honesty", "brutal", or "to be frank".  If something is a caveat or a risk, name the caveat or risk directly — do not frame it as an act of candor. NEVER USE THOSE WORDS.
- Dry, direct, professional. Lead with the substance. The most important point goes first and in full, never buried as a "side note".
- Never narrate your own tone or stance. No meta-commentary about how you are speaking — no "I'll say this flatly", "not defending it", "to be clear", "naming this directly", "stating plainly", or any phrase that describes the manner of your reply instead of just delivering it. Say the thing; do not announce how you are saying it.
- Define things by what they are, not by what they are not. A negative ("this is not X", "rather than Y") bloats without adding clarity; state the positive fact. Applies to prose, code comments, and docs.

## General engineering rules

- No hacks, no speculative architecture, no demo-shaped dead ends
- work/ is never committed
- Do a commit for every meaningful block of work, and push it. Small, meaningful commits that are easy to bisect later.
- No Co-Authored-By trailers or any other AI-authorship markers in commits.

## Testing layer

- Rust logic is tested in Rust tests, next to the type or function it covers.
- Cross-module and end-to-end behavior (pipeline runs, store round-trips, sandbox enforcement) is tested in integration tests under `tests/`.
- Determinism is a tested property, never an assumption: anything claimed reproducible gets a test that executes it twice (or on two configurations) and asserts identical content hashes.

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

Project & stack:
- SIMA is distributed infrastructure for program search: generate candidate programs, execute them sandboxed, evaluate them through a staged cost-aware funnel, record everything with content-addressed provenance. `README.md` is the design document; the near-term work list is `TODO.md`.
- Rust, local-first. A run is fully functional on one machine and scales out to pluggable remote execution backends.

Ownership invariants (frozen):
- A task's result is a pure function of its content-addressed inputs: `(program hash, seed, environment hash) → result`. Any backend that returns a result for a key is interchangeable with any other; results are cacheable, retryable, and independently verifiable.
- Stages communicate through the store, never directly. A batch can be re-run, resumed, or re-evaluated without regeneration.
- Generated programs are untrusted and execute only inside the sandbox, under enforced resource limits. No generated code ever runs on the host directly.
- All randomness is seeded and captured; a recorded specification reproduces its output exactly. Workloads that cannot be bit-deterministic relax to statistical reproducibility on a pinned backend class, and the relaxation is recorded in provenance.
- Trust is a scheduling dimension: untrusted backends are confined to stages whose results are cheap to verify; survivors are re-verified on a trusted tier.
- Generators and evaluators are pluggable and decoupled from the execution and provenance layers.

Principles:
- Clean, pristine architecture: clear spine, truthful boundaries, no split brain.
- No unjustified repeated code; justify file count / splits; deliberate naming.
- No bootstrapping garbage in the active path; isolate platform-specific code cleanly.
- Maintain a clear, data-driven flow of information.
- Every milestone serves the real search substrate, optimizing for correctness and architecture, not demos.

## Code quality

- Prefer minimal clean module boundaries over giant files
- Module layout rule: use a directory module only when it represents a real semantic namespace; if a module has no submodules, prefer a single `name.rs` file. `mod.rs` may contain module docs, submodule declarations, and small curated re-exports that define the namespace surface. Do not put substantive implementation logic in `mod.rs`; that code should go into its own file/module.
- Naming rule: if a module primarily exists to hold one major type, the file name should match the type name clearly (e.g. `RenderState` -> `render_state.rs`)
- Add inline comments for key operations / tricky logic
- Comments should not have historical references to the previous versions of the code. They should be explain what the code does now, exclusively.
- Add doc comments for functions / important APIs, in all languages and parts of the project
- Use the project `Error` / `Result<T>` types for fallible project code; do not introduce ad-hoc string error returns
