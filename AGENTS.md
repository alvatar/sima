# PROJECT_RULES

GIVE SHORT REPLIES

## Communication (read first)

- Use only dry and professional tone.
- No sycophancy. Do not praise, flatter, or validate. State findings flat. No "You are right" or variations.
- No fake candor. Just communicate directly. You have to be honest without being explicit about this fact.
- No meta-commentary about how you are speaking.
- Never talk in the negative "what something is not". I don't care about that. I care only about what things are.
- Synthetize. Use bullet points.
- Use direct and simple language. No made up hyphenated terms. Use standard words.
- Answer only to what is asked, go to the point.
- Avoid hedging in your responses.

## General engineering rules

- No hacks, no speculative architecture, no demo-shaped dead ends
- Before introducing backwards compatibility, always ask.
- work/ is never committed
- Always commit and push every meaningful block of work. Err on the side of small rather than big commits.
- Docs and comments are timeless: they state the current state and its rationale. Never record decision dates, timestamps, milestone numbers, or narrations of how something came to be. History lives in git. The roadmap file (`TODO.md`) is the one exemption, since tracking progression is its function.
- No AI attribution anywhere. AI is mentioned in the README.

## Testing

- Rust tests live in the same file as the code they cover, in a `#[cfg(test)] mod tests` at the end of the file. A separate test file is justified only for integration tests spanning multiple files/modules.
- Cross-crate and end-to-end behavior is tested in integration tests under the consuming crate's `tests/` directory (workspace: each crate owns its integration tests).
- End-to-end tests of real domains through the full spine live in `crates/sima-integration`.
- Do not ignore tests. `#[ignore]` is permitted only for tests that rent machines, call a paid API, or are manual benchmarks — always with a reason string naming the requirement. Needing a device is never grounds for it; that is what the `on_device` marker is for.
- A test that needs a real device carries `on_device` in its path — a containing `mod on_device`, or an `_on_device` suffix where the file holds a single such test. CI is hosted and runs the unmarked subset, `cargo test --workspace --no-fail-fast -- --skip on_device`, so that substring is what keeps the test on the device machine. The marked tests run there, in the pre-merge device gate: `env -u VK_DRIVER_FILES cargo test --workspace --no-fail-fast`.

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

The settled invariants and principles are the RULES section of
@docs/architecture.md.

## Documentation

Applies to PR text, `docs/` and long-form comments:
- Language is professional and dry, headings included.
- Be concrete: every heading and sentence names its subject.
- Word substitutions in prose: seam → boundary, fold → merge.
- Enumerations are written as bullet lists or subsections, never buried inside prose paragraphs.
- Long paragraphs are broken at idea boundaries.
- Bold is used for genuinely key short phrases that aid scanning.
- Equations use proper math notation (GitHub-flavored LaTeX in markdown: `$...$` inline, `$$...$$` display), not ASCII character equations.
- GitHub bodies: one line per paragraph and list item, since a newline renders as a break.

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
