# drive9.ai Homepage Redesign

## Status: DRAFT - pending team review

## Background

Meeting feedback from 唐刘 identified critical issues with the current drive9.ai homepage:
information architecture mixes audiences, hero lacks pain point, features are listed
without scenarios, TiDB Cloud / localhost / mem9 appear too early, and there is no FAQ
or trust section.

This document defines the new page structure, copy direction, and constraints before
any code changes to `site/index.html`.

---

## Positioning

**Category label:** Shared filesystem for AI agents

**One-line promise:** Give every agent the same workspace.

**Expanded:** Mount a shared cloud workspace so Claude Code, Codex, and your own
agents can read, write, share, and search the same files.

**Homepage reader:** Human developers and technical decision-makers building agent
workflows. The homepage is read by humans, but it must clearly present drive9's
two-layer product model (see below).

---

## Product Model: Layer 1 vs Layer 2

drive9 serves two distinct user types through two layers. The homepage must present
both layers explicitly, with clear separation — not mixed together.

### Layer 1: Filesystem layer — used by agents

The core substrate. Agents interact with drive9 as a filesystem:
- Mount a shared remote namespace via FUSE/WebDAV
- Read, write, list, copy, rename files
- Share skills, artifacts, and working state across agents
- Access via mount, CLI, OpenAPI, or skill definitions

**Who uses it:** AI agents (Claude Code, Codex, custom agents, agent runtimes)
**Value proposition:** Agents get a persistent, shared workspace instead of
isolated local directories that disappear between sessions.

### Layer 2: Storage + semantic analysis layer — used by humans

Built on top of the FS layer. Humans and applications consume intelligent features:
- Semantic and full-text search across uploaded files
- Searchable text extraction from supported file types
- Knowledge base management, asset organization
- Team sharing (scoped access and permissions are on the roadmap)

**Who uses it:** Human developers, teams, and upper-layer applications
**Value proposition:** Turn the agent workspace into a searchable, manageable,
intelligent storage system.

### How the two layers relate

Layer 1 is the foundation — without it, there's nothing to search or manage.
Layer 2 adds human-facing intelligence on top. The homepage must present Layer 1
first (core substrate), then Layer 2 (human value-add). Layer 2 must not overshadow
Layer 1 or make drive9 look like "just another AI cloud storage."

---

## Page Structure (section by section)

### Section 1: Hero

**Goal:** In 5 seconds, answer: what is drive9, who is it for, why do I need it.

**Layout:**
- Eyebrow: `Shared filesystem for AI agents`
- H1: `Give every agent the same workspace.`
- Subcopy: `Agents mount and share one remote filesystem. Teams search, organize,
  and manage the files they produce.`
- Primary CTA: `Get Started` (links to #getting-started)
- Secondary CTA: `Read Docs` (links to GitHub README)
- Visual: diagram showing agents (Layer 1) writing to a shared namespace,
  humans (Layer 2) searching and managing it

**Must NOT appear in hero:**
- CLI install command
- `skill.md` / machine-readable agent instruction block
- "works with any agent" ecosystem grid
- TiDB Cloud / Powered by
- SDK tabs or code blocks
- mem9 cross-reference
- localhost:9009

---

### Section 2: Problem

**Goal:** Make the reader nod — "yes, I have this problem."

Three short pain bullets (no feature mentions):

1. **Files are scattered.**
   Every agent writes to its own local directory. Other agents can't see the output.

2. **Sharing is fragile.**
   Syncing files between Claude Code, Codex, and custom agents means custom glue,
   environment variables, and manual copy.

3. **Storage is split across too many systems.**
   S3 for blobs, a vector DB for search, local disk for working files, metadata in
   yet another store. Four systems for one job.

---

### Section 3: Two-Layer Model

**Goal:** After the reader recognizes the problem, show drive9's two-layer solution.

Two cards side by side:

**For your agents (Layer 1: Filesystem)**
- Mount a shared remote workspace
- Read, write, and share files across Claude Code, Codex, and custom agents
- Persist skills, artifacts, and working state between sessions

**For you and your team (Layer 2: Storage + Intelligence)**
- Search files by meaning, not just filename
- Extract searchable text from supported files
- Organize knowledge bases and asset libraries
- Team sharing (scoped access on the roadmap)

This section replaces the old "feature dump" approach by framing capabilities
through who uses them.

---

### Section 4: Scenarios (the main course)

**Goal:** Show concrete before/after stories, not feature lists. Each scenario
should show which layer is at work.

Three scenario cards with clear priority ordering:

#### Scenario A (Primary): Shared Agent Workspace
**Layers:** Layer 1 (core)

> **Before:** Claude Code writes skills to `~/.agents/skills/`. Codex can't see them.
> You copy files manually or build sync scripts.
>
> **After:** Both agents mount `drive9 mount :/skills ~/.agents/skills`. One agent
> writes a skill, the other reads it immediately. Same namespace, zero sync.

Tags: `mount` `multi-agent` `shared namespace`

#### Scenario B (Secondary): Team Knowledge Workspace
**Layers:** Layer 1 (agents read/search) + Layer 2 (humans organize and tag)

> **Before:** Company docs, prompts, and runbooks live in Notion, Google Drive, and
> local repos. Agents can't access them without per-tool integrations.
>
> **After:** Upload team knowledge to drive9. Agents mount and search it with
> `drive9 fs grep "deploy procedure"`. Humans organize and tag files.

Tags: `search` `team sharing` `tagging`

#### Scenario C (Tertiary): Searchable Asset Library
**Layers:** Layer 2 (humans search and manage) built on Layer 1 (storage substrate)

> **Before:** Images and documents are uploaded to S3. Finding "the architecture
> diagram from last quarter" means grepping filenames or maintaining a separate index.
>
> **After:** Upload to drive9. Files are automatically indexed. Search by meaning:
> `drive9 fs grep "architecture diagram Q1"` finds it by content, not filename.

Tags: `auto-indexing` `semantic search` `media`

---

### Section 5: Core Capabilities

**Goal:** Prove the scenarios work. Max 4 capabilities, each tagged by layer.

#### Mount as local filesystem (Layer 1)
Mount drive9 at any local path. Use `ls`, `cat`, `vim`, `cp` — your tools and agents
work without code changes.
**So what:** Agents don't need a new SDK or API; they just read and write files.

#### Semantic search, built in (Layer 2)
Searchable text is extracted from supported files. Combine semantic, full-text,
and keyword search to find files by meaning.
**So what:** Humans and agents find relevant files without knowing exact names or paths.

#### Large-file reliability (Layer 1)
Streaming upload/download with automatic resume. Works for configs and datasets alike.
**So what:** No partial uploads, no manual retry scripts.

#### Zero-copy operations (Layer 1)
Copy and rename without re-uploading data. Metadata-only, instant.
**So what:** Organize and deduplicate without storage cost or wait time.

---

### Section 6: Works With

**Goal:** Social proof, not a feature section.

One line of logos/names: Claude Code, Codex, Cursor, Cline, VS Code, custom agents.

Subcopy: `drive9 works with any tool that reads and writes files.`

Keep this minimal — a trust strip, not a grid section.

---

### Section 7: Getting Started

**Goal:** One clear path to first value.

```
1. Install CLI
   $ curl -fsSL https://drive9.ai/install.sh | sh

2. Create a workspace
   $ drive9 create
   $ drive9 fs mkdir :/skills

3. Mount and use
   $ mkdir -p ~/drive9-skills
   $ drive9 mount :/skills ~/drive9-skills
   $ ls ~/drive9-skills/
```

Secondary paths (smaller, below the main flow):
- `For agents:` link to skill.md
- `SDK & API:` link to docs
- `GitHub:` link to repo

---

### Section 8: FAQ

**Goal:** Answer trust questions that block adoption.

**Is my data secure?**
Data is encrypted in transit (TLS). Each workspace is isolated per tenant.
[Needs deployment confirmation: at-rest encryption details, key management.]

**Where is data stored?**
Metadata and small files are stored in the drive9 backend (db9). Larger files use
tiered object storage (S3) via presigned URLs. Search indexes are maintained
alongside metadata.
[Needs confirmation: exact size threshold, region/residency details.]

**Why does drive9 use FUSE / WebDAV locally?**
To present the remote workspace as a local directory. Your agents and tools read/write
files normally — drive9 handles sync in the background.

**How is drive9 different from S3?**
S3 is blob storage. drive9 is a filesystem with directories, metadata, search, and
mount. Agents use it like a local drive, not an object API.

**How is drive9 different from a vector database?**
A vector DB searches embeddings. drive9 is a full filesystem that includes automatic
embedding and search. You don't need a separate vector DB pipeline.

**Can multiple agents share one workspace?**
Yes. That's the primary use case. Multiple agents mount the same remote namespace
and see each other's changes.

**What does it cost?**
Free during beta. [Confirm pricing status with @qiffang.]

**What is mem9?**
mem9 is a separate product for agent memory (conversation history, learned facts).
drive9 is for files and workspace. They are complementary but independent.

---

### Section 9: Footer

- Logo + tagline: `Shared filesystem for AI agents.`
- Links: GitHub, Documentation, Skill File, Privacy, Contact
- `Powered by TiDB Cloud` (here, not in hero)
- License: Apache 2.0

---

## Must-Not-Appear-Above-Fold Checklist

- [ ] `localhost:9009`
- [ ] `Powered by TiDB Cloud`
- [ ] `Read https://drive9.ai/skill.md and follow instructions`
- [ ] Multi-language SDK code tabs
- [ ] mem9 cross-reference
- [ ] CLI install command as hero centerpiece
- [ ] Unshipped features presented as available
- [ ] Multiple equal-weight CTAs competing for attention

---

## Copy Safety List

Each claim in the homepage copy must have a verified shipped status.
Any claim not marked `shipped` MUST NOT appear in hero, scenario, or capability
main text — it can only appear in FAQ or with an explicit roadmap annotation.

| Claim | Status | Notes |
|-------|--------|-------|
| `drive9 create` | shipped | exists in CLI |
| `drive9 fs mkdir` | shipped | PR #369 / main |
| `drive9 mount :/path` | shipped | FUSE + WebDAV |
| `drive9 fs grep` | shipped | semantic + FTS + keyword |
| Semantic search | shipped (partial) | depends on content type and workspace config; not "every file automatically" |
| Smart summaries | **not shipped** | roadmap; do not use in main copy |
| Permission / access control | **not shipped** | mount --read-only exists for FUSE only; general RBAC is roadmap |
| Auto-extract metadata/tags | **partially shipped** | upload --tag exists; auto-extraction limited to supported types |
| Encryption at rest | **needs confirmation** | pkg/encrypt exists but deployment status unclear |
| Tenant isolation | shipped | per-tenant namespace |
| Storage architecture | shipped | db9 (small files) + S3 presigned (large files) |
| Data residency | **needs confirmation** | region details TBD |
| Pricing | **needs confirmation** | "free during beta" is placeholder |

## Fact-Check Before Implementation

Before changing `site/index.html`, verify all `needs confirmation` items above
with @qiffang and update the Copy Safety List accordingly.

---

## Execution Plan

**Phase 1: Content restructure** (this phase)
- Rewrite `site/index.html` with new section order and copy
- Remove all items from must-not-appear-above-fold list
- Add FAQ section

**Phase 2: Trust and proof**
- Add demo GIF or terminal recording
- Add usage metrics / GitHub stars if available
- Finalize security / privacy copy

**Phase 3: Visual polish**
- Animation, spacing, typography refinements
- Mobile responsiveness audit
- Dark/light mode consistency

---

## Review Gates

- **architect-1**: information architecture, section flow, two-layer model clarity
- **adversary-1**: hard gate on above-fold violations, unshipped claims, layer mixing
- **dat9-dev2**: implementation feasibility, fact-check on CLI commands
- **qiffang**: final copy direction, positioning, and two-layer balance approval

### Two-Layer Invariants (must hold across all sections)

1. Layer 1 (FS for agents) and Layer 2 (storage + semantics for humans) must be
   presented as separate, clearly labeled concepts — never mixed into one blurry pitch.
2. Layer 1 is the foundation; Layer 2 is built on top. Page order reflects this.
3. Hero can reference both layers but must not collapse them into one vague statement.
4. Each capability and scenario must be tagged with which layer it primarily serves.
5. Layer 2 must not overshadow Layer 1 — drive9 is a filesystem substrate first,
   not "another AI cloud storage with search."
6. Machine-readable agent instructions (skill.md, OpenAPI) serve Layer 1 but belong
   in docs/secondary paths, not in the hero or marketing narrative.
7. Any Layer 2 claim without shipped proof MUST NOT appear in hero, scenario, or
   capability main text — only in FAQ or with explicit roadmap annotation.
   See Copy Safety List for current status of each claim.
