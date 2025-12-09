# CLAUDE.md

> This file provides guidance to Claude Code when working with this repository.

## Mission

**Democratize access to forest inventory data.** The USDA Forest Inventory and Analysis (FIA) program collects invaluable data about America's forests, but this data is locked behind technical barriers that exclude most potential users.

## Vision

Make FIA data accessible to everyone who needs it—from family forest landowners to policy makers—regardless of their technical expertise.

## The Problem We Solve

### The Expertise Gap

| Who Needs FIA Data | Current Reality |
|-------------------|-----------------|
| 11M+ forest landowners | Cannot access data at all |
| 15,000 consulting foresters | Hours spent wrestling with SQL |
| State forestry agencies | Long report cycles, limited staff |
| Policy makers | Wait 2-3 years for published reports |
| Carbon developers | Building custom pipelines from scratch |

**Current requirements to access FIA data:**
1. Understand 200+ columns across 50+ tables
2. Know EVALIDs, expansion factors, stratification
3. SQL, Python, or R programming skills
4. Statistical expertise for variance estimation

**Our solution:** Remove these barriers entirely.

## Product Strategy

### Phase 1: Technical Foundation (Current)
Python library with statistically valid FIA estimation—the engine.

### Phase 2: Natural Language Interface (Next)
"Ask the Forest" - Plain English queries powered by LLMs.

```
User: "What's the pine volume in Georgia?"
System: "Georgia has 12.3 billion cubic feet of pine (± 2.1% SE)"
```

### Phase 3: Accessible Data Platform (Future)
Web interface, API, integrations—forest data for everyone.

## Target Users (Prioritized)

1. **Consulting foresters** - Reduce hours of data work to seconds
2. **State forestry analysts** - Faster reporting, fewer backlogs
3. **Carbon project developers** - Rapid baseline inventory
4. **Extension services** - Answer landowner questions instantly
5. **Researchers** - More time for science, less for data wrangling

## Design Philosophy

### Simplicity First
- **No over-engineering**: Avoid unnecessary patterns (Strategy, Factory, Builder)
- **Direct functions**: `volume(db)` not `VolumeEstimatorFactory.create().estimate()`
- **YAGNI**: Don't build for hypothetical future needs
- **Flat structure**: Maximum 3 levels of directory nesting

### Statistical Rigor
- Design-based estimation following Bechtold & Patterson (2005)
- Results must match EVALIDator (official USFS tool)
- Always include uncertainty estimates (SE, confidence intervals)
- Never compromise accuracy for convenience

### User Trust
- Show your work: Transparent methodology
- Validate against official sources
- Clear error messages when queries can't be answered
- Honest about limitations

## Business Objectives

### Year 1 Milestones
| Milestone | Target | Metric |
|-----------|--------|--------|
| NL Proof of Concept | Month 3 | 80% query success rate |
| Design Partners | Month 3 | 5 organizations |
| Beta Launch | Month 6 | 100 MAU |
| First Revenue | Month 9 | $99+ MRR |
| Product-Market Fit | Month 12 | 50 paying customers |

### Revenue Model
- **Free**: Open source library, 100 NL queries/month
- **Pro** ($99/mo): 1,000 queries, API access, exports
- **Team** ($299/mo): 5,000 queries, collaboration
- **Enterprise**: Custom pricing, on-prem, SLA

### Key Metrics to Track
- Query success rate (target: 95%+)
- Time to answer vs. traditional methods (target: 100x faster)
- User retention (target: <5% monthly churn)
- EVALIDator validation accuracy (target: 99%+)

## Competitive Position

**pyFIA is the only Python-native FIA estimation library.**

| Competitor | Language | NL Interface | Our Advantage |
|------------|----------|--------------|---------------|
| rFIA | R | No | Python ecosystem, NL roadmap |
| FIESTA | R | No | Simpler API, faster |
| EVALIDator | Web | No | Programmatic, customizable |
| Consultants | Human | Yes | 100x faster, 10x cheaper |

## Documentation Map

| Document | Purpose |
|----------|---------|
| [README.md](./README.md) | Quick start for users |
| [docs/BUSINESS_EVALUATION.md](./docs/BUSINESS_EVALUATION.md) | Market analysis, strategy |
| [docs/DEVELOPMENT.md](./docs/DEVELOPMENT.md) | Technical setup, architecture |
| [docs/fia_technical_context.md](./docs/fia_technical_context.md) | FIA methodology reference |

## Development Quick Reference

```bash
# Setup
uv venv && source .venv/bin/activate && uv pip install -e .[dev]

# Test
uv run pytest

# Quality
uv run ruff format && uv run ruff check --fix && uv run mypy src/pyfia/
```

## Core Principles for Contributors

1. **User value first**: Every feature should reduce friction for end users
2. **Statistical validity**: Never ship estimates that could mislead
3. **Simplicity**: When in doubt, choose the simpler approach
4. **Real data testing**: Always test with actual FIA databases
5. **Documentation**: If it's not documented, it doesn't exist

## Important Notes

- **No backward compatibility debt**: Refactor freely, don't maintain old APIs
- **Performance matters**: Choose fast implementations over elegant abstractions
- **YAML schemas are source of truth**: FIA table definitions live in YAML
- **`mortality()` is the documentation gold standard**: Match its docstring quality
