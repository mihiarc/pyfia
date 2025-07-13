# Development Guidelines

## Git Commit Strategy

### Commit Pattern for Progress Stages

For each major development task, create focused commits that represent logical progress stages:

1. **Feature Implementation**: Add core functionality
2. **Validation**: Compare with rFIA ground truth
3. **Documentation**: Update CLAUDE.md with results
4. **Cleanup**: Remove temporary files and artifacts

### Commit Message Format

```
<type>: <short description>

<detailed description>
- Key changes
- Validation results
- Impact on codebase
```

### Commit Types

- **feat**: New estimator or major feature
- **fix**: Bug fixes
- **docs**: Documentation updates
- **test**: Add or update tests
- **refactor**: Code restructuring
- **clean**: Remove artifacts/cleanup
- **validate**: Validation against rFIA

### Example Workflow

```bash
# 1. Implement feature
git add pyfia/new_estimator.py
git commit -m "feat: add mortality estimation module

- Implement mortality estimator following rFIA methodology
- Support for live/dead tree filtering
- EVALID-based filtering integrated"

# 2. Validate against rFIA
git add validation_results.md
git commit -m "validate: mortality estimator against rFIA ground truth

✅ Mortality rate: 1.23% - EXACT MATCH with rFIA (1.23%)
✅ Dead tree volume: 45.6 cu ft/acre - EXACT MATCH
Production-ready implementation validated"

# 3. Update documentation
git add CLAUDE.md
git commit -m "docs: add mortality validation results to CLAUDE.md

- Update ground truth section with mortality results
- Mark mortality estimator as validated
- Add implementation notes"

# 4. Push progress
git push origin master
```

### Validation Standards

Each estimator must achieve:
- **<1% difference** from rFIA ground truth (preferably <0.1%)
- **Same plot counts** or documented reason for difference
- **All major parameters** tested (different domains, groupings)
- **Production readiness** confirmed

### Current Validated Estimators

✅ **Area**: EXACT MATCH (0.0% difference)
✅ **Biomass**: EXACT MATCH (0.0% difference)
✅ **Volume**: EXACT MATCH (<0.1% difference)
⚠️ **TPA**: 3.8% difference (acceptable range)

### Next Priorities

1. **Test suite**: Add comprehensive Python tests
2. **CI/CD**: Automated validation pipeline
3. **Mortality**: Complete mortality estimator
4. **Growth**: Implement growth/removal/mortality
5. **Performance**: Benchmark against rFIA speed