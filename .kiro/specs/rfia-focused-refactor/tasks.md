# Implementation Plan

- [x] 1. Create backup of AI code and prepare for removal
  - Create a backup branch or archive of the AI agent code before removal
  - Document the backup location for future reference
  - _Requirements: 2.1, 2.7_

- [x] 1.1 Create backup branch for AI code preservation
  - Create a new git branch named `ai-agent-backup` from current main branch
  - Push the backup branch to preserve all AI agent development work
  - _Requirements: 2.1, 2.7_

- [x] 1.2 Remove AI module directory from main codebase
  - Delete the `src/pyfia/ai/` directory and all its contents
  - Remove AI-related imports from other modules that reference the ai package
  - _Requirements: 2.2_

- [x] 1.3 Remove AI CLI interface module
  - Delete the `src/pyfia/cli/ai_interface.py` file
  - Remove any imports or references to ai_interface in other CLI modules
  - _Requirements: 2.3_

- [x] 2. Clean up dependencies and package configuration
  - Remove AI-related dependencies from pyproject.toml
  - Update package scripts and metadata
  - _Requirements: 3.1, 3.2, 3.3_

- [x] 2.1 Remove AI dependencies from pyproject.toml
  - Remove the `langchain` optional dependency group from pyproject.toml
  - Remove `pyfia-ai` script entry point from project.scripts section
  - Update the `all` optional dependency to exclude langchain
  - _Requirements: 3.1, 3.2_

- [x] 2.2 Update package metadata and description
  - Update the project description to focus on rFIA compatibility
  - Remove AI-related classifiers and keywords if present
  - Update package URLs to reflect the focused scope
  - _Requirements: 3.3_

- [x] 2.3 Clean up package data configuration
  - Remove AI-related package data entries from setuptools.package-data
  - Remove references to ai.prompts or similar AI-specific data
  - _Requirements: 3.3_

- [x] 3. Refactor CLI interface to remove AI functionality
  - Update base CLI class to remove AI references
  - Ensure direct CLI remains fully functional
  - _Requirements: 2.5, 6.1, 6.2, 6.3_

- [x] 3.1 Clean up base CLI class
  - Remove any AI-related imports or references from `src/pyfia/cli/base.py`
  - Update help text and documentation to remove AI mentions
  - Ensure all shared CLI functionality remains intact
  - _Requirements: 6.1, 6.2_

- [x] 3.2 Update direct CLI interface
  - Review `src/pyfia/cli/direct.py` for any AI-related code or references
  - Ensure all direct database operations and estimation commands work properly
  - Update help text to focus on direct FIA functionality
  - _Requirements: 6.1, 6.3_

- [x] 3.3 Update CLI configuration and utilities
  - Review `src/pyfia/cli/config.py` and `src/pyfia/cli/utils.py` for AI references
  - Remove any AI-related configuration options or utilities
  - Ensure all remaining CLI utilities function correctly
  - _Requirements: 6.1, 6.2_

- [x] 4. Update documentation to focus on rFIA functionality
  - Rewrite README.md to emphasize rFIA compatibility
  - Remove AI agent documentation sections
  - Update examples and usage patterns
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [x] 4.1 Rewrite README.md for rFIA focus
  - Remove all AI agent sections from README.md
  - Update the overview and features sections to focus on rFIA compatibility
  - Update installation instructions to remove AI-related dependencies
  - _Requirements: 5.1, 5.2_

- [x] 4.2 Update examples and quick start guide
  - Replace AI agent examples with direct rFIA-equivalent usage patterns
  - Update the quick start section to show statistical estimation workflows
  - Ensure all code examples work with the refactored codebase
  - _Requirements: 5.3, 5.4_

- [x] 4.3 Remove AI agent documentation directory
  - Delete the `docs/ai_agent/` directory and all its contents
  - Update any documentation links that reference AI agent docs
  - _Requirements: 5.5_

- [x] 5. Validate core FIA functionality remains intact
  - Run existing tests to ensure no regressions
  - Verify all estimation functions work correctly
  - Test CLI functionality end-to-end
  - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [x] 5.1 Run existing test suite
  - Execute the full test suite to identify any broken imports or functionality
  - Fix any test failures caused by the AI code removal
  - Ensure all core estimation tests pass without modification
  - _Requirements: 7.1, 7.2_

- [x] 5.2 Test core FIA class functionality
  - Test database connection with both DuckDB and SQLite engines
  - Verify EVALID management functions work correctly
  - Test all estimation method calls from the FIA class
  - _Requirements: 7.4_

- [x] 5.3 Validate estimation function accuracy
  - Run validation tests against existing rFIA benchmarks
  - Ensure biomass, volume, area, and TPA functions maintain exact accuracy
  - Verify mortality and growth functions work correctly
  - _Requirements: 7.3_

- [x] 5.4 Test CLI interface functionality
  - Test all direct CLI commands (connect, evalid, area, biomass, volume, tpa, mortality)
  - Verify command parsing and parameter handling works correctly
  - Test export functionality and result display
  - _Requirements: 7.4_

- [x] 6. Final cleanup and optimization
  - Remove any remaining AI-related code or comments
  - Update import statements throughout the codebase
  - Optimize package structure for focused scope
  - _Requirements: 2.4, 3.4_

- [x] 6.1 Clean up remaining AI references
  - Search codebase for any remaining AI-related imports, comments, or code
  - Remove or update any lingering references to AI functionality
  - Ensure all import statements are clean and functional
  - _Requirements: 2.4_

- [x] 6.2 Update module imports and dependencies
  - Review all Python files for import statements that reference removed AI modules
  - Update any conditional imports that check for AI dependencies
  - Ensure all remaining imports resolve correctly
  - _Requirements: 2.4, 3.4_

- [x] 6.3 Optimize package structure and build
  - Test package building with updated configuration
  - Verify package installation works correctly without AI dependencies
  - Ensure package size is reduced after AI code removal
  - _Requirements: 3.4_

- [ ] 7. Final validation and testing
  - Comprehensive end-to-end testing of refactored package
  - Performance testing to ensure no regressions
  - Documentation review and final updates
  - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [ ] 7.1 Comprehensive integration testing
  - Test complete workflows from database connection to result export
  - Verify all estimation methods work with various parameter combinations
  - Test error handling and edge cases
  - _Requirements: 7.1, 7.2_

- [ ] 7.2 Performance benchmarking
  - Run performance tests to ensure refactoring doesn't impact speed
  - Compare memory usage before and after refactoring
  - Verify DuckDB and SQLite performance remains optimal
  - _Requirements: 7.3_

- [ ] 7.3 Final documentation review
  - Review all remaining documentation for accuracy and completeness
  - Ensure installation instructions work correctly
  - Verify all examples and code snippets function properly
  - _Requirements: 5.1, 5.2, 5.3, 5.4_