"""
Pipeline Validation Engine for pyFIA Phase 4.

This module provides comprehensive validation capabilities for pipeline configuration,
data contracts, step compatibility, and pre-execution validation.
"""

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Type, Union, Callable

import polars as pl
from pydantic import BaseModel, Field, validator
from rich.console import Console
from rich.table import Table

from .core import (
    PipelineStep, ExecutionContext, DataContract,
    PipelineException, StepValidationError, DataContractViolation
)
from .contracts import (
    RawTablesContract, FilteredDataContract, JoinedDataContract,
    ValuedDataContract, PlotEstimatesContract, StratifiedEstimatesContract,
    PopulationEstimatesContract, FormattedOutputContract
)
from ..config import EstimatorConfig
from ..lazy_evaluation import LazyFrameWrapper


# === Enums ===

class ValidationLevel(str, Enum):
    """Validation strictness level."""
    NONE = "none"  # No validation
    BASIC = "basic"  # Basic type checking
    STANDARD = "standard"  # Standard validation (default)
    STRICT = "strict"  # Strict validation with all checks
    COMPREHENSIVE = "comprehensive"  # Full validation including data content


class ValidationResult(str, Enum):
    """Result of validation check."""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


# === Data Classes ===

@dataclass
class ValidationIssue:
    """Represents a validation issue."""
    
    level: ValidationResult
    category: str
    message: str
    step_id: Optional[str] = None
    field_name: Optional[str] = None
    expected: Optional[Any] = None
    actual: Optional[Any] = None
    suggestion: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "level": self.level.value,
            "category": self.category,
            "message": self.message,
            "step_id": self.step_id,
            "field_name": self.field_name,
            "expected": str(self.expected) if self.expected else None,
            "actual": str(self.actual) if self.actual else None,
            "suggestion": self.suggestion
        }
    
    def __str__(self) -> str:
        """String representation."""
        parts = [f"[{self.level.value.upper()}] {self.category}: {self.message}"]
        if self.step_id:
            parts.append(f"Step: {self.step_id}")
        if self.field_name:
            parts.append(f"Field: {self.field_name}")
        if self.suggestion:
            parts.append(f"Suggestion: {self.suggestion}")
        return " | ".join(parts)


@dataclass
class ValidationReport:
    """Complete validation report."""
    
    pipeline_id: str
    timestamp: float
    validation_level: ValidationLevel
    issues: List[ValidationIssue] = field(default_factory=list)
    checks_performed: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    checks_warned: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_valid(self) -> bool:
        """Check if validation passed."""
        return self.checks_failed == 0
    
    @property
    def has_warnings(self) -> bool:
        """Check if there are warnings."""
        return self.checks_warned > 0
    
    def add_issue(self, issue: ValidationIssue) -> None:
        """Add a validation issue."""
        self.issues.append(issue)
        
        if issue.level == ValidationResult.FAILED:
            self.checks_failed += 1
        elif issue.level == ValidationResult.WARNING:
            self.checks_warned += 1
        elif issue.level == ValidationResult.PASSED:
            self.checks_passed += 1
            
        self.checks_performed += 1
    
    def get_issues_by_level(self, level: ValidationResult) -> List[ValidationIssue]:
        """Get issues by level."""
        return [i for i in self.issues if i.level == level]
    
    def get_issues_by_step(self, step_id: str) -> List[ValidationIssue]:
        """Get issues for a specific step."""
        return [i for i in self.issues if i.step_id == step_id]
    
    def summary(self) -> Dict[str, Any]:
        """Get validation summary."""
        return {
            "pipeline_id": self.pipeline_id,
            "validation_level": self.validation_level.value,
            "is_valid": self.is_valid,
            "checks_performed": self.checks_performed,
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed,
            "checks_warned": self.checks_warned,
            "critical_issues": len(self.get_issues_by_level(ValidationResult.FAILED)),
            "warnings": len(self.get_issues_by_level(ValidationResult.WARNING))
        }


# === Base Validator ===

class Validator(ABC):
    """Abstract base class for validators."""
    
    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STANDARD):
        """
        Initialize validator.
        
        Parameters
        ----------
        validation_level : ValidationLevel
            Level of validation to perform
        """
        self.validation_level = validation_level
        self.console = Console()
    
    @abstractmethod
    def validate(self, target: Any, context: Optional[Dict[str, Any]] = None) -> List[ValidationIssue]:
        """
        Perform validation.
        
        Parameters
        ----------
        target : Any
            Target to validate
        context : Optional[Dict[str, Any]]
            Additional context for validation
            
        Returns
        -------
        List[ValidationIssue]
            List of validation issues
        """
        pass
    
    def should_validate(self, check_level: ValidationLevel) -> bool:
        """Check if validation should be performed at given level."""
        level_order = {
            ValidationLevel.NONE: 0,
            ValidationLevel.BASIC: 1,
            ValidationLevel.STANDARD: 2,
            ValidationLevel.STRICT: 3,
            ValidationLevel.COMPREHENSIVE: 4
        }
        return level_order[self.validation_level] >= level_order[check_level]


# === Schema Validator ===

class SchemaValidator(Validator):
    """Validates data schemas and contracts."""
    
    def validate(
        self,
        data: Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper],
        contract: Type[DataContract],
        context: Optional[Dict[str, Any]] = None
    ) -> List[ValidationIssue]:
        """
        Validate data against contract schema.
        
        Parameters
        ----------
        data : Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper]
            Data to validate
        contract : Type[DataContract]
            Contract to validate against
        context : Optional[Dict[str, Any]]
            Additional context
            
        Returns
        -------
        List[ValidationIssue]
            Validation issues
        """
        issues = []
        
        # Extract underlying frame
        if isinstance(data, LazyFrameWrapper):
            frame = data.frame
        else:
            frame = data
        
        # Get schema
        if isinstance(frame, pl.LazyFrame):
            schema = frame.collect_schema()
        else:
            schema = frame.schema
        
        # Check required columns
        if self.should_validate(ValidationLevel.BASIC):
            required_cols = self._get_required_columns(contract)
            missing_cols = required_cols - set(schema.names())
            
            if missing_cols:
                issues.append(ValidationIssue(
                    level=ValidationResult.FAILED,
                    category="schema",
                    message=f"Missing required columns: {missing_cols}",
                    field_name="columns",
                    expected=required_cols,
                    actual=set(schema.names()),
                    suggestion="Add missing columns or update contract"
                ))
        
        # Check column types
        if self.should_validate(ValidationLevel.STANDARD):
            type_mismatches = self._check_column_types(schema, contract)
            for col, expected, actual in type_mismatches:
                issues.append(ValidationIssue(
                    level=ValidationResult.WARNING,
                    category="schema",
                    message=f"Column type mismatch for '{col}'",
                    field_name=col,
                    expected=expected,
                    actual=actual,
                    suggestion=f"Cast column to {expected}"
                ))
        
        # Check data quality
        if self.should_validate(ValidationLevel.COMPREHENSIVE):
            quality_issues = self._check_data_quality(frame)
            issues.extend(quality_issues)
        
        return issues
    
    def _get_required_columns(self, contract: Type[DataContract]) -> Set[str]:
        """Get required columns from contract."""
        # This would be implemented based on contract definition
        if hasattr(contract, "get_required_columns"):
            return contract.get_required_columns()
        return set()
    
    def _check_column_types(
        self,
        schema: Dict[str, pl.DataType],
        contract: Type[DataContract]
    ) -> List[Tuple[str, str, str]]:
        """Check column types against contract."""
        mismatches = []
        
        # This would check expected types from contract
        expected_types = getattr(contract, "_column_types", {})
        
        for col, expected_type in expected_types.items():
            if col in schema:
                actual_type = schema[col]
                if not self._types_compatible(actual_type, expected_type):
                    mismatches.append((col, str(expected_type), str(actual_type)))
        
        return mismatches
    
    def _types_compatible(self, actual: pl.DataType, expected: pl.DataType) -> bool:
        """Check if types are compatible."""
        # Simple compatibility check - could be more sophisticated
        return actual == expected or (
            isinstance(actual, pl.Utf8) and isinstance(expected, pl.Categorical)
        )
    
    def _check_data_quality(
        self,
        frame: Union[pl.DataFrame, pl.LazyFrame]
    ) -> List[ValidationIssue]:
        """Check data quality issues."""
        issues = []
        
        # Check for empty data
        if isinstance(frame, pl.DataFrame):
            if len(frame) == 0:
                issues.append(ValidationIssue(
                    level=ValidationResult.WARNING,
                    category="data_quality",
                    message="Empty dataframe",
                    suggestion="Verify data loading and filtering"
                ))
        
        # Additional quality checks could be added here
        
        return issues


# === Configuration Validator ===

class ConfigurationValidator(Validator):
    """Validates estimation configuration."""
    
    def validate(
        self,
        config: EstimatorConfig,
        context: Optional[Dict[str, Any]] = None
    ) -> List[ValidationIssue]:
        """
        Validate estimation configuration.
        
        Parameters
        ----------
        config : EstimatorConfig
            Configuration to validate
        context : Optional[Dict[str, Any]]
            Additional context
            
        Returns
        -------
        List[ValidationIssue]
            Validation issues
        """
        issues = []
        
        # Check basic configuration
        if self.should_validate(ValidationLevel.BASIC):
            # Check estimator type
            if not config.estimator:
                issues.append(ValidationIssue(
                    level=ValidationResult.FAILED,
                    category="configuration",
                    message="No estimator specified",
                    field_name="estimator",
                    suggestion="Set estimator type (e.g., 'tpa', 'volume', 'biomass')"
                ))
            
            # Check evaluation type
            if config.eval_type not in ["VOL", "GRM", "CHNG", "CURR", "ALL"]:
                issues.append(ValidationIssue(
                    level=ValidationResult.WARNING,
                    category="configuration",
                    message=f"Unusual evaluation type: {config.eval_type}",
                    field_name="eval_type",
                    expected="VOL, GRM, CHNG, CURR, or ALL",
                    actual=config.eval_type
                ))
        
        # Check domain specifications
        if self.should_validate(ValidationLevel.STANDARD):
            # Validate domain SQL
            for domain_type in ["tree_domain", "area_domain", "plot_domain"]:
                domain_value = getattr(config, domain_type, None)
                if domain_value:
                    validation_result = self._validate_domain_sql(domain_value)
                    if validation_result:
                        issues.append(ValidationIssue(
                            level=ValidationResult.WARNING,
                            category="configuration",
                            message=f"Potential issue in {domain_type}",
                            field_name=domain_type,
                            actual=domain_value,
                            suggestion=validation_result
                        ))
        
        # Check grouping variables
        if self.should_validate(ValidationLevel.STRICT):
            if config.by_species and config.by_forest_type:
                issues.append(ValidationIssue(
                    level=ValidationResult.WARNING,
                    category="configuration",
                    message="Both by_species and by_forest_type are enabled",
                    suggestion="Consider if both groupings are needed"
                ))
        
        return issues
    
    def _validate_domain_sql(self, domain: str) -> Optional[str]:
        """Validate domain SQL string."""
        # Basic SQL injection check
        dangerous_keywords = ["DROP", "DELETE", "INSERT", "UPDATE", "CREATE", "ALTER"]
        upper_domain = domain.upper()
        
        for keyword in dangerous_keywords:
            if keyword in upper_domain:
                return f"Remove potentially dangerous keyword: {keyword}"
        
        # Check for balanced parentheses
        if domain.count("(") != domain.count(")"):
            return "Unbalanced parentheses in domain expression"
        
        return None


# === Step Compatibility Validator ===

class StepCompatibilityValidator(Validator):
    """Validates compatibility between pipeline steps."""
    
    def validate(
        self,
        steps: List[PipelineStep],
        context: Optional[Dict[str, Any]] = None
    ) -> List[ValidationIssue]:
        """
        Validate step compatibility.
        
        Parameters
        ----------
        steps : List[PipelineStep]
            Pipeline steps to validate
        context : Optional[Dict[str, Any]]
            Additional context
            
        Returns
        -------
        List[ValidationIssue]
            Validation issues
        """
        issues = []
        
        if len(steps) < 2:
            return issues
        
        # Check sequential compatibility
        for i in range(len(steps) - 1):
            current_step = steps[i]
            next_step = steps[i + 1]
            
            # Check contract compatibility
            if self.should_validate(ValidationLevel.BASIC):
                current_output = current_step.get_output_contract()
                next_input = next_step.get_input_contract()
                
                if not self._contracts_compatible(current_output, next_input):
                    issues.append(ValidationIssue(
                        level=ValidationResult.FAILED,
                        category="compatibility",
                        message=f"Incompatible contracts between steps",
                        step_id=f"{current_step.step_id} -> {next_step.step_id}",
                        expected=next_input.__name__,
                        actual=current_output.__name__,
                        suggestion="Add adapter step or modify contracts"
                    ))
            
            # Check for logical flow
            if self.should_validate(ValidationLevel.STANDARD):
                flow_issues = self._check_logical_flow(current_step, next_step)
                issues.extend(flow_issues)
        
        # Check for duplicate step IDs
        if self.should_validate(ValidationLevel.BASIC):
            step_ids = [s.step_id for s in steps]
            duplicates = [sid for sid in step_ids if step_ids.count(sid) > 1]
            
            if duplicates:
                issues.append(ValidationIssue(
                    level=ValidationResult.FAILED,
                    category="compatibility",
                    message=f"Duplicate step IDs found: {duplicates}",
                    suggestion="Ensure all step IDs are unique"
                ))
        
        return issues
    
    def _contracts_compatible(
        self,
        output_contract: Type,
        input_contract: Type
    ) -> bool:
        """Check if contracts are compatible."""
        # Direct match
        if output_contract == input_contract:
            return True
        
        # Check inheritance
        try:
            if issubclass(output_contract, input_contract):
                return True
        except TypeError:
            pass
        
        # Check if adapter exists
        if self._has_adapter(output_contract, input_contract):
            return True
        
        return False
    
    def _has_adapter(
        self,
        from_contract: Type,
        to_contract: Type
    ) -> bool:
        """Check if adapter exists between contracts."""
        # Known compatible transitions
        compatible_transitions = [
            (RawTablesContract, FilteredDataContract),
            (FilteredDataContract, JoinedDataContract),
            (JoinedDataContract, ValuedDataContract),
            (ValuedDataContract, PlotEstimatesContract),
            (PlotEstimatesContract, StratifiedEstimatesContract),
            (StratifiedEstimatesContract, PopulationEstimatesContract),
            (PopulationEstimatesContract, FormattedOutputContract)
        ]
        
        for from_type, to_type in compatible_transitions:
            if from_contract == from_type and to_contract == to_type:
                return True
        
        return False
    
    def _check_logical_flow(
        self,
        current_step: PipelineStep,
        next_step: PipelineStep
    ) -> List[ValidationIssue]:
        """Check logical flow between steps."""
        issues = []
        
        # Example: Check if filtering comes before joining
        if "join" in current_step.step_id.lower() and "filter" in next_step.step_id.lower():
            issues.append(ValidationIssue(
                level=ValidationResult.WARNING,
                category="flow",
                message="Filtering after joining may be inefficient",
                step_id=f"{current_step.step_id} -> {next_step.step_id}",
                suggestion="Consider filtering before joining for better performance"
            ))
        
        return issues


# === Pre-execution Validator ===

class PreExecutionValidator(Validator):
    """Validates pipeline before execution."""
    
    def validate(
        self,
        pipeline: Any,  # Would be EstimationPipeline
        context: Optional[Dict[str, Any]] = None
    ) -> List[ValidationIssue]:
        """
        Perform pre-execution validation.
        
        Parameters
        ----------
        pipeline : Any
            Pipeline to validate
        context : Optional[Dict[str, Any]]
            Additional context including db and config
            
        Returns
        -------
        List[ValidationIssue]
            Validation issues
        """
        issues = []
        
        # Check pipeline has steps
        if self.should_validate(ValidationLevel.BASIC):
            if not hasattr(pipeline, "steps") or not pipeline.steps:
                issues.append(ValidationIssue(
                    level=ValidationResult.FAILED,
                    category="pipeline",
                    message="Pipeline has no steps",
                    suggestion="Add steps to the pipeline"
                ))
                return issues
        
        # Check database connection
        if context and "db" in context:
            db_issues = self._validate_database(context["db"])
            issues.extend(db_issues)
        
        # Check resource availability
        if self.should_validate(ValidationLevel.STANDARD):
            resource_issues = self._check_resources()
            issues.extend(resource_issues)
        
        # Estimate execution requirements
        if self.should_validate(ValidationLevel.STRICT):
            requirements = self._estimate_requirements(pipeline)
            if requirements:
                for req_type, req_value in requirements.items():
                    if req_type == "memory_gb" and req_value > 16:
                        issues.append(ValidationIssue(
                            level=ValidationResult.WARNING,
                            category="resources",
                            message=f"Pipeline may require {req_value}GB memory",
                            suggestion="Consider using lazy evaluation or batching"
                        ))
        
        return issues
    
    def _validate_database(self, db: Any) -> List[ValidationIssue]:
        """Validate database connection."""
        issues = []
        
        # Check if database is connected
        if not hasattr(db, "engine") or db.engine is None:
            issues.append(ValidationIssue(
                level=ValidationResult.FAILED,
                category="database",
                message="Database not connected",
                suggestion="Ensure database connection is established"
            ))
        
        # Check required tables
        required_tables = ["PLOT", "TREE", "COND", "POP_EVAL"]
        if hasattr(db, "engine"):
            try:
                existing_tables = db.engine.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                existing_names = [t[0] for t in existing_tables]
                
                missing_tables = [t for t in required_tables if t not in existing_names]
                if missing_tables:
                    issues.append(ValidationIssue(
                        level=ValidationResult.WARNING,
                        category="database",
                        message=f"Missing expected tables: {missing_tables}",
                        suggestion="Verify database is complete FIA database"
                    ))
            except Exception as e:
                issues.append(ValidationIssue(
                    level=ValidationResult.WARNING,
                    category="database",
                    message=f"Could not verify database tables: {e}"
                ))
        
        return issues
    
    def _check_resources(self) -> List[ValidationIssue]:
        """Check system resources."""
        issues = []
        
        try:
            import psutil
            
            # Check available memory
            mem = psutil.virtual_memory()
            if mem.available < 1e9:  # Less than 1GB
                issues.append(ValidationIssue(
                    level=ValidationResult.WARNING,
                    category="resources",
                    message=f"Low available memory: {mem.available / 1e9:.1f}GB",
                    suggestion="Close other applications or use lazy evaluation"
                ))
            
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)
            if cpu_percent > 90:
                issues.append(ValidationIssue(
                    level=ValidationResult.WARNING,
                    category="resources",
                    message=f"High CPU usage: {cpu_percent}%",
                    suggestion="Consider delaying pipeline execution"
                ))
                
        except ImportError:
            # psutil not available, skip resource checks
            pass
        
        return issues
    
    def _estimate_requirements(self, pipeline: Any) -> Dict[str, Any]:
        """Estimate pipeline resource requirements."""
        requirements = {}
        
        # Estimate based on steps
        if hasattr(pipeline, "steps"):
            num_steps = len(pipeline.steps)
            
            # Rough estimates
            requirements["memory_gb"] = num_steps * 0.5  # 500MB per step estimate
            requirements["execution_time_seconds"] = num_steps * 2  # 2s per step estimate
            
            # Check for memory-intensive steps
            for step in pipeline.steps:
                if "join" in step.step_id.lower():
                    requirements["memory_gb"] += 2
                elif "stratif" in step.step_id.lower():
                    requirements["memory_gb"] += 1
        
        return requirements


# === Main Pipeline Validator ===

class PipelineValidator:
    """
    Comprehensive pipeline validation orchestrator.
    
    Coordinates all validation types and produces unified validation reports.
    """
    
    def __init__(
        self,
        validation_level: ValidationLevel = ValidationLevel.STANDARD,
        stop_on_error: bool = False
    ):
        """
        Initialize pipeline validator.
        
        Parameters
        ----------
        validation_level : ValidationLevel
            Level of validation to perform
        stop_on_error : bool
            Whether to stop validation on first error
        """
        self.validation_level = validation_level
        self.stop_on_error = stop_on_error
        self.console = Console()
        
        # Initialize validators
        self.schema_validator = SchemaValidator(validation_level)
        self.config_validator = ConfigurationValidator(validation_level)
        self.compatibility_validator = StepCompatibilityValidator(validation_level)
        self.pre_execution_validator = PreExecutionValidator(validation_level)
    
    def validate_pipeline(
        self,
        pipeline: Any,
        db: Optional[Any] = None,
        config: Optional[EstimatorConfig] = None,
        show_report: bool = True
    ) -> ValidationReport:
        """
        Perform complete pipeline validation.
        
        Parameters
        ----------
        pipeline : Any
            Pipeline to validate
        db : Optional[Any]
            Database connection
        config : Optional[EstimatorConfig]
            Estimation configuration
        show_report : bool
            Whether to display validation report
            
        Returns
        -------
        ValidationReport
            Complete validation report
        """
        import time
        
        # Initialize report
        report = ValidationReport(
            pipeline_id=getattr(pipeline, "pipeline_id", "unknown"),
            timestamp=time.time(),
            validation_level=self.validation_level
        )
        
        # Validate configuration
        if config:
            config_issues = self.config_validator.validate(config)
            for issue in config_issues:
                report.add_issue(issue)
                if self.stop_on_error and issue.level == ValidationResult.FAILED:
                    return report
        
        # Validate step compatibility
        if hasattr(pipeline, "steps"):
            compat_issues = self.compatibility_validator.validate(pipeline.steps)
            for issue in compat_issues:
                report.add_issue(issue)
                if self.stop_on_error and issue.level == ValidationResult.FAILED:
                    return report
        
        # Pre-execution validation
        context = {"db": db, "config": config} if db or config else None
        pre_exec_issues = self.pre_execution_validator.validate(pipeline, context)
        for issue in pre_exec_issues:
            report.add_issue(issue)
            if self.stop_on_error and issue.level == ValidationResult.FAILED:
                return report
        
        # Display report if requested
        if show_report:
            self.display_report(report)
        
        return report
    
    def validate_data_contract(
        self,
        data: Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper],
        contract: Type[DataContract],
        show_report: bool = True
    ) -> ValidationReport:
        """
        Validate data against contract.
        
        Parameters
        ----------
        data : Union[pl.DataFrame, pl.LazyFrame, LazyFrameWrapper]
            Data to validate
        contract : Type[DataContract]
            Contract to validate against
        show_report : bool
            Whether to display report
            
        Returns
        -------
        ValidationReport
            Validation report
        """
        import time
        
        report = ValidationReport(
            pipeline_id="data_validation",
            timestamp=time.time(),
            validation_level=self.validation_level
        )
        
        schema_issues = self.schema_validator.validate(data, contract)
        for issue in schema_issues:
            report.add_issue(issue)
        
        if show_report:
            self.display_report(report)
        
        return report
    
    def display_report(self, report: ValidationReport) -> None:
        """Display validation report in console."""
        # Create summary table
        table = Table(title=f"Validation Report: {report.pipeline_id}")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        table.add_row("Validation Level", report.validation_level.value)
        table.add_row("Checks Performed", str(report.checks_performed))
        table.add_row("Checks Passed", f"[green]{report.checks_passed}[/green]")
        table.add_row("Checks Failed", f"[red]{report.checks_failed}[/red]")
        table.add_row("Warnings", f"[yellow]{report.checks_warned}[/yellow]")
        table.add_row("Valid", "[green]Yes[/green]" if report.is_valid else "[red]No[/red]")
        
        self.console.print(table)
        
        # Show critical issues
        critical_issues = report.get_issues_by_level(ValidationResult.FAILED)
        if critical_issues:
            self.console.print("\n[red]Critical Issues:[/red]")
            for issue in critical_issues:
                self.console.print(f"  • {issue.message}")
                if issue.suggestion:
                    self.console.print(f"    [dim]→ {issue.suggestion}[/dim]")
        
        # Show warnings
        warnings = report.get_issues_by_level(ValidationResult.WARNING)
        if warnings and self.validation_level != ValidationLevel.BASIC:
            self.console.print("\n[yellow]Warnings:[/yellow]")
            for issue in warnings[:5]:  # Show first 5 warnings
                self.console.print(f"  • {issue.message}")
            if len(warnings) > 5:
                self.console.print(f"  [dim]... and {len(warnings) - 5} more[/dim]")


# === Validation Decorators ===

def validate_input(contract: Type[DataContract]) -> Callable:
    """Decorator to validate step input."""
    def decorator(func: Callable) -> Callable:
        def wrapper(self, input_data: Any, context: ExecutionContext) -> Any:
            # Validate input
            validator = SchemaValidator()
            issues = validator.validate(input_data, contract)
            
            if any(i.level == ValidationResult.FAILED for i in issues):
                raise StepValidationError(
                    f"Input validation failed: {issues[0].message}",
                    step_id=getattr(self, "step_id", None)
                )
            
            return func(self, input_data, context)
        return wrapper
    return decorator


def validate_output(contract: Type[DataContract]) -> Callable:
    """Decorator to validate step output."""
    def decorator(func: Callable) -> Callable:
        def wrapper(self, input_data: Any, context: ExecutionContext) -> Any:
            result = func(self, input_data, context)
            
            # Validate output
            validator = SchemaValidator()
            issues = validator.validate(result, contract)
            
            if any(i.level == ValidationResult.FAILED for i in issues):
                raise StepValidationError(
                    f"Output validation failed: {issues[0].message}",
                    step_id=getattr(self, "step_id", None)
                )
            
            return result
        return wrapper
    return decorator


def validate_config(validation_func: Callable[[EstimatorConfig], bool]) -> Callable:
    """Decorator to validate configuration."""
    def decorator(func: Callable) -> Callable:
        def wrapper(self, input_data: Any, context: ExecutionContext) -> Any:
            if not validation_func(context.config):
                raise StepValidationError(
                    "Configuration validation failed",
                    step_id=getattr(self, "step_id", None)
                )
            
            return func(self, input_data, context)
        return wrapper
    return decorator


# === Export public API ===

__all__ = [
    "PipelineValidator",
    "SchemaValidator",
    "ConfigurationValidator",
    "StepCompatibilityValidator",
    "PreExecutionValidator",
    "ValidationLevel",
    "ValidationResult",
    "ValidationIssue",
    "ValidationReport",
    "validate_input",
    "validate_output",
    "validate_config"
]