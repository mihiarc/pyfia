"""
Testing framework for pipeline steps and composed pipelines.

This module provides comprehensive testing utilities for validating
individual pipeline steps and complete estimation workflows with
mock data, assertions, and performance testing capabilities.
"""

from typing import Any, Callable, Dict, List, Optional, Type, Union
from abc import ABC, abstractmethod
import time
import warnings
from dataclasses import dataclass, field

import polars as pl
import numpy as np

from ...core import FIA
from ..config import EstimatorConfig
from ..lazy_evaluation import LazyFrameWrapper

from .core import (
    PipelineStep,
    EstimationPipeline,
    ExecutionContext,
    DataContract,
    StepResult,
    StepStatus,
    TableDataContract,
    FilteredDataContract,
    JoinedDataContract,
    ValuedDataContract,
    PlotEstimatesContract,
    StratifiedEstimatesContract,
    PopulationEstimatesContract,
    FormattedOutputContract,
    PipelineException,
    StepValidationError,
    TInput,
    TOutput
)


# === Mock Data Structures ===

@dataclass
class MockFIADatabase:
    """Mock FIA database for testing."""
    
    tables: Dict[str, pl.DataFrame] = field(default_factory=dict)
    current_evalids: Optional[List[int]] = None
    
    def load_table(self, table_name: str) -> None:
        """Mock table loading - tables are pre-populated."""
        if table_name not in self.tables:
            warnings.warn(f"Table {table_name} not found in mock database")
    
    def get_table(self, table_name: str) -> Optional[pl.DataFrame]:
        """Get table from mock database."""
        return self.tables.get(table_name)


@dataclass
class TestDataFactory:
    """
    Factory for creating mock test data.
    
    Generates realistic FIA data structures for testing
    pipeline components without requiring a real database.
    """
    
    @staticmethod
    def create_plot_data(n_plots: int = 100, statecd: int = 37) -> pl.DataFrame:
        """
        Create mock PLOT data.
        
        Parameters
        ----------
        n_plots : int
            Number of plots to create
        statecd : int
            State code for plots
            
        Returns
        -------
        pl.DataFrame
            Mock plot data
        """
        plot_data = {
            "PLT_CN": [f"PLOT_{i:06d}" for i in range(1, n_plots + 1)],
            "STATECD": [statecd] * n_plots,
            "UNITCD": np.random.randint(1, 5, n_plots).tolist(),
            "COUNTYCD": np.random.randint(1, 50, n_plots).tolist(),
            "PLOT": np.arange(1, n_plots + 1).tolist(),
            "EVALID": [202401] * n_plots,  # 2024 evaluation
            "INVYR": [2024] * n_plots,
            "LAT": (35.0 + np.random.random(n_plots) * 2).tolist(),  # NC latitude range
            "LON": (-84.0 + np.random.random(n_plots) * 4).tolist(),  # NC longitude range
        }
        
        return pl.DataFrame(plot_data)
    
    @staticmethod
    def create_condition_data(plot_data: pl.DataFrame) -> pl.DataFrame:
        """
        Create mock COND data based on plot data.
        
        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot data to base conditions on
            
        Returns
        -------
        pl.DataFrame
        Mock condition data
        """
        n_plots = len(plot_data)
        
        # Most plots have 1 condition, some have 2-3
        conditions_per_plot = np.random.choice([1, 2, 3], n_plots, p=[0.7, 0.25, 0.05])
        
        cond_data = []
        for i, plt_cn in enumerate(plot_data["PLT_CN"]):
            for condid in range(1, conditions_per_plot[i] + 1):
                cond_data.append({
                    "PLT_CN": plt_cn,
                    "CONDID": condid,
                    "COND_STATUS_CD": np.random.choice([1, 2, 3], p=[0.8, 0.15, 0.05]),  # Mostly accessible forest
                    "CONDPROP_UNADJ": np.random.uniform(0.1, 1.0),
                    "LANDCLCD": np.random.choice([10, 20, 30], p=[0.7, 0.2, 0.1]),  # Mostly forest
                    "FORTYPCD": np.random.randint(100, 400),
                    "OWNCD": np.random.choice([10, 20, 30, 40], p=[0.3, 0.4, 0.2, 0.1]),
                    "OWNGRPCD": np.random.choice([10, 20, 30, 40], p=[0.3, 0.4, 0.2, 0.1]),
                })
        
        return pl.DataFrame(cond_data)
    
    @staticmethod
    def create_tree_data(condition_data: pl.DataFrame) -> pl.DataFrame:
        """
        Create mock TREE data based on condition data.
        
        Parameters
        ----------
        condition_data : pl.DataFrame
            Condition data to base trees on
            
        Returns
        -------
        pl.DataFrame
            Mock tree data
        """
        tree_data = []
        tree_id = 1
        
        for row in condition_data.iter_rows(named=True):
            plt_cn = row["PLT_CN"]
            condid = row["CONDID"]
            
            # Number of trees per condition varies
            n_trees = np.random.poisson(15)  # Average 15 trees per condition
            
            for _ in range(n_trees):
                # Generate realistic tree attributes
                dia = np.random.lognormal(2.5, 0.5)  # Diameter distribution
                ht = 4.5 + dia * 5 + np.random.normal(0, 10)  # Height-diameter relationship
                ht = max(ht, 4.5)  # Minimum height
                
                # Species distribution (common southeastern species)
                spcd = np.random.choice(
                    [131, 110, 802, 833, 531],  # Loblolly, VA pine, white oak, chestnut oak, yellow poplar
                    p=[0.4, 0.2, 0.15, 0.15, 0.1]
                )
                
                tree_data.append({
                    "PLT_CN": plt_cn,
                    "CONDID": condid,
                    "TREE": tree_id,
                    "SUBP": np.random.randint(1, 5),
                    "SPCD": spcd,
                    "SPGRPCD": spcd // 100,  # Species group
                    "DIA": dia,
                    "HT": ht,
                    "ACTUALHT": ht if np.random.random() > 0.3 else None,  # Some missing heights
                    "STATUSCD": np.random.choice([1, 2, 3], p=[0.85, 0.10, 0.05]),  # Mostly live
                    "TPAADJ": 6.018046,  # Standard subplot expansion factor
                    "CARBON_AG": dia ** 2 * 0.02 + np.random.normal(0, 5),  # Mock carbon
                    "DRYBIO_AG": dia ** 2 * 0.05 + np.random.normal(0, 10),  # Mock biomass
                    "VOLCFNET": dia ** 2 * 0.005 + np.random.normal(0, 2),  # Mock volume
                    "AGENTCD": np.random.choice([0, 10, 20, 30], p=[0.8, 0.1, 0.05, 0.05]),  # Mortality agent
                    "PREVDIA": dia - np.random.uniform(0, 2) if np.random.random() > 0.3 else None,  # Previous diameter
                })
                
                tree_id += 1
        
        return pl.DataFrame(tree_data)
    
    @staticmethod
    def create_stratification_data(plot_data: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Create mock stratification data.
        
        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot data to create stratification for
            
        Returns
        -------
        tuple[pl.DataFrame, pl.DataFrame]
            Plot stratum assignments and stratum info
        """
        # Create 3 strata
        n_strata = 3
        stratum_data = []
        plot_assignments = []
        
        for stratum_id in range(1, n_strata + 1):
            stratum_cn = f"STRATUM_{stratum_id:03d}"
            stratum_data.append({
                "STRATUM_CN": stratum_cn,
                "EXPNS": np.random.uniform(100, 1000),  # Expansion factor
                "P2POINTCNT": np.random.randint(50, 200),  # Points in stratum
                "STRATUM_AREA": np.random.uniform(10000, 50000),  # Stratum area
            })
        
        # Assign plots to strata randomly
        for plt_cn in plot_data["PLT_CN"]:
            stratum_id = np.random.randint(1, n_strata + 1)
            stratum_cn = f"STRATUM_{stratum_id:03d}"
            
            plot_assignments.append({
                "PLT_CN": plt_cn,
                "STRATUM_CN": stratum_cn,
                "ADJ_FACTOR_MICRO_PLOT": np.random.uniform(0.8, 1.2),
            })
        
        return pl.DataFrame(plot_assignments), pl.DataFrame(stratum_data)
    
    @classmethod
    def create_complete_mock_database(
        self,
        n_plots: int = 100,
        statecd: int = 37
    ) -> MockFIADatabase:
        """
        Create a complete mock FIA database.
        
        Parameters
        ----------
        n_plots : int
            Number of plots to create
        statecd : int
            State code
            
        Returns
        -------
        MockFIADatabase
            Complete mock database
        """
        # Create core data
        plot_data = self.create_plot_data(n_plots, statecd)
        cond_data = self.create_condition_data(plot_data)
        tree_data = self.create_tree_data(cond_data)
        
        # Create stratification data
        plot_stratum, stratum_info = self.create_stratification_data(plot_data)
        
        # Create database
        mock_db = MockFIADatabase()
        mock_db.tables = {
            "PLOT": plot_data,
            "COND": cond_data,
            "TREE": tree_data,
            "POP_PLOT_STRATUM_ASSGN": plot_stratum,
            "POP_STRATUM": stratum_info,
        }
        mock_db.current_evalids = [202401]
        
        return mock_db


# === Mock Pipeline Steps ===

class MockStep(PipelineStep[TInput, TOutput]):
    """
    Mock pipeline step for testing.
    
    Provides configurable behavior for testing pipeline
    execution without real step implementations.
    """
    
    def __init__(
        self,
        input_contract: Type[TInput],
        output_contract: Type[TOutput],
        output_data: Optional[TOutput] = None,
        should_fail: bool = False,
        execution_time: float = 0.1,
        **kwargs
    ):
        """
        Initialize mock step.
        
        Parameters
        ----------
        input_contract : Type[TInput]
            Input contract type
        output_contract : Type[TOutput]
            Output contract type
        output_data : Optional[TOutput]
            Output data to return (if None, returns input as output)
        should_fail : bool
            Whether step should fail
        execution_time : float
            Mock execution time
        """
        super().__init__(**kwargs)
        self._input_contract = input_contract
        self._output_contract = output_contract
        self._output_data = output_data
        self._should_fail = should_fail
        self._execution_time = execution_time
    
    def get_input_contract(self) -> Type[TInput]:
        """Get input contract."""
        return self._input_contract
    
    def get_output_contract(self) -> Type[TOutput]:
        """Get output contract."""
        return self._output_contract
    
    def execute_step(self, input_data: TInput, context: ExecutionContext) -> TOutput:
        """Execute mock step."""
        # Simulate execution time
        time.sleep(self._execution_time)
        
        if self._should_fail:
            raise PipelineException(f"Mock step {self.step_id} configured to fail")
        
        if self._output_data:
            return self._output_data
        else:
            # Return input as output (type assertion for testing)
            return input_data


# === Step Testing Framework ===

class StepTester:
    """
    Framework for testing individual pipeline steps.
    
    Provides utilities for creating test inputs, running steps
    in isolation, and validating outputs.
    """
    
    def __init__(self, step: PipelineStep):
        """
        Initialize step tester.
        
        Parameters
        ----------
        step : PipelineStep
            Step to test
        """
        self.step = step
        self.test_results: List[Dict[str, Any]] = []
    
    def test_with_data(
        self,
        input_data: DataContract,
        db: Optional[MockFIADatabase] = None,
        config: Optional[EstimatorConfig] = None,
        expected_output_type: Optional[Type] = None,
        should_fail: bool = False
    ) -> StepResult:
        """
        Test step with specific input data.
        
        Parameters
        ----------
        input_data : DataContract
            Input data for testing
        db : Optional[MockFIADatabase]
            Mock database to use
        config : Optional[EstimatorConfig]
            Configuration to use
        expected_output_type : Optional[Type]
            Expected output type for validation
        should_fail : bool
            Whether step is expected to fail
            
        Returns
        -------
        StepResult
            Step execution result
        """
        # Create mock database and config if not provided
        if db is None:
            db = TestDataFactory.create_complete_mock_database()
        
        if config is None:
            config = EstimatorConfig()
        
        # Create execution context
        context = ExecutionContext(db, config, debug=True)
        
        # Execute step
        result = self.step.execute(input_data, context)
        
        # Validate result
        test_result = {
            "step_id": self.step.step_id,
            "success": result.success,
            "execution_time": result.execution_time,
            "error": str(result.error) if result.error else None,
            "warnings": result.warnings,
            "expected_failure": should_fail,
            "test_passed": result.success != should_fail
        }
        
        # Type validation
        if expected_output_type and result.success:
            type_match = isinstance(result.output, expected_output_type)
            test_result["type_validation"] = type_match
            test_result["actual_type"] = type(result.output).__name__
            test_result["expected_type"] = expected_output_type.__name__
        
        self.test_results.append(test_result)
        
        return result
    
    def test_with_mock_data(
        self,
        n_plots: int = 10,
        statecd: int = 37,
        **config_kwargs
    ) -> StepResult:
        """
        Test step with generated mock data.
        
        Parameters
        ----------
        n_plots : int
            Number of plots in mock data
        statecd : int
            State code for mock data
        **config_kwargs
            Configuration parameters
            
        Returns
        -------
        StepResult
            Step execution result
        """
        # Create mock database
        db = TestDataFactory.create_complete_mock_database(n_plots, statecd)
        config = EstimatorConfig(**config_kwargs)
        
        # Create appropriate input data for step type
        input_data = self._create_appropriate_input(db, config)
        
        return self.test_with_data(input_data, db, config)
    
    def _create_appropriate_input(
        self,
        db: MockFIADatabase,
        config: EstimatorConfig
    ) -> DataContract:
        """Create appropriate input data based on step's input contract."""
        input_contract = self.step.get_input_contract()
        
        if input_contract == TableDataContract:
            return TableDataContract(
                tables={name: LazyFrameWrapper(df) for name, df in db.tables.items()},
                evalid=db.current_evalids
            )
        elif input_contract == FilteredDataContract:
            return FilteredDataContract(
                tree_data=LazyFrameWrapper(db.tables["TREE"]) if "TREE" in db.tables else None,
                condition_data=LazyFrameWrapper(db.tables["COND"]),
                plot_data=LazyFrameWrapper(db.tables["PLOT"]) if "PLOT" in db.tables else None
            )
        elif input_contract == JoinedDataContract:
            # Create simple joined data
            joined_df = db.tables["TREE"].join(db.tables["COND"], on=["PLT_CN", "CONDID"])
            return JoinedDataContract(
                data=LazyFrameWrapper(joined_df),
                group_columns=[]
            )
        else:
            # Default to empty TableDataContract
            return TableDataContract(tables={})
    
    def run_performance_test(
        self,
        n_iterations: int = 10,
        n_plots: int = 100,
        **config_kwargs
    ) -> Dict[str, Any]:
        """
        Run performance test on step.
        
        Parameters
        ----------
        n_iterations : int
            Number of test iterations
        n_plots : int
            Number of plots in test data
        **config_kwargs
            Configuration parameters
            
        Returns
        -------
        Dict[str, Any]
            Performance test results
        """
        execution_times = []
        memory_usage = []
        
        for _ in range(n_iterations):
            result = self.test_with_mock_data(n_plots=n_plots, **config_kwargs)
            execution_times.append(result.execution_time)
            if result.memory_used:
                memory_usage.append(result.memory_used)
        
        return {
            "iterations": n_iterations,
            "n_plots": n_plots,
            "mean_execution_time": np.mean(execution_times),
            "std_execution_time": np.std(execution_times),
            "min_execution_time": np.min(execution_times),
            "max_execution_time": np.max(execution_times),
            "mean_memory_usage": np.mean(memory_usage) if memory_usage else None,
            "std_memory_usage": np.std(memory_usage) if memory_usage else None,
        }
    
    def get_test_summary(self) -> Dict[str, Any]:
        """Get summary of all tests run."""
        if not self.test_results:
            return {"message": "No tests run"}
        
        return {
            "total_tests": len(self.test_results),
            "passed_tests": sum(1 for r in self.test_results if r["test_passed"]),
            "failed_tests": sum(1 for r in self.test_results if not r["test_passed"]),
            "average_execution_time": np.mean([r["execution_time"] for r in self.test_results]),
            "test_details": self.test_results
        }


# === Pipeline Testing Framework ===

class PipelineTester:
    """
    Framework for testing complete estimation pipelines.
    
    Provides end-to-end testing of pipeline execution
    with validation of intermediate steps and final outputs.
    """
    
    def __init__(self, pipeline: EstimationPipeline):
        """
        Initialize pipeline tester.
        
        Parameters
        ----------
        pipeline : EstimationPipeline
            Pipeline to test
        """
        self.pipeline = pipeline
        self.test_results: List[Dict[str, Any]] = []
    
    def test_pipeline_execution(
        self,
        db: Optional[MockFIADatabase] = None,
        config: Optional[EstimatorConfig] = None,
        expected_output_columns: Optional[List[str]] = None,
        min_output_records: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Test complete pipeline execution.
        
        Parameters
        ----------
        db : Optional[MockFIADatabase]
            Mock database to use
        config : Optional[EstimatorConfig]
            Configuration to use
        expected_output_columns : Optional[List[str]]
            Expected columns in output
        min_output_records : Optional[int]
            Minimum number of output records expected
            
        Returns
        -------
        Dict[str, Any]
            Test results
        """
        # Create defaults if not provided
        if db is None:
            db = TestDataFactory.create_complete_mock_database()
        
        if config is None:
            config = EstimatorConfig()
        
        test_start = time.time()
        
        try:
            # Execute pipeline
            result_df = self.pipeline.execute(db, config)
            
            test_passed = True
            errors = []
            
            # Validate output
            if expected_output_columns:
                missing_cols = set(expected_output_columns) - set(result_df.columns)
                if missing_cols:
                    errors.append(f"Missing expected columns: {missing_cols}")
                    test_passed = False
            
            if min_output_records and len(result_df) < min_output_records:
                errors.append(
                    f"Output has {len(result_df)} records, minimum expected: {min_output_records}"
                )
                test_passed = False
            
            test_result = {
                "success": True,
                "test_passed": test_passed,
                "execution_time": time.time() - test_start,
                "output_records": len(result_df),
                "output_columns": result_df.columns,
                "errors": errors,
                "pipeline_summary": self.pipeline.get_execution_summary()
            }
            
        except Exception as e:
            test_result = {
                "success": False,
                "test_passed": False,
                "execution_time": time.time() - test_start,
                "error": str(e),
                "pipeline_summary": self.pipeline.get_execution_summary()
            }
        
        self.test_results.append(test_result)
        return test_result
    
    def test_pipeline_validation(self) -> Dict[str, Any]:
        """
        Test pipeline validation without execution.
        
        Returns
        -------
        Dict[str, Any]
            Validation test results
        """
        validation_issues = self.pipeline.validate_pipeline()
        
        return {
            "valid": len(validation_issues) == 0,
            "issues": validation_issues,
            "step_count": len(self.pipeline.steps),
            "step_ids": [step.step_id for step in self.pipeline.steps]
        }
    
    def test_step_isolation(self, step_index: int) -> Dict[str, Any]:
        """
        Test individual step in pipeline context.
        
        Parameters
        ----------
        step_index : int
            Index of step to test in isolation
            
        Returns
        -------
        Dict[str, Any]
            Isolated step test results
        """
        if step_index >= len(self.pipeline.steps):
            return {"error": f"Step index {step_index} out of range"}
        
        step = self.pipeline.steps[step_index]
        step_tester = StepTester(step)
        
        # Run step test
        result = step_tester.test_with_mock_data()
        
        return {
            "step_id": step.step_id,
            "step_index": step_index,
            "success": result.success,
            "execution_time": result.execution_time,
            "error": str(result.error) if result.error else None
        }
    
    def run_pipeline_benchmark(
        self,
        n_iterations: int = 5,
        plot_sizes: List[int] = [10, 50, 100, 500]
    ) -> Dict[str, Any]:
        """
        Run benchmark tests with different data sizes.
        
        Parameters
        ----------
        n_iterations : int
            Number of iterations per size
        plot_sizes : List[int]
            Different plot sizes to test
            
        Returns
        -------
        Dict[str, Any]
            Benchmark results
        """
        benchmark_results = {}
        
        for n_plots in plot_sizes:
            size_results = []
            
            for _ in range(n_iterations):
                db = TestDataFactory.create_complete_mock_database(n_plots=n_plots)
                config = EstimatorConfig()
                
                start_time = time.time()
                try:
                    result_df = self.pipeline.execute(db, config)
                    execution_time = time.time() - start_time
                    size_results.append({
                        "success": True,
                        "execution_time": execution_time,
                        "output_records": len(result_df)
                    })
                except Exception as e:
                    size_results.append({
                        "success": False,
                        "execution_time": time.time() - start_time,
                        "error": str(e)
                    })
            
            # Calculate statistics
            successful_runs = [r for r in size_results if r["success"]]
            if successful_runs:
                times = [r["execution_time"] for r in successful_runs]
                benchmark_results[n_plots] = {
                    "successful_runs": len(successful_runs),
                    "failed_runs": len(size_results) - len(successful_runs),
                    "mean_time": np.mean(times),
                    "std_time": np.std(times),
                    "min_time": np.min(times),
                    "max_time": np.max(times),
                }
            else:
                benchmark_results[n_plots] = {
                    "successful_runs": 0,
                    "failed_runs": len(size_results),
                    "error": "All runs failed"
                }
        
        return benchmark_results


# === Assertion Utilities ===

class AssertionStep(PipelineStep[TInput, TInput]):
    """
    Pipeline step that performs assertions for testing.
    
    Allows embedding test assertions directly in pipelines
    for validation during execution.
    """
    
    def __init__(
        self,
        assertion_function: Callable[[TInput, ExecutionContext], bool],
        assertion_message: str = "Assertion failed",
        **kwargs
    ):
        """
        Initialize assertion step.
        
        Parameters
        ----------
        assertion_function : Callable
            Function that performs the assertion
        assertion_message : str
            Message to show if assertion fails
        """
        super().__init__(**kwargs)
        self._assertion_function = assertion_function
        self._assertion_message = assertion_message
    
    def get_input_contract(self) -> Type[TInput]:
        """Input contract is generic."""
        return DataContract
    
    def get_output_contract(self) -> Type[TInput]:
        """Output contract same as input."""
        return DataContract
    
    def execute_step(self, input_data: TInput, context: ExecutionContext) -> TInput:
        """Execute assertion and pass through data."""
        try:
            assertion_result = self._assertion_function(input_data, context)
            if not assertion_result:
                raise AssertionError(self._assertion_message)
        except Exception as e:
            raise PipelineException(
                f"Assertion failed in step {self.step_id}: {e}",
                step_id=self.step_id,
                cause=e
            )
        
        # Pass through input unchanged
        return input_data


# === Common Assertion Functions ===

def assert_data_not_empty(data: DataContract, context: ExecutionContext) -> bool:
    """Assert that data contains non-empty frames."""
    for field_name, field_value in data.model_dump().items():
        if isinstance(field_value, (pl.DataFrame, pl.LazyFrame, LazyFrameWrapper)):
            if isinstance(field_value, LazyFrameWrapper):
                frame = field_value.collect() if field_value.is_lazy else field_value.frame
            elif isinstance(field_value, pl.LazyFrame):
                frame = field_value.collect()
            else:
                frame = field_value
            
            if len(frame) == 0:
                return False
    
    return True


def assert_required_columns(
    required_columns: List[str]
) -> Callable[[DataContract, ExecutionContext], bool]:
    """Create assertion function for required columns."""
    
    def assertion_func(data: DataContract, context: ExecutionContext) -> bool:
        for field_name, field_value in data.model_dump().items():
            if isinstance(field_value, (pl.DataFrame, pl.LazyFrame, LazyFrameWrapper)):
                if isinstance(field_value, LazyFrameWrapper):
                    frame = field_value.frame
                else:
                    frame = field_value
                
                if isinstance(frame, pl.LazyFrame):
                    available_cols = set(frame.collect_schema().names())
                else:
                    available_cols = set(frame.columns)
                
                missing_cols = set(required_columns) - available_cols
                if missing_cols:
                    return False
        
        return True
    
    return assertion_func


def assert_no_null_values(
    columns: List[str]
) -> Callable[[DataContract, ExecutionContext], bool]:
    """Create assertion function for null value checks."""
    
    def assertion_func(data: DataContract, context: ExecutionContext) -> bool:
        for field_name, field_value in data.model_dump().items():
            if isinstance(field_value, (pl.DataFrame, pl.LazyFrame, LazyFrameWrapper)):
                if isinstance(field_value, LazyFrameWrapper):
                    frame = field_value.collect() if field_value.is_lazy else field_value.frame
                elif isinstance(field_value, pl.LazyFrame):
                    frame = field_value.collect()
                else:
                    frame = field_value
                
                for col in columns:
                    if col in frame.columns:
                        null_count = frame.select(pl.col(col).is_null().sum()).item()
                        if null_count > 0:
                            return False
        
        return True
    
    return assertion_func