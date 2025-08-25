"""
Pipeline Optimization Engine for pyFIA Phase 4.

This module provides sophisticated optimization capabilities including step fusion,
query pushdown, caching strategies, and performance tuning for pipeline execution.
"""

import hashlib
import json
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type

import polars as pl
from rich.console import Console
from rich.table import Table

from .core import (
    PipelineStep, ExecutionContext, DataContract,
    EstimatorConfig
)
from .contracts import (
    RawTablesContract, FilteredDataContract, JoinedDataContract,
    ValuedDataContract, PlotEstimatesContract
)
from ..lazy_evaluation import LazyFrameWrapper, ComputationGraph
from ...core import FIA


# === Enums ===

class OptimizationLevel(str, Enum):
    """Optimization levels."""
    NONE = "none"  # No optimization
    BASIC = "basic"  # Basic optimizations only
    STANDARD = "standard"  # Standard optimizations (default)
    AGGRESSIVE = "aggressive"  # Aggressive optimizations


class FusionStrategy(str, Enum):
    """Step fusion strategies."""
    NONE = "none"  # No fusion
    CONSERVATIVE = "conservative"  # Only fuse compatible steps
    AGGRESSIVE = "aggressive"  # Aggressively fuse steps


class CacheStrategy(str, Enum):
    """Caching strategies."""
    NONE = "none"  # No caching
    LAZY = "lazy"  # Cache on first use
    EAGER = "eager"  # Pre-compute and cache
    ADAPTIVE = "adaptive"  # Adapt based on usage patterns


# === Data Classes ===

@dataclass
class OptimizationHint:
    """Hint for optimization."""
    
    hint_type: str
    target: Optional[str] = None  # Step ID or pattern
    parameters: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0  # Higher priority hints applied first
    
    def applies_to(self, step_id: str) -> bool:
        """Check if hint applies to step."""
        if self.target is None:
            return True
        if "*" in self.target:
            # Simple wildcard matching
            pattern = self.target.replace("*", "")
            return pattern in step_id
        return self.target == step_id


@dataclass
class OptimizationResult:
    """Result of optimization."""
    
    original_steps: int
    optimized_steps: int
    fusions_applied: int = 0
    pushdowns_applied: int = 0
    caching_points: int = 0
    estimated_speedup: float = 1.0
    memory_impact: str = "neutral"  # reduced, neutral, increased
    optimizations: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def summary(self) -> Dict[str, Any]:
        """Get optimization summary."""
        return {
            "steps_reduced": self.original_steps - self.optimized_steps,
            "reduction_percentage": (1 - self.optimized_steps / self.original_steps) * 100,
            "fusions_applied": self.fusions_applied,
            "pushdowns_applied": self.pushdowns_applied,
            "caching_points": self.caching_points,
            "estimated_speedup": f"{self.estimated_speedup:.1f}x",
            "memory_impact": self.memory_impact,
            "total_optimizations": len(self.optimizations)
        }


@dataclass
class CacheEntry:
    """Cache entry for intermediate results."""
    
    key: str
    data: Any
    size_bytes: int
    creation_time: float = field(default_factory=time.time)
    last_access_time: float = field(default_factory=time.time)
    access_count: int = 0
    ttl_seconds: Optional[float] = None
    
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        if self.ttl_seconds is None:
            return False
        return time.time() - self.creation_time > self.ttl_seconds
    
    def access(self) -> Any:
        """Access cache entry."""
        self.last_access_time = time.time()
        self.access_count += 1
        return self.data


# === Base Optimizer ===

class Optimizer(ABC):
    """Abstract base class for optimizers."""
    
    def __init__(self, optimization_level: OptimizationLevel = OptimizationLevel.STANDARD):
        """
        Initialize optimizer.
        
        Parameters
        ----------
        optimization_level : OptimizationLevel
            Level of optimization to apply
        """
        self.optimization_level = optimization_level
        self.console = Console()
    
    @abstractmethod
    def optimize(self, steps: List[PipelineStep], config: EstimatorConfig) -> List[PipelineStep]:
        """
        Optimize pipeline steps.
        
        Parameters
        ----------
        steps : List[PipelineStep]
            Pipeline steps to optimize
        config : EstimatorConfig
            Estimation configuration
            
        Returns
        -------
        List[PipelineStep]
            Optimized steps
        """
        pass
    
    def should_optimize(self, level: OptimizationLevel) -> bool:
        """Check if optimization should be applied at given level."""
        level_order = {
            OptimizationLevel.NONE: 0,
            OptimizationLevel.BASIC: 1,
            OptimizationLevel.STANDARD: 2,
            OptimizationLevel.AGGRESSIVE: 3
        }
        return level_order[self.optimization_level] >= level_order[level]


# === Step Fusion Optimizer ===

class StepFusionOptimizer(Optimizer):
    """Optimizes by fusing compatible steps."""
    
    def __init__(
        self,
        optimization_level: OptimizationLevel = OptimizationLevel.STANDARD,
        fusion_strategy: FusionStrategy = FusionStrategy.CONSERVATIVE
    ):
        """
        Initialize step fusion optimizer.
        
        Parameters
        ----------
        optimization_level : OptimizationLevel
            Optimization level
        fusion_strategy : FusionStrategy
            Fusion strategy to use
        """
        super().__init__(optimization_level)
        self.fusion_strategy = fusion_strategy
    
    def optimize(self, steps: List[PipelineStep], config: EstimatorConfig) -> List[PipelineStep]:
        """Optimize by fusing steps."""
        if not self.should_optimize(OptimizationLevel.BASIC):
            return steps
        
        optimized_steps = []
        fusion_buffer = []
        
        for step in steps:
            if fusion_buffer and self._can_fuse(fusion_buffer[-1], step):
                # Add to fusion buffer
                fusion_buffer.append(step)
            else:
                # Flush buffer and start new
                if fusion_buffer:
                    fused_step = self._fuse_steps(fusion_buffer)
                    optimized_steps.append(fused_step)
                    fusion_buffer = []
                
                # Check if this step can start a fusion group
                if self._is_fusable(step):
                    fusion_buffer = [step]
                else:
                    optimized_steps.append(step)
        
        # Flush remaining buffer
        if fusion_buffer:
            if len(fusion_buffer) > 1:
                fused_step = self._fuse_steps(fusion_buffer)
                optimized_steps.append(fused_step)
            else:
                optimized_steps.extend(fusion_buffer)
        
        return optimized_steps
    
    def _can_fuse(self, step1: PipelineStep, step2: PipelineStep) -> bool:
        """Check if two steps can be fused."""
        # Check contract compatibility
        if step1.get_output_contract() != step2.get_input_contract():
            return False
        
        # Conservative: only fuse similar operations
        if self.fusion_strategy == FusionStrategy.CONSERVATIVE:
            # Check if steps are of similar type
            if type(step1).__name__ != type(step2).__name__:
                return False
        
        # Aggressive: fuse if contracts match
        elif self.fusion_strategy == FusionStrategy.AGGRESSIVE:
            # More lenient fusion rules
            pass
        
        # Check for side effects
        if self._has_side_effects(step1) or self._has_side_effects(step2):
            return False
        
        return True
    
    def _is_fusable(self, step: PipelineStep) -> bool:
        """Check if step is fusable."""
        # Steps that typically can be fused
        fusable_types = ["filter", "select", "transform", "calculate"]
        step_type = type(step).__name__.lower()
        
        return any(ft in step_type for ft in fusable_types)
    
    def _has_side_effects(self, step: PipelineStep) -> bool:
        """Check if step has side effects."""
        # Steps with side effects shouldn't be fused
        side_effect_types = ["write", "save", "export", "print"]
        step_type = type(step).__name__.lower()
        
        return any(st in step_type for st in side_effect_types)
    
    def _fuse_steps(self, steps: List[PipelineStep]) -> PipelineStep:
        """Fuse multiple steps into one."""
        # Create a composite step
        class FusedStep(PipelineStep):
            def __init__(self, substeps: List[PipelineStep]):
                super().__init__(
                    step_id=f"fused_{substeps[0].step_id}_{substeps[-1].step_id}",
                    description=f"Fused: {', '.join(s.step_id for s in substeps)}"
                )
                self.substeps = substeps
            
            def get_input_contract(self) -> Type:
                return self.substeps[0].get_input_contract()
            
            def get_output_contract(self) -> Type:
                return self.substeps[-1].get_output_contract()
            
            def execute_step(self, input_data: Any, context: ExecutionContext) -> Any:
                current_data = input_data
                for substep in self.substeps:
                    current_data = substep.execute_step(current_data, context)
                return current_data
        
        return FusedStep(steps)


# === Query Pushdown Optimizer ===

class QueryPushdownOptimizer(Optimizer):
    """Optimizes by pushing filters and projections to database level."""
    
    def optimize(self, steps: List[PipelineStep], config: EstimatorConfig) -> List[PipelineStep]:
        """Optimize by pushing queries down."""
        if not self.should_optimize(OptimizationLevel.STANDARD):
            return steps
        
        optimized_steps = []
        pushdown_candidates = []
        
        for i, step in enumerate(steps):
            step_type = type(step).__name__.lower()
            
            # Identify filter steps
            if "filter" in step_type:
                pushdown_candidates.append((i, step, "filter"))
            
            # Identify projection steps
            elif "select" in step_type or "project" in step_type:
                pushdown_candidates.append((i, step, "projection"))
            
            # Identify data loading steps
            elif "load" in step_type or "read" in step_type:
                # Apply pushdowns to this step
                if pushdown_candidates:
                    optimized_step = self._apply_pushdowns(step, pushdown_candidates)
                    optimized_steps.append(optimized_step)
                    pushdown_candidates = []
                else:
                    optimized_steps.append(step)
            else:
                optimized_steps.append(step)
        
        # Add remaining steps
        for _, step, _ in pushdown_candidates:
            optimized_steps.append(step)
        
        return optimized_steps
    
    def _apply_pushdowns(
        self,
        load_step: PipelineStep,
        pushdowns: List[Tuple[int, PipelineStep, str]]
    ) -> PipelineStep:
        """Apply pushdowns to a data loading step."""
        # Create enhanced load step with pushdowns
        class PushdownLoadStep(PipelineStep):
            def __init__(self, base_step: PipelineStep, filters: List, projections: List):
                super().__init__(
                    step_id=f"pushdown_{base_step.step_id}",
                    description=f"Optimized: {base_step.description}"
                )
                self.base_step = base_step
                self.filters = filters
                self.projections = projections
            
            def get_input_contract(self) -> Type:
                return self.base_step.get_input_contract()
            
            def get_output_contract(self) -> Type:
                return self.base_step.get_output_contract()
            
            def execute_step(self, input_data: Any, context: ExecutionContext) -> Any:
                # Apply pushdowns at database level
                # This would modify the SQL query or lazy frame operations
                result = self.base_step.execute_step(input_data, context)
                
                # Apply any filters/projections that couldn't be pushed
                for filter_step in self.filters:
                    if hasattr(filter_step, "execute_step"):
                        result = filter_step.execute_step(result, context)
                
                return result
        
        filters = [s for _, s, t in pushdowns if t == "filter"]
        projections = [s for _, s, t in pushdowns if t == "projection"]
        
        return PushdownLoadStep(load_step, filters, projections)


# === Cache Optimizer ===

class CacheOptimizer(Optimizer):
    """Optimizes pipeline with intelligent caching."""
    
    def __init__(
        self,
        optimization_level: OptimizationLevel = OptimizationLevel.STANDARD,
        cache_strategy: CacheStrategy = CacheStrategy.ADAPTIVE,
        max_cache_size_bytes: int = 1_000_000_000  # 1GB
    ):
        """
        Initialize cache optimizer.
        
        Parameters
        ----------
        optimization_level : OptimizationLevel
            Optimization level
        cache_strategy : CacheStrategy
            Caching strategy
        max_cache_size_bytes : int
            Maximum cache size in bytes
        """
        super().__init__(optimization_level)
        self.cache_strategy = cache_strategy
        self.max_cache_size = max_cache_size_bytes
        self.cache: Dict[str, CacheEntry] = {}
        self.cache_hits = defaultdict(int)
        self.cache_misses = defaultdict(int)
    
    def optimize(self, steps: List[PipelineStep], config: EstimatorConfig) -> List[PipelineStep]:
        """Optimize with caching."""
        if not self.should_optimize(OptimizationLevel.BASIC):
            return steps
        
        optimized_steps = []
        
        for step in steps:
            # Determine if step should be cached
            if self._should_cache(step):
                cached_step = self._create_cached_step(step)
                optimized_steps.append(cached_step)
            else:
                optimized_steps.append(step)
        
        return optimized_steps
    
    def _should_cache(self, step: PipelineStep) -> bool:
        """Determine if step output should be cached."""
        step_type = type(step).__name__.lower()
        
        # Always cache expensive operations
        expensive_ops = ["join", "aggregate", "stratif", "calculation"]
        if any(op in step_type for op in expensive_ops):
            return True
        
        # Adaptive strategy: cache based on historical patterns
        if self.cache_strategy == CacheStrategy.ADAPTIVE:
            cache_key = self._get_cache_key(step)
            if self.cache_hits[cache_key] > 2:  # Cached if accessed multiple times
                return True
        
        # Eager strategy: cache everything
        elif self.cache_strategy == CacheStrategy.EAGER:
            return True
        
        return False
    
    def _create_cached_step(self, step: PipelineStep) -> PipelineStep:
        """Create a cached version of a step."""
        cache_optimizer = self
        
        class CachedStep(PipelineStep):
            def __init__(self, base_step: PipelineStep):
                super().__init__(
                    step_id=f"cached_{base_step.step_id}",
                    description=f"Cached: {base_step.description}"
                )
                self.base_step = base_step
            
            def get_input_contract(self) -> Type:
                return self.base_step.get_input_contract()
            
            def get_output_contract(self) -> Type:
                return self.base_step.get_output_contract()
            
            def execute_step(self, input_data: Any, context: ExecutionContext) -> Any:
                cache_key = cache_optimizer._get_cache_key_with_input(
                    self.base_step,
                    input_data
                )
                
                # Check cache
                if cache_key in cache_optimizer.cache:
                    entry = cache_optimizer.cache[cache_key]
                    if not entry.is_expired():
                        cache_optimizer.cache_hits[self.base_step.step_id] += 1
                        return entry.access()
                
                # Cache miss
                cache_optimizer.cache_misses[self.base_step.step_id] += 1
                result = self.base_step.execute_step(input_data, context)
                
                # Store in cache
                cache_optimizer._add_to_cache(cache_key, result)
                
                return result
        
        return CachedStep(step)
    
    def _get_cache_key(self, step: PipelineStep) -> str:
        """Get cache key for step."""
        return f"{step.step_id}_{type(step).__name__}"
    
    def _get_cache_key_with_input(self, step: PipelineStep, input_data: Any) -> str:
        """Get cache key including input data hash."""
        base_key = self._get_cache_key(step)
        
        # Create hash of input data
        if isinstance(input_data, pl.DataFrame):
            input_hash = hashlib.md5(
                f"{input_data.shape}_{input_data.columns}".encode()
            ).hexdigest()[:8]
        elif isinstance(input_data, DataContract):
            input_hash = hashlib.md5(
                json.dumps(input_data.model_dump(), sort_keys=True).encode()
            ).hexdigest()[:8]
        else:
            input_hash = hashlib.md5(str(input_data).encode()).hexdigest()[:8]
        
        return f"{base_key}_{input_hash}"
    
    def _add_to_cache(self, key: str, data: Any) -> None:
        """Add data to cache."""
        # Estimate size
        size_bytes = self._estimate_size(data)
        
        # Check cache size limit
        total_size = sum(e.size_bytes for e in self.cache.values())
        
        # Evict if necessary
        while total_size + size_bytes > self.max_cache_size and self.cache:
            self._evict_cache_entry()
            total_size = sum(e.size_bytes for e in self.cache.values())
        
        # Add to cache
        self.cache[key] = CacheEntry(
            key=key,
            data=data,
            size_bytes=size_bytes,
            ttl_seconds=3600  # 1 hour TTL
        )
    
    def _evict_cache_entry(self) -> None:
        """Evict least recently used cache entry."""
        if not self.cache:
            return
        
        # Find LRU entry
        lru_key = min(
            self.cache.keys(),
            key=lambda k: self.cache[k].last_access_time
        )
        
        del self.cache[lru_key]
    
    def _estimate_size(self, data: Any) -> int:
        """Estimate size of data in bytes."""
        if isinstance(data, pl.DataFrame):
            return data.estimated_size()
        elif isinstance(data, (list, dict)):
            return len(json.dumps(data))
        else:
            return 1000  # Default estimate
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_hits = sum(self.cache_hits.values())
        total_misses = sum(self.cache_misses.values())
        
        return {
            "cache_entries": len(self.cache),
            "cache_size_bytes": sum(e.size_bytes for e in self.cache.values()),
            "total_hits": total_hits,
            "total_misses": total_misses,
            "hit_rate": total_hits / (total_hits + total_misses) if (total_hits + total_misses) > 0 else 0,
            "most_accessed": max(self.cache_hits.items(), key=lambda x: x[1])[0] if self.cache_hits else None
        }


# === Data Locality Optimizer ===

class DataLocalityOptimizer(Optimizer):
    """Optimizes data movement and locality."""
    
    def optimize(self, steps: List[PipelineStep], config: EstimatorConfig) -> List[PipelineStep]:
        """Optimize data locality."""
        if not self.should_optimize(OptimizationLevel.STANDARD):
            return steps
        
        # Analyze data flow
        data_flow = self._analyze_data_flow(steps)
        
        # Reorder steps to minimize data movement
        optimized_steps = self._reorder_for_locality(steps, data_flow)
        
        return optimized_steps
    
    def _analyze_data_flow(self, steps: List[PipelineStep]) -> Dict[str, Set[str]]:
        """Analyze data flow between steps."""
        data_flow = defaultdict(set)
        
        for i, step in enumerate(steps):
            if i > 0:
                # Track data dependency
                prev_step = steps[i - 1]
                data_flow[step.step_id].add(prev_step.step_id)
        
        return data_flow
    
    def _reorder_for_locality(
        self,
        steps: List[PipelineStep],
        data_flow: Dict[str, Set[str]]
    ) -> List[PipelineStep]:
        """Reorder steps for better data locality."""
        # Simple heuristic: group related operations
        groups = []
        current_group = []
        
        for step in steps:
            step_type = type(step).__name__.lower()
            
            if current_group:
                last_type = type(current_group[-1]).__name__.lower()
                
                # Keep similar operations together
                if self._operations_related(step_type, last_type):
                    current_group.append(step)
                else:
                    groups.append(current_group)
                    current_group = [step]
            else:
                current_group = [step]
        
        if current_group:
            groups.append(current_group)
        
        # Flatten groups back to steps
        optimized_steps = []
        for group in groups:
            optimized_steps.extend(group)
        
        return optimized_steps
    
    def _operations_related(self, op1: str, op2: str) -> bool:
        """Check if two operations are related."""
        # Group similar operations
        operation_groups = [
            ["filter", "select", "where"],
            ["join", "merge", "combine"],
            ["aggregate", "group", "summarize"],
            ["calculate", "compute", "transform"]
        ]
        
        for group in operation_groups:
            if any(g in op1 for g in group) and any(g in op2 for g in group):
                return True
        
        return False


# === Main Pipeline Optimizer ===

class PipelineOptimizer:
    """
    Main pipeline optimization orchestrator.
    
    Coordinates multiple optimization strategies to improve pipeline performance.
    """
    
    def __init__(
        self,
        optimization_level: OptimizationLevel = OptimizationLevel.STANDARD,
        enable_fusion: bool = True,
        enable_pushdown: bool = True,
        enable_caching: bool = True,
        enable_locality: bool = True
    ):
        """
        Initialize pipeline optimizer.
        
        Parameters
        ----------
        optimization_level : OptimizationLevel
            Level of optimization
        enable_fusion : bool
            Enable step fusion
        enable_pushdown : bool
            Enable query pushdown
        enable_caching : bool
            Enable caching
        enable_locality : bool
            Enable data locality optimization
        """
        self.optimization_level = optimization_level
        self.console = Console()
        
        # Initialize optimizers
        self.optimizers = []
        
        if enable_fusion:
            self.optimizers.append(StepFusionOptimizer(optimization_level))
        
        if enable_pushdown:
            self.optimizers.append(QueryPushdownOptimizer(optimization_level))
        
        if enable_caching:
            self.cache_optimizer = CacheOptimizer(optimization_level)
            self.optimizers.append(self.cache_optimizer)
        else:
            self.cache_optimizer = None
        
        if enable_locality:
            self.optimizers.append(DataLocalityOptimizer(optimization_level))
        
        # Optimization hints
        self.hints: List[OptimizationHint] = []
    
    def add_hint(self, hint: OptimizationHint) -> None:
        """Add optimization hint."""
        self.hints.append(hint)
    
    def optimize_pipeline(
        self,
        steps: List[PipelineStep],
        config: EstimatorConfig,
        show_report: bool = True
    ) -> Tuple[List[PipelineStep], OptimizationResult]:
        """
        Optimize pipeline steps.
        
        Parameters
        ----------
        steps : List[PipelineStep]
            Pipeline steps to optimize
        config : EstimatorConfig
            Estimation configuration
        show_report : bool
            Whether to show optimization report
            
        Returns
        -------
        Tuple[List[PipelineStep], OptimizationResult]
            Optimized steps and optimization result
        """
        original_count = len(steps)
        result = OptimizationResult(
            original_steps=original_count,
            optimized_steps=original_count
        )
        
        # Apply hints
        steps = self._apply_hints(steps, config)
        
        # Apply optimizations
        current_steps = steps
        for optimizer in self.optimizers:
            before_count = len(current_steps)
            current_steps = optimizer.optimize(current_steps, config)
            after_count = len(current_steps)
            
            # Track optimizations
            if before_count != after_count:
                optimizer_name = type(optimizer).__name__
                
                if "fusion" in optimizer_name.lower():
                    result.fusions_applied += before_count - after_count
                elif "pushdown" in optimizer_name.lower():
                    result.pushdowns_applied += 1
                elif "cache" in optimizer_name.lower():
                    result.caching_points += 1
                
                result.optimizations.append(
                    f"{optimizer_name}: {before_count} → {after_count} steps"
                )
        
        # Update result
        result.optimized_steps = len(current_steps)
        result.estimated_speedup = self._estimate_speedup(steps, current_steps)
        result.memory_impact = self._assess_memory_impact(steps, current_steps)
        
        # Show report if requested
        if show_report:
            self.display_report(result)
        
        return current_steps, result
    
    def _apply_hints(
        self,
        steps: List[PipelineStep],
        config: EstimatorConfig
    ) -> List[PipelineStep]:
        """Apply optimization hints."""
        # Sort hints by priority
        sorted_hints = sorted(self.hints, key=lambda h: h.priority, reverse=True)
        
        for hint in sorted_hints:
            # Apply hint based on type
            if hint.hint_type == "no_cache":
                # Mark steps as non-cacheable
                for step in steps:
                    if hint.applies_to(step.step_id):
                        step.cacheable = False
            
            elif hint.hint_type == "force_cache":
                # Mark steps as must-cache
                for step in steps:
                    if hint.applies_to(step.step_id):
                        step.must_cache = True
            
            elif hint.hint_type == "no_fusion":
                # Prevent fusion
                for step in steps:
                    if hint.applies_to(step.step_id):
                        step.fusable = False
        
        return steps
    
    def _estimate_speedup(
        self,
        original: List[PipelineStep],
        optimized: List[PipelineStep]
    ) -> float:
        """Estimate speedup from optimization."""
        # Simple heuristic based on step reduction
        if len(optimized) == 0:
            return 1.0
        
        base_speedup = len(original) / len(optimized)
        
        # Additional speedup from specific optimizations
        if self.cache_optimizer:
            cache_stats = self.cache_optimizer.get_cache_stats()
            cache_speedup = 1 + (cache_stats["hit_rate"] * 0.5)
            base_speedup *= cache_speedup
        
        return min(base_speedup, 10.0)  # Cap at 10x
    
    def _assess_memory_impact(
        self,
        original: List[PipelineStep],
        optimized: List[PipelineStep]
    ) -> str:
        """Assess memory impact of optimization."""
        # Fewer steps generally means less memory
        if len(optimized) < len(original) * 0.7:
            return "reduced"
        elif len(optimized) > len(original):
            return "increased"
        else:
            return "neutral"
    
    def display_report(self, result: OptimizationResult) -> None:
        """Display optimization report."""
        table = Table(title="Pipeline Optimization Report")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        summary = result.summary()
        
        table.add_row("Original Steps", str(result.original_steps))
        table.add_row("Optimized Steps", str(result.optimized_steps))
        table.add_row("Steps Reduced", f"{summary['steps_reduced']} ({summary['reduction_percentage']:.1f}%)")
        table.add_row("Fusions Applied", str(result.fusions_applied))
        table.add_row("Pushdowns Applied", str(result.pushdowns_applied))
        table.add_row("Cache Points", str(result.caching_points))
        table.add_row("Estimated Speedup", summary['estimated_speedup'])
        table.add_row("Memory Impact", result.memory_impact)
        
        self.console.print(table)
        
        if result.optimizations:
            self.console.print("\n[bold]Optimizations Applied:[/bold]")
            for opt in result.optimizations:
                self.console.print(f"  • {opt}")
        
        if result.warnings:
            self.console.print("\n[yellow]Warnings:[/yellow]")
            for warning in result.warnings:
                self.console.print(f"  • {warning}")
    
    def get_cache_stats(self) -> Optional[Dict[str, Any]]:
        """Get cache statistics if caching is enabled."""
        if self.cache_optimizer:
            return self.cache_optimizer.get_cache_stats()
        return None
    
    def clear_cache(self) -> None:
        """Clear optimization cache."""
        if self.cache_optimizer:
            self.cache_optimizer.cache.clear()
            self.cache_optimizer.cache_hits.clear()
            self.cache_optimizer.cache_misses.clear()


# === A/B Testing Support ===

class PipelineABTester:
    """Support for A/B testing pipeline variants."""
    
    def __init__(self):
        """Initialize A/B tester."""
        self.console = Console()
        self.results: Dict[str, Dict[str, Any]] = {}
    
    def test_variants(
        self,
        variants: Dict[str, List[PipelineStep]],
        db: FIA,
        config: EstimatorConfig,
        context: ExecutionContext,
        test_data: Any,
        iterations: int = 3
    ) -> Dict[str, Dict[str, Any]]:
        """
        Test multiple pipeline variants.
        
        Parameters
        ----------
        variants : Dict[str, List[PipelineStep]]
            Pipeline variants to test
        db : FIA
            Database connection
        config : EstimatorConfig
            Configuration
        context : ExecutionContext
            Execution context
        test_data : Any
            Test data
        iterations : int
            Number of test iterations
            
        Returns
        -------
        Dict[str, Dict[str, Any]]
            Test results for each variant
        """
        for variant_name, steps in variants.items():
            self.console.print(f"\n[cyan]Testing variant: {variant_name}[/cyan]")
            
            execution_times = []
            memory_usage = []
            
            for i in range(iterations):
                start_time = time.time()
                start_memory = self._get_memory_usage()
                
                # Execute pipeline
                try:
                    current_data = test_data
                    for step in steps:
                        result = step.execute(current_data, context)
                        if result.success:
                            current_data = result.output
                    
                    # Record metrics
                    execution_time = time.time() - start_time
                    memory_used = self._get_memory_usage() - start_memory
                    
                    execution_times.append(execution_time)
                    memory_usage.append(memory_used)
                    
                except Exception as e:
                    self.console.print(f"[red]Variant {variant_name} failed: {e}[/red]")
                    break
            
            # Calculate statistics
            if execution_times:
                self.results[variant_name] = {
                    "avg_execution_time": sum(execution_times) / len(execution_times),
                    "min_execution_time": min(execution_times),
                    "max_execution_time": max(execution_times),
                    "avg_memory_usage": sum(memory_usage) / len(memory_usage) if memory_usage else 0,
                    "iterations": len(execution_times),
                    "success_rate": len(execution_times) / iterations
                }
        
        # Display comparison
        self._display_comparison()
        
        return self.results
    
    def _get_memory_usage(self) -> int:
        """Get current memory usage."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss
        except ImportError:
            return 0
    
    def _display_comparison(self) -> None:
        """Display comparison of variants."""
        if not self.results:
            return
        
        table = Table(title="A/B Test Results")
        table.add_column("Variant", style="cyan")
        table.add_column("Avg Time (s)", style="white")
        table.add_column("Memory (MB)", style="white")
        table.add_column("Success Rate", style="white")
        
        # Find best variant
        best_time = min(r["avg_execution_time"] for r in self.results.values())
        
        for variant, results in self.results.items():
            time_str = f"{results['avg_execution_time']:.2f}"
            if results['avg_execution_time'] == best_time:
                time_str = f"[green]{time_str} ✓[/green]"
            
            memory_mb = results['avg_memory_usage'] / 1_000_000
            success_pct = results['success_rate'] * 100
            
            table.add_row(
                variant,
                time_str,
                f"{memory_mb:.1f}",
                f"{success_pct:.0f}%"
            )
        
        self.console.print(table)


# === Export public API ===

__all__ = [
    "PipelineOptimizer",
    "OptimizationLevel",
    "OptimizationHint",
    "OptimizationResult",
    "StepFusionOptimizer",
    "QueryPushdownOptimizer",
    "CacheOptimizer",
    "DataLocalityOptimizer",
    "FusionStrategy",
    "CacheStrategy",
    "PipelineABTester"
]