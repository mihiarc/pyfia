"""
Pydantic models for pyFIA data validation.
"""

from typing import Any, Dict, List, Optional, Union

import polars as pl
from pydantic import BaseModel, Field, field_validator


class EvaluationInfo(BaseModel):
    """Model for FIA evaluation information."""
    evalid: int
    statecd: int
    eval_typ: str = 'VOL'
    start_invyr: int
    end_invyr: int
    nplots: Optional[int] = None

    @field_validator('eval_typ')
    @classmethod
    def validate_eval_type(cls, v: str) -> str:
        valid_types = ['VOL', 'GRM', 'CHNG', 'DWM', 'INVASIVE']
        if v not in valid_types:
            raise ValueError(f'eval_typ must be one of {valid_types}')
        return v


class DatabaseSummary(BaseModel):
    """Model for database summary information."""
    database_name: str
    total_evaluations: int
    states: List[int]
    year_range: Optional[tuple[int, int]] = None
    evalids: List[int]


class EstimationResult(BaseModel):
    """Model for estimation results."""
    estimate: float
    variance: float
    se: float
    cv: float
    total: float
    area: float
    nPlots: Optional[int] = None

    @field_validator('cv')
    @classmethod
    def validate_cv(cls, v: float) -> float:
        if v < 0:
            raise ValueError('CV cannot be negative')
        return v


class QueryParameters(BaseModel):
    """Model for query parameters."""
    by_species: bool = False
    by_size_class: bool = False
    land_type: str = Field(default='forest', pattern='^(forest|timber|all)$')
    tree_domain: Optional[str] = None
    area_domain: Optional[str] = None
    evalid: Optional[Union[int, List[int]]] = None
    most_recent: bool = False

    @field_validator('evalid')
    @classmethod
    def validate_evalid(cls, v: Optional[Union[int, List[int]]]) -> Optional[List[int]]:
        if v is None:
            return None
        if isinstance(v, int):
            return [v]
        return v


class CLICommand(BaseModel):
    """Model for CLI command parsing."""
    command: str
    args: List[str] = []
    kwargs: Dict[str, Any] = {}

    @field_validator('command')
    @classmethod
    def validate_command(cls, v: str) -> str:
        valid_commands = [
            'connect', 'info', 'evalid', 'clip', 'tpa', 'biomass',
            'volume', 'mortality', 'show', 'export', 'help', 'exit',
            'recent', 'shortcut', 'setdefault', 'clear'
        ]
        if v not in valid_commands:
            raise ValueError(f'Unknown command: {v}')
        return v


class FIADataFrameWrapper(BaseModel):
    """Wrapper for polars DataFrames with validation."""

    class Config:
        arbitrary_types_allowed = True

    data: pl.DataFrame
    table_name: str
    row_count: int = 0
    column_names: List[str] = []

    @field_validator('data')
    @classmethod
    def validate_dataframe(cls, v: pl.DataFrame) -> pl.DataFrame:
        if not isinstance(v, pl.DataFrame):
            raise ValueError('data must be a polars DataFrame')
        return v

    def model_post_init(self, __context) -> None:
        """Post-init to set derived fields."""
        self.row_count = len(self.data)
        self.column_names = self.data.columns
