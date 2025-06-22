"""
FIA AI Agent with Cognee Memory Integration.

This module provides a clean integration of Cognee's memory system with
the FIA agent for enhanced context and learning capabilities.
"""

import os
import asyncio
from typing import Optional, Dict, Any, List, Union
from pathlib import Path
from datetime import datetime

# Import setup FIRST to configure environment
from . import cognee_setup

# Now import cognee after environment is configured
import cognee

# Configure Cognee with our directories
cognee_setup.configure_cognee()
from langchain_openai import ChatOpenAI

from .duckdb_query_interface import DuckDBQueryInterface
from .core import FIA


class CogneeFIAAgent:
    """FIA Agent enhanced with Cognee's memory capabilities."""
    
    def __init__(self, db_path: Union[str, Path], api_key: Optional[str] = None):
        """Initialize the agent."""
        self.db_path = Path(db_path)
        self.query_interface = DuckDBQueryInterface(db_path)
        self.fia = FIA(db_path, engine="duckdb")
        
        # Set up API key
        self.api_key = api_key or os.environ.get('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key required")
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            api_key=self.api_key,
            model="gpt-4o-mini",
            temperature=0.1
        )
        
        self._initialized = False
    
    async def initialize(self):
        """Initialize Cognee with pyFIA knowledge."""
        if self._initialized:
            return
        
        print("Initializing Cognee memory...")
        
        try:
            # Clear existing data
            await cognee.prune.prune_data()
            await cognee.prune.prune_system()
        except Exception as e:
            print(f"Note: Could not clear existing data: {e}")
        
        # Load pyFIA knowledge
        await self._load_core_knowledge()
        
        # Cognify
        print("Processing knowledge with Cognee...")
        try:
            await cognee.cognify()
        except Exception as e:
            print(f"Warning: Cognify encountered an issue: {e}")
            print("Continuing without full cognification...")
        
        self._initialized = True
        print("Cognee memory initialized!")
    
    async def _load_core_knowledge(self):
        """Load core pyFIA and FIA knowledge."""
        print("Loading FIA knowledge into memory...")
        
        knowledge_items = [
            # FIA Core Concepts
            """
            FIA Core Concepts:
            - EVALID: 6-digit identifier for statistically valid plot groupings
            - Post-stratified estimation: Statistical method using stratification
            - Expansion factors: EXPNS in POP_STRATUM table for area calculations
            - Tree basis: MICR (microplot), SUBP (subplot), MACR (macroplot)
            - Adjustment factors: Applied based on tree basis for proper expansion
            """,
            
            # pyFIA Usage
            """
            pyFIA Usage Patterns:
            from pyfia import FIA
            
            # Initialize and filter by EVALID
            fia = FIA('database.db')
            fia_filtered = fia.clip_by_evalid(372301)
            
            # Estimation functions
            tpa_results = fia_filtered.tpa(bySpecies=True)
            biomass_results = fia_filtered.biomass(component='AG')
            volume_results = fia_filtered.volume(volType='NET')
            mortality_results = fia_filtered.mortality()
            area_results = fia_filtered.area()
            """,
            
            # Database Structure
            """
            FIA Database Key Tables:
            - PLOT: Plot-level data with location and measurement info
            - TREE: Individual tree measurements with species and size
            - COND: Forest condition data 
            - POP_EVAL: Evaluation definitions with EVALID
            - POP_STRATUM: Stratification info and expansion factors
            - POP_PLOT_STRATUM_ASSGN: Links plots to evaluations
            - REF_SPECIES: Species codes and names
            """,
            
            # Statistical Methods
            """
            FIA Statistical Methods:
            1. Tree Level: Apply TPA_UNADJ * adjustment factor
            2. Plot Level: Sum tree values within plots
            3. Stratum Level: Calculate means and variances
            4. Population Level: Weight by stratum areas
            5. Ratio Estimation: Total / Area for per-acre values
            
            Variance uses delta method for ratio estimates
            """,
            
            # Common Queries
            """
            Common FIA SQL Patterns:
            
            -- Trees per acre by species
            SELECT t.SPCD, SUM(t.TPA_UNADJ * ps.ADJ_FACTOR_SUBP) as tpa
            FROM TREE t
            JOIN PLOT p ON t.PLT_CN = p.CN
            JOIN POP_PLOT_STRATUM_ASSGN ppsa ON p.CN = ppsa.PLT_CN
            JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
            WHERE ppsa.EVALID = ? AND t.STATUSCD = 1
            GROUP BY t.SPCD
            
            -- Forest area
            SELECT SUM(c.CONDPROP_UNADJ * ps.EXPNS) as forest_acres
            FROM COND c
            JOIN PLOT p ON c.PLT_CN = p.CN
            JOIN POP_PLOT_STRATUM_ASSGN ppsa ON p.CN = ppsa.PLT_CN
            JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
            WHERE ppsa.EVALID = ? AND c.COND_STATUS_CD = 1
            """
        ]
        
        # Add each knowledge item to Cognee
        for i, item in enumerate(knowledge_items, 1):
            try:
                await cognee.add(item.strip())
                print(f"  Loaded knowledge item {i}/{len(knowledge_items)}")
            except Exception as e:
                print(f"  Warning: Failed to load knowledge item {i}: {e}")
        
        print(f"Knowledge loading complete!")
    
    async def query(self, user_query: str) -> str:
        """Process a user query with Cognee memory enhancement."""
        # Ensure initialized
        await self.initialize()
        
        # Search Cognee memory
        print("Searching memory...")
        try:
            memory_results = await cognee.search(user_query)
        except Exception as e:
            print(f"Warning: Memory search failed: {e}")
            memory_results = []
        
        # Build context from memory
        memory_context = "Relevant Knowledge from Memory:\n"
        if memory_results:
            for i, result in enumerate(memory_results[:3], 1):
                memory_context += f"{i}. {result}\n"
        else:
            # Fallback to hardcoded knowledge if memory fails
            memory_context = self._get_fallback_context(user_query)
        
        # Check if this is a data query or knowledge query
        is_data_query = any(word in user_query.lower() for word in 
                           ['how many', 'what is the', 'show me', 'calculate', 'total'])
        
        if is_data_query:
            # Generate SQL query
            sql_prompt = f"""
            Based on this context from memory:
            {memory_context}
            
            Generate a SQL query for: {user_query}
            
            Important:
            - Use proper FIA table relationships
            - Include EVALID filtering if needed
            - Add LIMIT clause for safety
            
            Return only the SQL query, nothing else.
            """
            
            sql_response = self.llm.invoke(sql_prompt)
            sql_query = sql_response.content.strip()
            
            # Clean up SQL query - remove markdown code blocks if present
            if "```sql" in sql_query:
                sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
            elif "```" in sql_query:
                sql_query = sql_query.split("```")[1].split("```")[0].strip()
            
            # Execute query
            try:
                result = self.query_interface.execute_query(sql_query, limit=100)
                formatted_result = self.query_interface.format_results_for_llm(result)
                
                # Generate final response
                response = f"""
Based on the FIA database query:

**Query:** {user_query}

**SQL Generated:**
```sql
{sql_query}
```

**Results:**
{formatted_result}

*Note: This query used EVALID-filtered data for statistical validity.*
                """
            except Exception as e:
                response = f"Error executing query: {str(e)}\n\nSQL attempted:\n```sql\n{sql_query}\n```"
        
        else:
            # Knowledge/explanation query
            explanation_prompt = f"""
            Using this context from the FIA knowledge base:
            {memory_context}
            
            Answer this question: {user_query}
            
            Provide a clear, helpful explanation. If relevant, include:
            - pyFIA code examples
            - Statistical concepts
            - Best practices
            """
            
            explanation_response = self.llm.invoke(explanation_prompt)
            response = explanation_response.content
        
        # Store this interaction in memory for future learning
        try:
            interaction = f"Q: {user_query}\nA: {response[:500]}..."
            await cognee.add(interaction)
        except Exception as e:
            print(f"Warning: Could not store interaction: {e}")
        
        return response
    
    def _get_fallback_context(self, query: str) -> str:
        """Provide fallback context when memory search fails."""
        query_lower = query.lower()
        
        if "evalid" in query_lower:
            return """Relevant Knowledge:
1. EVALID is a 6-digit identifier (2-state, 2-year, 2-type) that defines statistically valid plot groupings
2. Each EVALID represents a specific evaluation with temporal boundaries and plot assignments
3. Must use EVALID filtering to ensure proper statistical estimation"""
        
        elif "biomass" in query_lower:
            return """Relevant Knowledge:
1. pyFIA biomass calculation: fia.biomass(component='AG'|'BG'|'TOTAL', bySpecies=True)
2. Components: AG (aboveground), BG (belowground), TOTAL (AG+BG)
3. Uses Jenkins equations for biomass estimation from DBH"""
        
        elif "tree" in query_lower and "basis" in query_lower:
            return """Relevant Knowledge:
1. MICR: Microplot (6.8 ft radius) for trees 1.0-4.9" DBH
2. SUBP: Subplot (24.0 ft radius) for trees 5.0"+ DBH
3. MACR: Macroplot (58.9 ft radius) for large trees above MACRO_BREAKPOINT_DIA"""
        
        elif "table" in query_lower:
            return """Relevant Knowledge:
1. TREE table: Individual tree measurements (PLT_CN, SPCD, DIA, HT, TPA_UNADJ)
2. PLOT table: Plot-level data (CN, LAT, LON, INVYR)
3. COND table: Forest condition data (PLT_CN, CONDID, COND_STATUS_CD)
4. POP_STRATUM: Stratification and expansion factors (EXPNS)"""
        
        else:
            return """Relevant Knowledge:
1. pyFIA is a Python implementation of rFIA for Forest Inventory Analysis
2. Use clip_by_evalid() to filter data for valid statistical estimates
3. Main estimation functions: tpa(), biomass(), volume(), mortality(), area()"""
    
    def query_sync(self, user_query: str) -> str:
        """Synchronous wrapper for query method."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.query(user_query))
        finally:
            loop.close()


async def test_agent():
    """Test the Cognee FIA agent."""
    agent = CogneeFIAAgent("fia.duckdb")
    
    test_queries = [
        "What is EVALID and why is it important?",
        "How do I use pyFIA to calculate biomass?",
        "What tables contain tree data?"
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print(f"{'='*60}")
        
        response = await agent.query(query)
        print(response)


if __name__ == "__main__":
    # Run test
    asyncio.run(test_agent())