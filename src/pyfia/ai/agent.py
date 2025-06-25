"""
AI Agent for Forest Inventory Analysis using 2025 LangChain patterns.

This module implements a streamlined FIA agent using LangGraph's create_react_agent
pattern with built-in memory, tool calling, and human-in-the-loop capabilities.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime

# LangChain imports
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
# Removed unused import

# Local imports
from ..database.query_interface import DuckDBQueryInterface
from ..core import FIA

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class FIAAgent:
    """
    AI Agent for Forest Inventory Analysis using LangGraph's create_react_agent pattern.
    
    This implementation follows 2025 best practices:
    - Simple tool functions with clear docstrings
    - Built-in memory and persistence
    - Human-in-the-loop capabilities
    - Streamlined architecture
    """
    
    def __init__(
        self,
        db_path: Union[str, Path],
        api_key: Optional[str] = None,
        model_name: str = "gpt-4-turbo-preview",
        temperature: float = 0,
        verbose: bool = False,
        enable_human_approval: bool = False,
        checkpoint_dir: Optional[str] = None,
    ):
        """
        Initialize the modern FIA agent.
        
        Args:
            db_path: Path to FIA DuckDB database
            api_key: OpenAI API key (uses env var if not provided)
            model_name: LLM model to use
            temperature: LLM temperature
            verbose: Enable debug output
            enable_human_approval: Require human approval for tool calls
            checkpoint_dir: Directory for conversation persistence
        """
        self.db_path = Path(db_path)
        self.verbose = verbose
        self.enable_human_approval = enable_human_approval
        
        # Initialize database interfaces
        self.query_interface = DuckDBQueryInterface(db_path)
        self.fia = FIA(str(db_path))
        
        # Initialize LLM
        api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key required")
            
        self.llm = ChatOpenAI(
            model=model_name,
            temperature=temperature,
            api_key=api_key,
        )
        
        # Create checkpointer for memory
        # Note: Using MemorySaver for now, can upgrade to persistent storage later
        self.checkpointer = MemorySaver()
        
        # Create the agent
        self.agent = self._create_agent()
        
    def _create_agent(self):
        """Create the ReAct agent with tools."""
        
        # Define tools as simple functions
        def execute_fia_query(query: str, limit: int = 1000) -> str:
            """
            Execute a SQL query against the FIA database.
            
            Args:
                query: SQL query to execute
                limit: Maximum number of results to return
                
            Returns:
                Formatted query results or error message
            """
            if self.verbose:
                print(f"[DEBUG] Executing query: {query[:100]}...")
                
            try:
                # Clean query if it has markdown formatting
                query = query.strip()
                if query.startswith('```sql'):
                    query = query[6:].strip()
                elif query.startswith('```'):
                    query = query[3:].strip()
                if query.endswith('```'):
                    query = query[:-3].strip()
                    
                result = self.query_interface.execute_query(query, limit=limit)
                
                if self.verbose:
                    print(f"[DEBUG] Query returned {len(result)} rows")
                    
                return self.query_interface.format_results_for_llm(result)
            except Exception as e:
                return f"Query execution failed: {str(e)}"
        
        def get_database_schema(table_name: Optional[str] = None) -> str:
            """
            Get database schema information.
            
            Args:
                table_name: Specific table name, or None for all tables
                
            Returns:
                Schema information as formatted text
            """
            try:
                if table_name:
                    return self.query_interface.get_table_summary(table_name)
                else:
                    return self.query_interface.get_natural_language_context()
            except Exception as e:
                return f"Error getting schema: {str(e)}"
        
        def find_species_codes(species_name: str) -> str:
            """
            Find species codes by common or scientific name.
            
            Args:
                species_name: Common or scientific name to search
                
            Returns:
                Matching species with codes
            """
            try:
                species_clean = species_name.replace("'", "''")
                query = f"""
                SELECT SPCD, COMMON_NAME, SCIENTIFIC_NAME
                FROM REF_SPECIES
                WHERE LOWER(COMMON_NAME) LIKE LOWER('%{species_clean}%')
                   OR LOWER(SCIENTIFIC_NAME) LIKE LOWER('%{species_clean}%')
                ORDER BY 
                    CASE 
                        WHEN LOWER(COMMON_NAME) = LOWER('{species_clean}') THEN 1
                        WHEN LOWER(COMMON_NAME) LIKE LOWER('{species_clean}%') THEN 2
                        ELSE 3
                    END,
                    COMMON_NAME
                LIMIT 20
                """
                result = self.query_interface.execute_query(query)
                
                if len(result) == 0:
                    return f"No species found matching '{species_name}'"
                
                formatted = f"Species matches for '{species_name}':\n"
                for row in result.head(10).iter_rows(named=True):
                    formatted += f"- Code {row['SPCD']}: {row['COMMON_NAME']} ({row['SCIENTIFIC_NAME']})\n"
                
                if len(result) > 10:
                    formatted += f"\n(Showing first 10 of {len(result)} matches)"
                    
                return formatted
            except Exception as e:
                return f"Error finding species: {str(e)}"
        
        def get_evalid_info(state_code: Optional[int] = None, eval_type: Optional[str] = None) -> str:
            """
            Get FIA evaluation information with intelligent prioritization.
            
            Args:
                state_code: Optional state FIPS code to filter by
                eval_type: Optional evaluation type (EXPVOL, EXPCURR, etc.)
                
            Returns:
                Evaluation information with IDs and descriptions, prioritizing statewide over regional evaluations
            """
            try:
                from ..filters.evalid import get_evalid_info as get_evalid_info_impl
                return get_evalid_info_impl(self.query_interface, state_code, eval_type)
            except Exception as e:
                return f"Error getting EVALID info: {str(e)}"
        
        def get_state_codes() -> str:
            """
            Get list of state codes and names in the database.
            
            Returns:
                List of states with their FIPS codes
            """
            try:
                query = """
                SELECT DISTINCT 
                    VALUE AS STATECD,
                    MEANING AS STATE_NAME
                FROM REF_STATECD
                ORDER BY STATE_NAME
                """
                result = self.query_interface.execute_query(query)
                
                formatted = "State Codes in Database:\n"
                for row in result.iter_rows(named=True):
                    formatted += f"- {row['STATE_NAME']}: {row['STATECD']}\n"
                
                return formatted
            except Exception as e:
                return f"Error getting state codes: {str(e)}"
        
        def get_recommended_evalid(state_code: int, analysis_type: str = "tree_count") -> str:
            """
            Get recommended EVALID for a specific state and analysis type.
            
            Args:
                state_code: FIPS state code (e.g., 48 for Texas)
                analysis_type: Type of analysis (tree_count, volume, biomass, etc.)
                
            Returns:
                Recommended EVALID with explanation
            """
            try:
                from ..filters.evalid import get_recommended_evalid as get_rec_evalid
                evalid, explanation = get_rec_evalid(self.query_interface, state_code, analysis_type)
                
                if evalid:
                    return f"Recommended EVALID: {evalid}\n\n{explanation}"
                else:
                    return explanation
            except Exception as e:
                return f"Error getting recommended EVALID: {str(e)}"

        def execute_tree_command(command_args: str) -> str:
            """
            Execute tree count commands using the CLI interface.
            
            Args:
                command_args: CLI-style arguments (e.g., "bySpecies treeType=live")
                
            Returns:
                Formatted tree count results
            """
            try:
                # Parse CLI arguments into kwargs
                kwargs = {}
                
                # Handle boolean flags
                if "bySpecies" in command_args:
                    kwargs["by_species"] = True
                if "bySizeClass" in command_args:
                    kwargs["by_size_class"] = True
                
                # Parse key=value pairs
                import shlex
                try:
                    parts = shlex.split(command_args)
                except ValueError:
                    parts = command_args.split()
                
                for part in parts:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        # Convert camelCase to snake_case for consistency
                        if key == "treeDomain":
                            key = "tree_domain"
                        elif key == "areaDomain":
                            key = "area_domain"
                        elif key == "treeType":
                            key = "tree_type"
                        elif key == "landType":
                            key = "land_type"
                        elif key == "grpBy":
                            key = "grp_by"
                        
                        # Handle quoted strings
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        else:
                            # Try to convert to appropriate type
                            try:
                                value = int(value)
                            except ValueError:
                                try:
                                    value = float(value)
                                except ValueError:
                                    pass  # Keep as string
                        kwargs[key] = value
                
                # Always include totals for population estimates
                kwargs["totals"] = True
                
                # Execute tree count through the estimation module
                from ..estimation.tree import tree_count
                result = tree_count(self.fia, **kwargs)
                
                if len(result) == 0:
                    return "No trees found matching the specified criteria."
                
                # Format results for LLM consumption
                formatted = "Tree Count Results:\n\n"
                
                for row in result.iter_rows(named=True):
                    if 'COMMON_NAME' in row and row['COMMON_NAME']:
                        formatted += f"Species: {row['COMMON_NAME']}"
                        if 'SCIENTIFIC_NAME' in row and row['SCIENTIFIC_NAME']:
                            formatted += f" ({row['SCIENTIFIC_NAME']})"
                        formatted += "\n"
                    
                    if 'SIZE_CLASS' in row and row['SIZE_CLASS']:
                        formatted += f"Size Class: {row['SIZE_CLASS']}\n"
                    
                    if 'TREE_COUNT' in row:
                        formatted += f"Total Population: {row['TREE_COUNT']:,.0f} trees\n"
                    
                    if 'SE' in row:
                        formatted += f"Standard Error: {row['SE']:,.0f}\n"
                        
                    if 'SE_PERCENT' in row:
                        formatted += f"Standard Error %: {row['SE_PERCENT']:.1f}%\n"
                    
                    formatted += "\n"
                
                formatted += "(Statistically valid population estimate using FIA methodology)\n"
                
                return formatted
                
            except Exception as e:
                return f"Error executing tree command: {str(e)}"
        
        # Create system prompt
        system_prompt = """You are an expert Forest Inventory Analysis (FIA) assistant.

Your role is to help users query and analyze FIA data using natural language.

Key concepts:
- EVALID: Evaluation ID groups statistically valid plot measurements
- Always use EVALID for population estimates, not raw year filtering
- EVALID selection prioritizes statewide evaluations over regional ones
- Species are identified by SPCD (species code)
- States are identified by STATECD (FIPS code)

When users ask questions:
1. First identify what they're looking for (species, location, metric)
2. Find appropriate EVALIDs using get_evalid_info (prioritizes statewide evaluations)
3. Look up species codes with find_species_codes if needed
4. Use execute_tree_command for tree counts or execute_fia_query for complex queries
5. Provide clear, concise answers with appropriate context

Tree counting examples:
- "bySpecies treeType=live" - Live trees by species
- "bySizeClass landType=timber" - Trees by size class on timber land
- "treeDomain=\"SPCD == 131\" treeType=live" - Live loblolly pine trees

Always explain what EVALID you're using and why."""
        
        # Define interrupt points if human approval is enabled
        interrupt_before = ["execute_fia_query", "count_trees_by_criteria"] if self.enable_human_approval else None
        
        # Create the agent
        tools = [
            execute_fia_query,
            get_database_schema,
            find_species_codes,
            get_evalid_info,
            get_recommended_evalid,
            get_state_codes,
            execute_tree_command,
        ]
        
        agent = create_react_agent(
            model=self.llm,
            tools=tools,
            checkpointer=self.checkpointer,
            prompt=system_prompt,
            interrupt_before=interrupt_before,
        )
        
        return agent
    
    def query(
        self,
        question: str,
        thread_id: Optional[str] = None,
        config: Optional[Dict] = None,
    ) -> str:
        """
        Query the FIA database with a natural language question.
        
        Args:
            question: Natural language query
            thread_id: Conversation thread ID for memory
            config: Additional configuration options
            
        Returns:
            Agent's response
        """
        if not thread_id:
            thread_id = f"default_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create config with thread
        run_config = {
            "configurable": {"thread_id": thread_id}
        }
        if config:
            run_config.update(config)
        
        try:
            # Invoke the agent
            response = self.agent.invoke(
                {"messages": [HumanMessage(content=question)]},
                config=run_config,
            )
            
            # Extract the final message
            final_message = response["messages"][-1].content
            return final_message
            
        except Exception as e:
            return f"Error processing query: {str(e)}"
    
    def get_conversation_history(self, thread_id: str) -> List[BaseMessage]:
        """
        Get conversation history for a thread.
        
        Args:
            thread_id: Thread ID to retrieve
            
        Returns:
            List of messages in the conversation
        """
        state = self.agent.get_state({"configurable": {"thread_id": thread_id}})
        return state.values.get("messages", [])
    
    def clear_memory(self, thread_id: Optional[str] = None):
        """
        Clear conversation memory.
        
        Args:
            thread_id: Specific thread to clear, or None for all
        """
        if isinstance(self.checkpointer, MemorySaver):
            # For in-memory checkpointer, we need to clear it
            self.checkpointer = MemorySaver()
            self.agent = self._create_agent()


# Convenience functions for backward compatibility and ease of use

def create_fia_agent(
    db_path: Union[str, Path], 
    api_key: Optional[str] = None, 
    **kwargs
) -> FIAAgent:
    """
    Create a FIA AI Agent with optional configuration.
    
    Args:
        db_path: Path to FIA DuckDB database
        api_key: OpenAI API key
        **kwargs: Additional configuration options
        
    Returns:
        Configured FIAAgent instance
    """
    return FIAAgent(db_path, api_key=api_key, **kwargs)


# Aliases for backward compatibility
FIAAgentModern = FIAAgent  # Alias for old imports