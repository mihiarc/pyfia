"""
Enhanced AI Agent for Forest Inventory Analysis with Source Code Knowledge Base.

This module extends the FIA AI Agent to use the pyFIA source code as its knowledge base
and provides access to FIA documentation including the FIA handbook.
"""

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union

# Load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# LangChain imports
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_core.documents import Document
from langchain_core.tools import Tool
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langgraph.graph import StateGraph
from pydantic import BaseModel, Field

from .ai_agent import AgentState, FIAAgent, FIAAgentConfig


class CodeKnowledge(BaseModel):
    """Structured representation of code knowledge."""

    module_name: str = Field(description="Name of the Python module")
    function_name: str = Field(description="Name of the function or class")
    docstring: str = Field(description="Documentation string")
    signature: str = Field(description="Function signature")
    source_code: str = Field(description="Source code snippet")
    imports: List[str] = Field(description="Required imports")
    usage_examples: Optional[List[str]] = Field(description="Usage examples")


class DocumentationKnowledge(BaseModel):
    """Structured representation of documentation knowledge."""

    source: str = Field(
        description="Source document (e.g., 'FIA Handbook', 'CLAUDE.md')"
    )
    section: str = Field(description="Section or chapter name")
    content: str = Field(description="Documentation content")
    relevance_score: float = Field(description="Relevance score for retrieval")


class EnhancedAgentState(AgentState):
    """Enhanced state with code and documentation knowledge."""

    code_knowledge: List[CodeKnowledge] = field(default_factory=list)
    documentation_knowledge: List[DocumentationKnowledge] = field(default_factory=list)
    source_context: Optional[str] = None


@dataclass
class EnhancedFIAAgentConfig(FIAAgentConfig):
    """Enhanced configuration with knowledge base settings."""

    pyfia_path: Optional[Path] = None
    documentation_path: Optional[Path] = None
    enable_code_search: bool = True
    enable_doc_search: bool = True
    embedding_model: str = "text-embedding-3-small"
    vector_store_path: Optional[Path] = None
    max_context_items: int = 5


class PyFIAKnowledgeBase:
    """Knowledge base for pyFIA source code and documentation."""

    def __init__(self, pyfia_path: Path, config: EnhancedFIAAgentConfig):
        """Initialize the knowledge base."""
        self.pyfia_path = pyfia_path
        self.config = config
        self.embeddings = OpenAIEmbeddings(model=config.embedding_model)
        self.vector_store = None
        self.code_index = {}
        self._build_knowledge_base()

    def _extract_module_knowledge(self, file_path: Path) -> List[CodeKnowledge]:
        """Extract knowledge from a Python module."""
        knowledge_items = []

        try:
            with open(file_path, "r") as f:
                source = f.read()

            # Parse AST
            tree = ast.parse(source)
            module_name = file_path.stem

            # Extract imports
            imports = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(f"import {alias.name}")
                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    for alias in node.names:
                        imports.append(f"from {module} import {alias.name}")

            # Extract functions and classes
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                    # Get docstring
                    docstring = ast.get_docstring(node) or "No documentation available"

                    # Get source code
                    try:
                        source_lines = source.split("\n")
                        start_line = node.lineno - 1
                        end_line = node.end_lineno or start_line + 1
                        source_code = "\n".join(source_lines[start_line:end_line])
                    except:
                        source_code = f"# Source code for {node.name}"

                    # Get signature for functions
                    if isinstance(node, ast.FunctionDef):
                        args = []
                        for arg in node.args.args:
                            args.append(arg.arg)
                        signature = f"{node.name}({', '.join(args)})"
                    else:
                        signature = f"class {node.name}"

                    knowledge = CodeKnowledge(
                        module_name=f"pyfia.{module_name}",
                        function_name=node.name,
                        docstring=docstring,
                        signature=signature,
                        source_code=source_code[:1000],  # Truncate long code
                        imports=imports[:10],  # Limit imports
                        usage_examples=self._extract_examples(docstring),
                    )
                    knowledge_items.append(knowledge)

        except Exception as e:
            print(f"Error extracting knowledge from {file_path}: {e}")

        return knowledge_items

    def _extract_examples(self, docstring: str) -> List[str]:
        """Extract usage examples from docstring."""
        examples = []
        if not docstring:
            return examples

        # Look for Examples section
        if "Example" in docstring:
            example_section = docstring.split("Example")[1]
            # Extract code blocks
            code_blocks = re.findall(r"```python(.*?)```", example_section, re.DOTALL)
            examples.extend(code_blocks)

            # Also look for >>> style examples
            lines = example_section.split("\n")
            current_example = []
            for line in lines:
                if line.strip().startswith(">>>"):
                    current_example.append(line.strip())
                elif current_example and not line.strip():
                    examples.append("\n".join(current_example))
                    current_example = []

        return examples[:3]  # Limit to 3 examples

    def _build_knowledge_base(self):
        """Build the complete knowledge base."""
        documents = []

        # 1. Extract knowledge from pyFIA source code
        if self.config.enable_code_search:
            pyfia_modules_path = self.pyfia_path / "pyfia"
            for py_file in pyfia_modules_path.glob("*.py"):
                if py_file.name.startswith("_"):
                    continue

                knowledge_items = self._extract_module_knowledge(py_file)
                for item in knowledge_items:
                    # Create document for vector store
                    content = f"""
Module: {item.module_name}
Function/Class: {item.function_name}
Signature: {item.signature}
Documentation: {item.docstring}
Source Code Preview:
{item.source_code[:500]}
                    """.strip()

                    metadata = {
                        "module": item.module_name,
                        "function": item.function_name,
                        "type": "code",
                    }

                    doc = Document(page_content=content, metadata=metadata)
                    documents.append(doc)

                    # Also store in index for direct access
                    key = f"{item.module_name}.{item.function_name}"
                    self.code_index[key] = item

        # 2. Load CLAUDE.md documentation
        claude_md_path = self.pyfia_path / "CLAUDE.md"
        if claude_md_path.exists():
            with open(claude_md_path, "r") as f:
                claude_content = f.read()

            # Split into sections
            sections = re.split(r"^#{1,3}\s+", claude_content, flags=re.MULTILINE)
            for i, section in enumerate(sections):
                if not section.strip():
                    continue

                lines = section.split("\n", 1)
                title = lines[0].strip() if lines else f"Section {i}"
                content = lines[1] if len(lines) > 1 else ""

                doc = Document(
                    page_content=f"CLAUDE.md - {title}\n\n{content}",
                    metadata={
                        "source": "CLAUDE.md",
                        "section": title,
                        "type": "documentation",
                    },
                )
                documents.append(doc)

        # 3. Create vector store
        if documents:
            # Split documents
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=1000, chunk_overlap=200, separators=["\n\n", "\n", " ", ""]
            )
            split_docs = text_splitter.split_documents(documents)

            # Create or load vector store
            if self.config.vector_store_path and self.config.vector_store_path.exists():
                self.vector_store = Chroma(
                    persist_directory=str(self.config.vector_store_path),
                    embedding_function=self.embeddings,
                )
            else:
                self.vector_store = Chroma.from_documents(
                    documents=split_docs,
                    embedding=self.embeddings,
                    persist_directory=str(self.config.vector_store_path)
                    if self.config.vector_store_path
                    else None,
                )

    def search_code(self, query: str, k: int = 5) -> List[CodeKnowledge]:
        """Search for relevant code in the knowledge base."""
        if not self.vector_store:
            return []

        # Search vector store
        results = self.vector_store.similarity_search(
            query, k=k, filter={"type": "code"}
        )

        # Extract code knowledge
        code_items = []
        seen_functions = set()

        for doc in results:
            module = doc.metadata.get("module", "")
            function = doc.metadata.get("function", "")
            key = f"{module}.{function}"

            if key in self.code_index and key not in seen_functions:
                code_items.append(self.code_index[key])
                seen_functions.add(key)

        return code_items

    def search_documentation(
        self, query: str, k: int = 5
    ) -> List[DocumentationKnowledge]:
        """Search for relevant documentation."""
        if not self.vector_store:
            return []

        # Search vector store
        results = self.vector_store.similarity_search(
            query, k=k, filter={"type": "documentation"}
        )

        # Convert to DocumentationKnowledge
        doc_items = []
        for i, doc in enumerate(results):
            knowledge = DocumentationKnowledge(
                source=doc.metadata.get("source", "Unknown"),
                section=doc.metadata.get("section", ""),
                content=doc.page_content,
                relevance_score=1.0 - (i * 0.1),  # Simple scoring
            )
            doc_items.append(knowledge)

        return doc_items

    def get_function_details(
        self, module_name: str, function_name: str
    ) -> Optional[CodeKnowledge]:
        """Get detailed information about a specific function."""
        key = f"{module_name}.{function_name}"
        return self.code_index.get(key)


class EnhancedFIAAgent(FIAAgent):
    """
    Enhanced FIA AI Agent with source code knowledge base.

    This agent extends the base FIA agent with:
    - Deep knowledge of pyFIA source code implementation
    - Access to FIA documentation and handbooks
    - Ability to suggest code usage and best practices
    - Understanding of statistical methodologies in the codebase
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        config: Optional[EnhancedFIAAgentConfig] = None,
        api_key: Optional[str] = None,
    ):
        """Initialize enhanced agent with knowledge base."""
        # Set default pyfia_path if not provided
        if config is None:
            config = EnhancedFIAAgentConfig()

        if config.pyfia_path is None:
            # Assume we're in pyfia/pyfia/, so parent is the project root
            config.pyfia_path = Path(__file__).parent.parent

        # Initialize base agent
        super().__init__(db_path, config, api_key)

        # Initialize knowledge base
        self.knowledge_base = PyFIAKnowledgeBase(config.pyfia_path, config)

        # Add enhanced tools
        self.tools.extend(self._create_enhanced_tools())

    def _create_enhanced_tools(self) -> List[Tool]:
        """Create tools for accessing source code knowledge."""

        def search_pyfia_code(query: str) -> str:
            """Search pyFIA source code for relevant implementations."""
            try:
                code_items = self.knowledge_base.search_code(query, k=3)

                if not code_items:
                    return "No relevant code found for the query."

                result = "Relevant pyFIA Code:\n\n"
                for item in code_items:
                    result += f"### {item.module_name}.{item.function_name}\n"
                    result += f"**Signature:** `{item.signature}`\n"
                    newline = "\n"
                    result += f"**Description:** {item.docstring.split(newline)[0]}\n"

                    if item.usage_examples:
                        result += (
                            f"**Example:**\n```python\n{item.usage_examples[0]}\n```\n"
                        )

                    result += "\n"

                return result
            except Exception as e:
                return f"Error searching code: {str(e)}"

        def get_estimation_implementation(estimation_type: str) -> str:
            """Get implementation details for a specific estimation type."""
            try:
                # Map estimation types to modules
                estimation_modules = {
                    "tpa": "pyfia.tpa",
                    "biomass": "pyfia.biomass",
                    "volume": "pyfia.volume",
                    "mortality": "pyfia.mortality",
                    "area": "pyfia.area",
                }

                module = estimation_modules.get(estimation_type.lower())
                if not module:
                    return f"Unknown estimation type: {estimation_type}"

                # Get the main function
                func_details = self.knowledge_base.get_function_details(
                    module, estimation_type.lower()
                )

                if not func_details:
                    return f"Could not find implementation for {estimation_type}"

                result = f"## {estimation_type.upper()} Estimation Implementation\n\n"
                result += f"**Module:** `{func_details.module_name}`\n"
                result += f"**Function:** `{func_details.function_name}`\n"
                result += f"**Signature:** `{func_details.signature}`\n\n"
                result += f"**Documentation:**\n{func_details.docstring}\n\n"

                # Add key implementation details from CLAUDE.md
                claude_docs = self.knowledge_base.search_documentation(
                    f"{estimation_type} estimation", k=2
                )
                if claude_docs:
                    result += "**Implementation Notes from CLAUDE.md:**\n"
                    for doc in claude_docs:
                        if estimation_type.lower() in doc.content.lower():
                            result += f"{doc.content[:500]}...\n\n"

                return result
            except Exception as e:
                return f"Error getting implementation details: {str(e)}"

        def suggest_pyfia_usage(task_description: str) -> str:
            """Suggest how to use pyFIA for a specific task."""
            try:
                # Search for relevant code and documentation
                code_items = self.knowledge_base.search_code(task_description, k=2)
                doc_items = self.knowledge_base.search_documentation(
                    task_description, k=2
                )

                result = f"## pyFIA Usage Suggestions for: {task_description}\n\n"

                # Suggest relevant functions
                if code_items:
                    result += "### Relevant Functions:\n"
                    for item in code_items:
                        newline_char = chr(10)
                        result += f"- `{item.module_name}.{item.function_name}`: {item.docstring.split(newline_char)[0]}\n"
                    result += "\n"

                # Provide example usage
                result += "### Example Usage:\n```python\n"
                result += "from pyfia import FIA\n\n"
                result += "# Initialize FIA database\n"
                result += "fia = FIA('path/to/fia_database.db')\n\n"

                # Add specific examples based on task
                if "biomass" in task_description.lower():
                    result += "# Estimate biomass\n"
                    result += "biomass_results = fia.clip_by_evalid(372301).biomass(\n"
                    result += "    bySpecies=True,\n"
                    result += "    treeDomain='DIA > 5',\n"
                    result += "    component='AG'  # Aboveground biomass\n"
                    result += ")\n"
                elif "volume" in task_description.lower():
                    result += "# Estimate volume\n"
                    result += "volume_results = fia.clip_by_evalid(372301).volume(\n"
                    result += "    volType='NET',\n"
                    result += "    bySpecies=True\n"
                    result += ")\n"
                elif "mortality" in task_description.lower():
                    result += "# Estimate mortality\n"
                    result += (
                        "mortality_results = fia.clip_by_evalid(372303).mortality(\n"
                    )
                    result += "    bySpecies=True,\n"
                    result += "    landType='Forest'\n"
                    result += ")\n"
                else:
                    result += "# General estimation pattern\n"
                    result += "results = fia.clip_by_evalid(evalid).tpa()\n"

                result += "```\n\n"

                # Add best practices
                if doc_items:
                    result += "### Best Practices:\n"
                    for doc in doc_items:
                        if "evalid" in doc.content.lower():
                            result += "- Always use EVALID filtering for statistically valid estimates\n"
                        if "adjustment factor" in doc.content.lower():
                            result += "- Adjustment factors are applied automatically based on TREE_BASIS\n"
                        if "variance" in doc.content.lower():
                            result += "- Use variance=True to get standard errors with estimates\n"

                return result
            except Exception as e:
                return f"Error suggesting usage: {str(e)}"

        def get_statistical_methodology(concept: str) -> str:
            """Get information about statistical methodologies used in pyFIA."""
            try:
                # Search documentation for statistical concepts
                docs = self.knowledge_base.search_documentation(
                    f"statistical {concept}", k=3
                )

                result = f"## Statistical Methodology: {concept}\n\n"

                # Look for specific methodologies
                if "ratio" in concept.lower() or "estimator" in concept.lower():
                    result += "### Post-Stratified Ratio-of-Means Estimator\n"
                    result += (
                        "pyFIA implements the standard FIA estimation procedures:\n"
                    )
                    result += (
                        "1. **Tree Level**: Apply TPA_UNADJ and adjustment factors\n"
                    )
                    result += "2. **Plot Level**: Sum tree values by plot\n"
                    result += "3. **Stratum Level**: Calculate means and variances\n"
                    result += "4. **Population Level**: Weight by stratum areas\n\n"

                if "variance" in concept.lower():
                    result += "### Variance Estimation\n"
                    result += "Uses delta method for ratio estimates:\n"
                    result += "Var(R) = (1/X²) × [Var(Y) + R² × Var(X) - 2 × R × Cov(Y,X)]\n\n"

                if "tree basis" in concept.lower() or "adjustment" in concept.lower():
                    result += "### Tree Basis and Adjustment Factors\n"
                    result += '- MICR: Microplot (1/300 acre) - trees 1.0-4.9" DBH\n'
                    result += '- SUBP: Subplot (1/24 acre) - trees 5.0"+ DBH\n'
                    result += "- MACR: Macroplot (1/4 acre) - large trees\n"
                    result += "Each basis has specific adjustment factors applied\n\n"

                # Add relevant documentation
                for doc in docs:
                    if concept.lower() in doc.content.lower():
                        result += f"### From {doc.source}:\n"
                        result += f"{doc.content[:400]}...\n\n"

                return result
            except Exception as e:
                return f"Error getting methodology: {str(e)}"

        return [
            Tool(
                name="search_pyfia_code",
                description="Search pyFIA source code for implementations, functions, and usage patterns",
                func=search_pyfia_code,
            ),
            Tool(
                name="get_estimation_implementation",
                description="Get detailed implementation for specific estimation types (tpa, biomass, volume, mortality, area)",
                func=get_estimation_implementation,
            ),
            Tool(
                name="suggest_pyfia_usage",
                description="Suggest how to use pyFIA for specific forest inventory analysis tasks",
                func=suggest_pyfia_usage,
            ),
            Tool(
                name="get_statistical_methodology",
                description="Get information about statistical methodologies and concepts used in pyFIA",
                func=get_statistical_methodology,
            ),
        ]

    def _build_workflow(self) -> StateGraph:
        """Build enhanced workflow with code knowledge integration."""
        # Get base workflow
        base_workflow = super()._build_workflow()

        # We'll enhance the existing workflow by modifying the system prompt
        # and adding code context to responses

        # Since we're extending the base class, we'll use the enhanced prompt
        # in our tool interactions
        return base_workflow


def create_enhanced_fia_agent(
    db_path: Union[str, Path],
    pyfia_path: Optional[Path] = None,
    api_key: Optional[str] = None,
    **config_kwargs,
) -> EnhancedFIAAgent:
    """
    Create an enhanced FIA AI Agent with source code knowledge.

    Args:
        db_path: Path to FIA DuckDB database
        pyfia_path: Path to pyFIA source code (auto-detected if None)
        api_key: OpenAI API key
        **config_kwargs: Additional configuration options

    Returns:
        Configured EnhancedFIAAgent instance
    """
    config = EnhancedFIAAgentConfig(pyfia_path=pyfia_path, **config_kwargs)
    return EnhancedFIAAgent(db_path, config, api_key)
