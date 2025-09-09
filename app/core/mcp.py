"""
Model Context Protocol (MCP) Implementation for ATABOT 2.0
Provides standardized interfaces for AI model interactions
"""
from typing import Dict, List, Any, Optional, Callable
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
import json
from loguru import logger
from abc import ABC, abstractmethod

from app.core.config import settings

class MCPToolType(Enum):
    """Types of MCP tools available"""
    QUERY = "query"           # Database queries
    SEARCH = "search"         # Vector search
    ANALYZE = "analyze"       # Schema analysis
    SYNC = "sync"            # Data synchronization
    TRANSFORM = "transform"   # Data transformation

class MCPResourceType(Enum):
    """Types of MCP resources"""
    DATABASE = "database"
    EMBEDDING = "embedding"
    SCHEMA = "schema"
    CACHE = "cache"
    DOCUMENT = "document"

@dataclass
class MCPContext:
    """Context object for MCP operations"""
    session_id: str
    user_id: Optional[str] = None
    active_schema: Optional[str] = None
    conversation_history: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    
    def add_message(self, role: str, content: str, metadata: Optional[Dict] = None):
        """Add a message to conversation history"""
        self.conversation_history.append({
            "role": role,
            "content": content,
            "metadata": metadata or {},
            "timestamp": datetime.now().isoformat()
        })
    
    def get_recent_context(self, n: int = 5) -> List[Dict[str, Any]]:
        """Get recent conversation context"""
        return self.conversation_history[-n:] if self.conversation_history else []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary"""
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "active_schema": self.active_schema,
            "conversation_history": self.conversation_history,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat()
        }

@dataclass
class MCPTool:
    """Represents an MCP tool"""
    name: str
    type: MCPToolType
    description: str
    parameters: Dict[str, Any]
    handler: Callable
    requires_context: bool = True
    requires_auth: bool = False
    
    async def execute(self, params: Dict[str, Any], context: Optional[MCPContext] = None) -> Any:
        """Execute the tool"""
        if self.requires_context and not context:
            raise ValueError(f"Tool {self.name} requires context")
        
        # Validate parameters
        self._validate_parameters(params)
        
        # Execute handler
        if context:
            return await self.handler(params, context)
        else:
            return await self.handler(params)
    
    def _validate_parameters(self, params: Dict[str, Any]):
        """Validate tool parameters"""
        required = self.parameters.get("required", [])
        for param in required:
            if param not in params:
                raise ValueError(f"Missing required parameter: {param}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert tool to dictionary for API response"""
        return {
            "name": self.name,
            "type": self.type.value,
            "description": self.description,
            "parameters": self.parameters,
            "requires_context": self.requires_context,
            "requires_auth": self.requires_auth
        }

@dataclass
class MCPResource:
    """Represents an MCP resource"""
    id: str
    type: MCPResourceType
    name: str
    description: str
    uri: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert resource to dictionary"""
        return {
            "id": self.id,
            "type": self.type.value,
            "name": self.name,
            "description": self.description,
            "uri": self.uri,
            "metadata": self.metadata
        }

class MCPProvider(ABC):
    """Abstract base class for MCP providers"""
    
    @abstractmethod
    async def list_tools(self) -> List[MCPTool]:
        """List available tools"""
        pass
    
    @abstractmethod
    async def list_resources(self) -> List[MCPResource]:
        """List available resources"""
        pass
    
    @abstractmethod
    async def execute_tool(self, tool_name: str, params: Dict[str, Any], context: MCPContext) -> Any:
        """Execute a specific tool"""
        pass
    
    @abstractmethod
    async def get_resource(self, resource_id: str) -> MCPResource:
        """Get a specific resource"""
        pass

class ATABOTMCPProvider(MCPProvider):
    """ATABOT-specific MCP provider implementation"""
    
    def __init__(self):
        self.tools: Dict[str, MCPTool] = {}
        self.resources: Dict[str, MCPResource] = {}
        self._initialize_tools()
        self._initialize_resources()
    
    def _initialize_tools(self):
        """Initialize available MCP tools"""
        
        # Database Query Tool
        self.tools["db_query"] = MCPTool(
            name="db_query",
            type=MCPToolType.QUERY,
            description="Execute SQL queries on the active schema",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL query to execute"},
                    "schema": {"type": "string", "description": "Schema name (optional, uses active schema)"},
                    "limit": {"type": "integer", "description": "Result limit", "default": 100}
                },
                "required": ["query"]
            },
            handler=self._handle_db_query,
            requires_context=True
        )
        
        # Vector Search Tool
        self.tools["vector_search"] = MCPTool(
            name="vector_search",
            type=MCPToolType.SEARCH,
            description="Perform semantic search using vector embeddings",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "schema": {"type": "string", "description": "Schema to search in"},
                    "table": {"type": "string", "description": "Specific table (optional)"},
                    "top_k": {"type": "integer", "description": "Number of results", "default": 10},
                    "filters": {"type": "object", "description": "SQL WHERE conditions"}
                },
                "required": ["query", "schema"]
            },
            handler=self._handle_vector_search,
            requires_context=True
        )
        
        # Schema Analysis Tool
        self.tools["analyze_schema"] = MCPTool(
            name="analyze_schema",
            type=MCPToolType.ANALYZE,
            description="Analyze database schema structure and patterns",
            parameters={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name to analyze"},
                    "deep_analysis": {"type": "boolean", "description": "Perform deep analysis", "default": False}
                },
                "required": ["schema"]
            },
            handler=self._handle_schema_analysis,
            requires_context=True
        )
        
        # Data Sync Tool
        self.tools["sync_data"] = MCPTool(
            name="sync_data",
            type=MCPToolType.SYNC,
            description="Synchronize data to vector store",
            parameters={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "table": {"type": "string", "description": "Table name"},
                    "mode": {"type": "string", "enum": ["full", "incremental"], "default": "incremental"}
                },
                "required": ["schema", "table"]
            },
            handler=self._handle_data_sync,
            requires_context=True,
            requires_auth=True
        )
        
        # Query Decomposition Tool
        self.tools["decompose_query"] = MCPTool(
            name="decompose_query",
            type=MCPToolType.TRANSFORM,
            description="Decompose complex queries into simpler sub-queries",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Complex query to decompose"},
                    "context": {"type": "object", "description": "Additional context"}
                },
                "required": ["query"]
            },
            handler=self._handle_query_decomposition,
            requires_context=True
        )
    
    def _initialize_resources(self):
        """Initialize available MCP resources"""
        
        # Database Resource
        self.resources["database"] = MCPResource(
            id="database",
            type=MCPResourceType.DATABASE,
            name="PostgreSQL Database",
            description="Main PostgreSQL database with pgvector",
            uri=settings.DATABASE_URL.split("@")[1] if "@" in settings.DATABASE_URL else "localhost",
            metadata={
                "pool_size": settings.DATABASE_POOL_SIZE,
                "has_pgvector": True
            }
        )
        
        # Embedding Service Resource
        self.resources["embeddings"] = MCPResource(
            id="embeddings",
            type=MCPResourceType.EMBEDDING,
            name="VoyageAI Embeddings",
            description="Embedding generation service",
            uri="api.voyageai.com",
            metadata={
                "model": settings.VOYAGE_MODEL,
                "dimensions": settings.EMBEDDING_DIMENSIONS
            }
        )
    
    async def list_tools(self) -> List[MCPTool]:
        """List all available tools"""
        return list(self.tools.values())
    
    async def list_resources(self) -> List[MCPResource]:
        """List all available resources"""
        return list(self.resources.values())
    
    async def execute_tool(self, tool_name: str, params: Dict[str, Any], context: MCPContext) -> Any:
        """Execute a specific tool"""
        if tool_name not in self.tools:
            raise ValueError(f"Tool not found: {tool_name}")
        
        tool = self.tools[tool_name]
        
        # Log tool execution
        logger.info(f"Executing MCP tool: {tool_name} with params: {params}")
        
        # Add to context history
        context.add_message(
            role="tool",
            content=f"Executing {tool_name}",
            metadata={"tool": tool_name, "params": params}
        )
        
        # Execute tool
        result = await tool.execute(params, context)
        
        # Add result to context
        context.add_message(
            role="tool_result",
            content=json.dumps(result) if not isinstance(result, str) else result,
            metadata={"tool": tool_name}
        )
        
        return result
    
    async def get_resource(self, resource_id: str) -> MCPResource:
        """Get a specific resource"""
        if resource_id not in self.resources:
            raise ValueError(f"Resource not found: {resource_id}")
        return self.resources[resource_id]
    
    # Tool Handlers
    async def _handle_db_query(self, params: Dict[str, Any], context: MCPContext) -> Any:
        """Handle database query execution"""
        from app.core.database import db_pool
        
        schema = params.get("schema", context.active_schema)
        if not schema:
            raise ValueError("No schema specified and no active schema in context")
        
        query = params["query"]
        limit = params.get("limit", 100)
        
        # Add LIMIT if not present
        if "LIMIT" not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        # Execute query
        try:
            rows = await db_pool.fetch(query)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Database query error: {e}")
            raise
    
    async def _handle_vector_search(self, params: Dict[str, Any], context: MCPContext) -> Any:
        """Handle vector search"""
        from app.services.search_service import search_service
        
        results = await search_service.hybrid_search(
            query=params["query"],
            schema=params["schema"],
            table=params.get("table"),
            top_k=params.get("top_k", 10),
            filters=params.get("filters", {})
        )
        
        return results
    
    async def _handle_schema_analysis(self, params: Dict[str, Any], context: MCPContext) -> Any:
        """Handle schema analysis"""
        from app.services.schema_analyzer import schema_analyzer
        
        analysis = await schema_analyzer.analyze_schema(
            schema_name=params["schema"]
        )
        
        return analysis
    
    async def _handle_data_sync(self, params: Dict[str, Any], context: MCPContext) -> Any:
        """Handle data synchronization"""
        from app.services.sync_service import sync_service
        
        result = await sync_service.sync_table(
            schema=params["schema"],
            table=params["table"],
            mode=params.get("mode", "incremental")
        )
        
        return result
    
    async def _handle_query_decomposition(self, params: Dict[str, Any], context: MCPContext) -> Any:
        """Handle query decomposition"""
        from app.services.query_decomposer import query_decomposer
        
        sub_queries = await query_decomposer.decompose(
            query=params["query"],
            context=params.get("context", {})
        )
        
        return sub_queries

# Global MCP provider instance
mcp_provider = ATABOTMCPProvider()

class MCPOrchestrator:
    """Orchestrates MCP operations for complex workflows"""
    
    def __init__(self, provider: MCPProvider):
        self.provider = provider
        self.contexts: Dict[str, MCPContext] = {}
    
    def create_context(self, session_id: str, user_id: Optional[str] = None) -> MCPContext:
        """Create a new MCP context"""
        context = MCPContext(
            session_id=session_id,
            user_id=user_id
        )
        self.contexts[session_id] = context
        return context
    
    def get_context(self, session_id: str) -> Optional[MCPContext]:
        """Get existing context"""
        return self.contexts.get(session_id)
    
    async def process_request(
        self,
        request: Dict[str, Any],
        session_id: str
    ) -> Dict[str, Any]:
        """
        Process an MCP request
        
        Request format:
        {
            "action": "execute_tool",
            "tool": "tool_name",
            "params": {...}
        }
        """
        # Get or create context
        context = self.get_context(session_id)
        if not context:
            context = self.create_context(session_id)
        
        action = request.get("action")
        
        if action == "execute_tool":
            tool_name = request.get("tool")
            params = request.get("params", {})
            result = await self.provider.execute_tool(tool_name, params, context)
            return {
                "success": True,
                "result": result,
                "context": context.to_dict()
            }
        
        elif action == "list_tools":
            tools = await self.provider.list_tools()
            return {
                "success": True,
                "tools": [t.to_dict() for t in tools]
            }
        
        elif action == "list_resources":
            resources = await self.provider.list_resources()
            return {
                "success": True,
                "resources": [r.to_dict() for r in resources]
            }
        
        elif action == "set_schema":
            schema = request.get("schema")
            context.active_schema = schema
            context.metadata["schema_set_at"] = datetime.now().isoformat()
            return {
                "success": True,
                "message": f"Active schema set to: {schema}",
                "context": context.to_dict()
            }
        
        else:
            raise ValueError(f"Unknown action: {action}")
    
    async def execute_workflow(
        self,
        workflow: List[Dict[str, Any]],
        session_id: str
    ) -> List[Dict[str, Any]]:
        """
        Execute a workflow of multiple MCP operations
        
        Workflow format:
        [
            {"action": "set_schema", "schema": "retail"},
            {"action": "execute_tool", "tool": "analyze_schema", "params": {...}},
            {"action": "execute_tool", "tool": "vector_search", "params": {...}}
        ]
        """
        results = []
        
        for step in workflow:
            try:
                result = await self.process_request(step, session_id)
                results.append(result)
                
                # Stop on failure
                if not result.get("success"):
                    break
                    
            except Exception as e:
                logger.error(f"Workflow step failed: {e}")
                results.append({
                    "success": False,
                    "error": str(e),
                    "step": step
                })
                break
        
        return results

# Global MCP orchestrator
mcp_orchestrator = MCPOrchestrator(mcp_provider)