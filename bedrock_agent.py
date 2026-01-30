# bedrock_agent.py
"""
AWS Bedrock-powered Agentic AI for transaction analytics.
Implements tool use pattern with multiple specialized capabilities.
"""
import boto3
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

class ToolType(Enum):
    QUERY_TRANSACTIONS = "query_transactions"
    ANALYZE_TRENDS = "analyze_trends"
    GET_INSIGHTS = "get_insights"
    SEARCH_KNOWLEDGE = "search_knowledge"


@dataclass
class Tool:
    name: str
    description: str
    parameters: Dict[str, Any]


class TransactionAgent:
    """
    Agentic AI for transaction data analysis using AWS Bedrock.
    Implements ReAct pattern with tool use.
    """
    
    def __init__(self, region: str = 'us-east-1', model_id: str = 'anthropic.claude-3-sonnet-20240229-v1:0'):
        self.bedrock_runtime = boto3.client(
            service_name='bedrock-runtime',
            region_name=region
        )
        self.model_id = model_id
        self.tools = self._define_tools()
        self.vector_store = None
        self.embedder = None
        
    def _define_tools(self) -> List[Dict]:
        """Define available tools for the agent"""
        return [
            {
                "toolSpec": {
                    "name": "query_transactions",
                    "description": "Query transaction database to find specific transactions or aggregates. Use for questions about sales, revenue, specific products, stores, or time periods.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "query_type": {
                                    "type": "string",
                                    "enum": ["aggregate", "filter", "top_n"],
                                    "description": "Type of query to execute"
                                },
                                "filters": {
                                    "type": "object",
                                    "properties": {
                                        "product": {"type": "string"},
                                        "region": {"type": "string"},
                                        "store_id": {"type": "string"},
                                        "date_from": {"type": "string"},
                                        "date_to": {"type": "string"},
                                        "min_amount": {"type": "number"},
                                        "max_amount": {"type": "number"}
                                    }
                                },
                                "aggregation": {
                                    "type": "string",
                                    "enum": ["sum", "avg", "count", "min", "max"],
                                    "description": "Aggregation function for aggregate queries"
                                },
                                "group_by": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Fields to group by"
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Number of results for top_n queries"
                                }
                            },
                            "required": ["query_type"]
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "analyze_trends",
                    "description": "Analyze trends in transaction data over time. Use for questions about growth, patterns, seasonality, or comparisons.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "metric": {
                                    "type": "string",
                                    "enum": ["revenue", "transaction_count", "avg_transaction", "promotion_rate"],
                                    "description": "Metric to analyze"
                                },
                                "dimension": {
                                    "type": "string",
                                    "enum": ["product", "region", "channel", "payment_method", "store"],
                                    "description": "Dimension to analyze by"
                                },
                                "time_period": {
                                    "type": "string",
                                    "enum": ["daily", "weekly", "monthly"],
                                    "description": "Time granularity"
                                },
                                "compare": {
                                    "type": "boolean",
                                    "description": "Whether to compare periods"
                                }
                            },
                            "required": ["metric"]
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "search_knowledge",
                    "description": "Search the knowledge base for relevant information about transactions, products, regions, or business insights. Use for contextual questions or when you need background information.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "Natural language search query"
                                },
                                "num_results": {
                                    "type": "integer",
                                    "description": "Number of results to return",
                                    "default": 5
                                },
                                "filter_type": {
                                    "type": "string",
                                    "enum": ["all", "transactions", "insights"],
                                    "description": "Type of documents to search"
                                }
                            },
                            "required": ["query"]
                        }
                    }
                }
            },
            {
                "toolSpec": {
                    "name": "generate_report",
                    "description": "Generate a formatted report or summary based on analysis results. Use after gathering data with other tools.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "report_type": {
                                    "type": "string",
                                    "enum": ["executive_summary", "detailed_analysis", "comparison", "recommendations"],
                                    "description": "Type of report to generate"
                                },
                                "data": {
                                    "type": "object",
                                    "description": "Data to include in the report"
                                },
                                "format": {
                                    "type": "string",
                                    "enum": ["text", "markdown", "json"],
                                    "description": "Output format"
                                }
                            },
                            "required": ["report_type"]
                        }
                    }
                }
            }
        ]
    
    def _execute_tool(self, tool_name: str, tool_input: Dict) -> str:
        """Execute a tool and return results"""
        
        if tool_name == "query_transactions":
            return self._query_transactions(tool_input)
        elif tool_name == "analyze_trends":
            return self._analyze_trends(tool_input)
        elif tool_name == "search_knowledge":
            return self._search_knowledge(tool_input)
        elif tool_name == "generate_report":
            return self._generate_report(tool_input)
        else:
            return f"Unknown tool: {tool_name}"
    
    def _query_transactions(self, params: Dict) -> str:
        """Execute transaction query using Athena or local data"""
        import pandas as pd
        
        try:
            df = pd.read_parquet("output/transactions_parquet")
        except:
            return json.dumps({"error": "Could not load transaction data"})
        
        # Apply filters
        filters = params.get('filters', {})
        if filters.get('product'):
            df = df[df['product'] == filters['product']]
        if filters.get('region'):
            df = df[df['region'] == filters['region']]
        if filters.get('store_id'):
            df = df[df['store_id'] == filters['store_id']]
        if filters.get('min_amount'):
            df = df[df['amount'] >= filters['min_amount']]
        if filters.get('max_amount'):
            df = df[df['amount'] <= filters['max_amount']]
        
        query_type = params.get('query_type', 'aggregate')
        
        if query_type == 'aggregate':
            group_by = params.get('group_by', [])
            agg_func = params.get('aggregation', 'sum')
            
            if group_by:
                result = df.groupby(group_by)['amount'].agg(agg_func).reset_index()
                return result.to_json(orient='records')
            else:
                result = {
                    'total_amount': float(df['amount'].sum()),
                    'avg_amount': float(df['amount'].mean()),
                    'count': int(len(df)),
                    'min_amount': float(df['amount'].min()),
                    'max_amount': float(df['amount'].max())
                }
                return json.dumps(result)
        
        elif query_type == 'top_n':
            limit = params.get('limit', 10)
            result = df.nlargest(limit, 'amount')[['transaction_id', 'product', 'amount', 'region', 'store_id']]
            return result.to_json(orient='records')
        
        elif query_type == 'filter':
            limit = params.get('limit', 100)
            result = df.head(limit)[['transaction_id', 'product', 'amount', 'region', 'timestamp']]
            return result.to_json(orient='records')
        
        return json.dumps({"error": "Invalid query type"})
    
    def _analyze_trends(self, params: Dict) -> str:
        """Analyze trends in transaction data"""
        import pandas as pd
        
        try:
            df = pd.read_parquet("output/transactions_parquet")
        except:
            return json.dumps({"error": "Could not load transaction data"})
        
        metric = params.get('metric', 'revenue')
        dimension = params.get('dimension')
        
        if metric == 'revenue':
            if dimension:
                result = df.groupby(dimension)['amount'].sum().sort_values(ascending=False)
            else:
                result = {'total_revenue': float(df['amount'].sum())}
                return json.dumps(result)
        
        elif metric == 'transaction_count':
            if dimension:
                result = df.groupby(dimension).size().sort_values(ascending=False)
            else:
                result = {'total_transactions': int(len(df))}
                return json.dumps(result)
        
        elif metric == 'avg_transaction':
            if dimension:
                result = df.groupby(dimension)['amount'].mean().sort_values(ascending=False)
            else:
                result = {'avg_transaction': float(df['amount'].mean())}
                return json.dumps(result)
        
        elif metric == 'promotion_rate':
            if dimension:
                result = df.groupby(dimension)['promotion_applied'].mean().sort_values(ascending=False)
            else:
                result = {'promotion_rate': float(df['promotion_applied'].mean())}
                return json.dumps(result)
        
        return result.to_json() if hasattr(result, 'to_json') else json.dumps(result)
    
    def _search_knowledge(self, params: Dict) -> str:
        """Search vector store for relevant information"""
        from bedrock_embeddings import BedrockEmbeddings
        from vector_store import FAISSVectorStore
        
        query = params.get('query', '')
        num_results = params.get('num_results', 5)
        
        # Initialize if needed
        if self.embedder is None:
            self.embedder = BedrockEmbeddings()
        if self.vector_store is None:
            self.vector_store = FAISSVectorStore()
            try:
                self.vector_store.load('transaction_vector_store')
            except:
                return json.dumps({"error": "Vector store not found. Run vector_store.py first."})
        
        # Get query embedding
        query_embedding = self.embedder.get_embedding(query)
        
        # Search
        results = self.vector_store.search(query_embedding, k=num_results)
        
        formatted_results = []
        for text, metadata, score in results:
            formatted_results.append({
                'text': text,
                'metadata': metadata,
                'relevance_score': score
            })
        
        return json.dumps(formatted_results, indent=2)
    
    def _generate_report(self, params: Dict) -> str:
        """Generate formatted report"""
        report_type = params.get('report_type', 'executive_summary')
        data = params.get('data', {})
        format_type = params.get('format', 'markdown')
        
        # This would be enhanced with actual formatting logic
        report = {
            'report_type': report_type,
            'generated_at': str(pd.Timestamp.now()),
            'data': data,
            'format': format_type
        }
        
        return json.dumps(report, indent=2)
    
    def chat(self, user_message: str, conversation_history: List[Dict] = None) -> str:
        """
        Main chat interface with tool use.
        Implements the agentic loop: think → act → observe → repeat
        """
        if conversation_history is None:
            conversation_history = []
        
        # Add user message to history
        messages = conversation_history + [
            {"role": "user", "content": [{"text": user_message}]}
        ]
        
        system_prompt = """You are an expert data analyst AI assistant specializing in transaction analytics.
        
You have access to a transaction database containing sales data with the following fields:
- transaction_id, customer_id, product, amount, timestamp
- store_id, payment_method, region, channel, promotion_applied

Use your tools to:
1. Query the transaction database for specific data
2. Analyze trends and patterns
3. Search the knowledge base for contextual information
4. Generate reports and recommendations

Always explain your reasoning and provide actionable insights.
When analyzing data, consider multiple dimensions and provide comparative analysis when relevant.
"""
        
        max_iterations = 10
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            # Call Bedrock with tools
            response = self.bedrock_runtime.converse(
                modelId=self.model_id,
                messages=messages,
                system=[{"text": system_prompt}],
                toolConfig={"tools": self.tools}
            )
            
            # Check stop reason
            stop_reason = response['stopReason']
            assistant_message = response['output']['message']
            
            # Add assistant response to messages
            messages.append(assistant_message)
            
            if stop_reason == 'end_turn':
                # Extract final text response
                final_response = ""
                for content in assistant_message['content']:
                    if 'text' in content:
                        final_response += content['text']
                return final_response
            
            elif stop_reason == 'tool_use':
                # Process tool calls
                tool_results = []
                
                for content in assistant_message['content']:
                    if 'toolUse' in content:
                        tool_use = content['toolUse']
                        tool_name = tool_use['name']
                        tool_input = tool_use['input']
                        tool_use_id = tool_use['toolUseId']
                        
                        print(f"  [Tool Call] {tool_name}: {json.dumps(tool_input)[:100]}...")
                        
                        # Execute tool
                        result = self._execute_tool(tool_name, tool_input)
                        
                        tool_results.append({
                            "toolResult": {
                                "toolUseId": tool_use_id,
                                "content": [{"text": result}]
                            }
                        })
                
                # Add tool results to messages
                messages.append({
                    "role": "user",
                    "content": tool_results
                })
            
            else:
                # Unexpected stop reason
                return f"Unexpected stop reason: {stop_reason}"
        
        return "Maximum iterations reached. Please try a more specific question."


def run_interactive_agent():
    """Run interactive agent session"""
    agent = TransactionAgent()
    conversation_history = []
    
    print("=" * 60)
    print("Transaction Analytics Agent")
    print("Powered by AWS Bedrock + RAG")
    print("=" * 60)
    print("\nAsk questions about your transaction data.")
    print("Type 'quit' to exit, 'clear' to reset conversation.\n")
    
    while True:
        user_input = input("\nYou: ").strip()
        
        if user_input.lower() == 'quit':
            print("Goodbye!")
            break
        
        if user_input.lower() == 'clear':
            conversation_history = []
            print("Conversation cleared.")
            continue
        
        if not user_input:
            continue
        
        print("\nAgent: ", end="")
        response = agent.chat(user_input, conversation_history)
        print(response)
        
        # Update conversation history
        conversation_history.append({"role": "user", "content": [{"text": user_input}]})
        conversation_history.append({"role": "assistant", "content": [{"text": response}]})


if __name__ == "__main__":
    run_interactive_agent()
