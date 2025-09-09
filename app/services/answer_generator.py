"""
Answer Generator Service - Converts query results to natural language
Generates human-friendly answers from data
"""
from typing import Dict, List, Any, Union
from datetime import datetime, date
from decimal import Decimal
from loguru import logger
import json
import re

from app.core.llm import llm_client


class AnswerGenerator:
    """
    Service for generating natural language answers from query results
    """
    
    def __init__(self):
        self.answer_cache = {}
        self.formatting_rules = self._init_formatting_rules()
    
    def _init_formatting_rules(self) -> Dict[str, Any]:
        """
        Initialize formatting rules for different data types
        """
        return {
            'currency_indicators': ['price', 'cost', 'amount', 'revenue', 'salary', 'fee', 'total'],
            'percentage_indicators': ['rate', 'percent', 'ratio', 'margin'],
            'date_formats': {
                'short': '%Y-%m-%d',
                'long': '%B %d, %Y',
                'month_year': '%B %Y'
            },
            'number_formats': {
                'large': lambda x: f"{x:,.0f}" if x >= 1000 else str(x),
                'decimal': lambda x: f"{x:,.2f}",
                'percentage': lambda x: f"{x:.1f}%"
            }
        }
    
    async def generate_answer(
        self,
        query: str,
        results: Union[List[Dict[str, Any]], Dict[str, Any]],
        context: Dict[str, Any] = {},
        language: str = "auto"
    ) -> str:
        """
        Generate natural language answer from query results
        
        Args:
            query: Original user query
            results: Query results (from SQL or vector search)
            context: Additional context (intent, schema info, etc.)
            language: Target language ('auto', 'en', 'id', etc.)
            
        Returns:
            Natural language answer
        """
        try:
            # Detect language if auto
            if language == "auto":
                language = self._detect_language(query)
            
            # Handle empty results
            if not results or (isinstance(results, list) and len(results) == 0):
                return self._generate_no_data_response(query, language)
            
            # Determine result type and generate appropriate answer
            if isinstance(results, dict):
                if 'rows' in results:
                    # SQL query results
                    answer = await self._generate_sql_answer(
                        query, results['rows'], context, language
                    )
                elif 'error' in results:
                    # Error result
                    answer = self._generate_error_response(
                        query, results['error'], language
                    )
                else:
                    # Single result
                    answer = await self._generate_single_answer(
                        query, results, context, language
                    )
            else:
                # List of results
                answer = await self._generate_list_answer(
                    query, results, context, language
                )
            
            return answer
            
        except Exception as e:
            logger.error(f"Answer generation failed: {e}")
            return self._generate_fallback_response(query, language)
    
    async def _generate_sql_answer(
        self,
        query: str,
        rows: List[Dict[str, Any]],
        context: Dict[str, Any],
        language: str
    ) -> str:
        """
        Generate answer from SQL query results
        """
        if not rows:
            return self._generate_no_data_response(query, language)
        
        # Get query intent
        intent = context.get('intent', {})
        
        # Handle different query types
        if intent.get('type') == 'aggregation':
            return await self._generate_aggregation_answer(
                query, rows, intent, language
            )
        elif intent.get('type') == 'comparison':
            return await self._generate_comparison_answer(
                query, rows, intent, language
            )
        elif len(rows) == 1:
            return await self._generate_single_answer(
                query, rows[0], context, language
            )
        else:
            return await self._generate_table_answer(
                query, rows, context, language
            )
    
    async def _generate_aggregation_answer(
        self,
        query: str,
        rows: List[Dict[str, Any]],
        intent: Dict[str, Any],
        language: str
    ) -> str:
        """
        Generate answer for aggregation queries
        """
        # Format aggregation results
        if len(rows) == 1 and len(rows[0]) == 1:
            # Single aggregation value
            value = list(rows[0].values())[0]
            formatted_value = self._format_value(value, list(rows[0].keys())[0])
            
            if language == "id":
                return f"Berdasarkan data yang tersedia, {formatted_value}."
            else:
                return f"Based on the available data, the result is {formatted_value}."
        
        # Multiple aggregation results or grouped data
        formatted_data = self._format_data_for_display(rows[:10])
        
        # Use LLM to generate natural language summary
        prompt = f"""Convert this data into a natural language answer.

Original question: "{query}"
Data:
{formatted_data}

Requirements:
- Be concise and clear
- Use natural language, not technical terms
- {"Answer in Indonesian" if language == "id" else "Answer in English"}
- If there are multiple rows, summarize key findings
- Format numbers appropriately (use commas for thousands)"""
        
        answer = await llm_client.generate(
            prompt=prompt,
            temperature=0.3,
            max_tokens=500
        )
        
        return answer
    
    async def _generate_comparison_answer(
        self,
        query: str,
        rows: List[Dict[str, Any]],
        intent: Dict[str, Any],
        language: str
    ) -> str:
        """
        Generate answer for comparison queries
        """
        formatted_data = self._format_data_for_display(rows)
        
        prompt = f"""Generate a comparison analysis from this data.

Original question: "{query}"
Data:
{formatted_data}

Requirements:
- Clearly compare the items requested
- Highlight differences and similarities
- Use percentages or ratios where appropriate
- {"Answer in Indonesian" if language == "id" else "Answer in English"}
- Be objective and data-driven"""
        
        answer = await llm_client.generate(
            prompt=prompt,
            temperature=0.3,
            max_tokens=600
        )
        
        return answer
    
    async def _generate_single_answer(
        self,
        query: str,
        data: Dict[str, Any],
        context: Dict[str, Any],
        language: str
    ) -> str:
        """
        Generate answer for single result
        """
        # Format the data
        formatted_parts = []
        
        for key, value in data.items():
            if value is not None and not key.startswith('_'):
                formatted_value = self._format_value(value, key)
                readable_key = self._make_readable(key)
                formatted_parts.append(f"{readable_key}: {formatted_value}")
        
        if language == "id":
            intro = f"Untuk pertanyaan '{query}', berikut informasinya:\n"
        else:
            intro = f"Regarding '{query}', here's the information:\n"
        
        return intro + "\n".join(formatted_parts[:10])  # Limit to 10 fields
    
    async def _generate_list_answer(
        self,
        query: str,
        results: List[Dict[str, Any]],
        context: Dict[str, Any],
        language: str
    ) -> str:
        """
        Generate answer for list of results
        """
        if len(results) > 10:
            # Too many results, summarize
            return await self._generate_summary_answer(
                query, results, context, language
            )
        
        formatted_data = self._format_data_for_display(results)
        
        if language == "id":
            intro = f"Ditemukan {len(results)} hasil untuk '{query}':\n\n"
        else:
            intro = f"Found {len(results)} results for '{query}':\n\n"
        
        return intro + formatted_data
    
    async def _generate_table_answer(
        self,
        query: str,
        rows: List[Dict[str, Any]],
        context: Dict[str, Any],
        language: str
    ) -> str:
        """
        Generate answer with tabular data
        """
        # Determine if we should show as table or summary
        if len(rows) <= 5 and len(rows[0]) <= 5:
            # Small dataset, show as formatted table
            return self._format_as_table(rows, query, language)
        else:
            # Large dataset, generate summary
            return await self._generate_summary_answer(
                query, rows, context, language
            )
    
    async def _generate_summary_answer(
        self,
        query: str,
        results: List[Any],
        context: Dict[str, Any],
        language: str
    ) -> str:
        """
        Generate summary for large result sets
        """
        # Sample data for summary
        sample_size = min(20, len(results))
        sample_data = results[:sample_size]
        
        formatted_sample = self._format_data_for_display(sample_data)
        
        prompt = f"""Summarize this data into key insights.

Original question: "{query}"
Total results: {len(results)}
Sample data (first {sample_size} rows):
{formatted_sample}

Requirements:
- Provide key statistics (totals, averages, ranges)
- Highlight notable patterns or outliers
- Mention this is based on {len(results)} total results
- {"Answer in Indonesian" if language == "id" else "Answer in English"}
- Be concise but informative"""
        
        answer = await llm_client.generate(
            prompt=prompt,
            temperature=0.3,
            max_tokens=600
        )
        
        return answer
    
    def _format_value(self, value: Any, field_name: str = "") -> str:
        """
        Format value based on type and field name
        """
        if value is None:
            return "N/A"
        
        field_lower = field_name.lower()
        
        # Handle different data types
        if isinstance(value, bool):
            return "Yes" if value else "No"
        
        elif isinstance(value, (datetime, date)):
            return value.strftime(self.formatting_rules['date_formats']['long'])
        
        elif isinstance(value, (int, float, Decimal)):
            # Check if it's currency
            if any(indicator in field_lower for indicator in self.formatting_rules['currency_indicators']):
                return f"${float(value):,.2f}"
            
            # Check if it's percentage
            elif any(indicator in field_lower for indicator in self.formatting_rules['percentage_indicators']):
                return f"{float(value):.1f}%"
            
            # Regular number
            elif isinstance(value, int) or float(value).is_integer():
                return f"{int(value):,}"
            else:
                return f"{float(value):,.2f}"
        
        elif isinstance(value, dict):
            # JSON data
            return json.dumps(value, indent=2)
        
        elif isinstance(value, list):
            return ', '.join(str(v) for v in value[:5])
        
        else:
            # String or other
            str_value = str(value)
            if len(str_value) > 100:
                return str_value[:100] + "..."
            return str_value
    
    def _format_data_for_display(
        self,
        data: List[Dict[str, Any]],
        max_rows: int = 10
    ) -> str:
        """
        Format data for display in answer
        """
        if not data:
            return "No data available"
        
        formatted_rows = []
        
        for i, row in enumerate(data[:max_rows], 1):
            formatted_fields = []
            for key, value in row.items():
                if not key.startswith('_') and value is not None:
                    formatted_value = self._format_value(value, key)
                    readable_key = self._make_readable(key)
                    formatted_fields.append(f"{readable_key}: {formatted_value}")
            
            if formatted_fields:
                formatted_rows.append(f"{i}. " + ", ".join(formatted_fields[:5]))
        
        result = "\n".join(formatted_rows)
        
        if len(data) > max_rows:
            result += f"\n... and {len(data) - max_rows} more results"
        
        return result
    
    def _format_as_table(
        self,
        rows: List[Dict[str, Any]],
        query: str,
        language: str
    ) -> str:
        """
        Format data as a simple text table
        """
        if not rows:
            return "No data"
        
        # Get columns
        columns = list(rows[0].keys())
        display_columns = [col for col in columns if not col.startswith('_')][:5]
        
        # Build header
        header = " | ".join(self._make_readable(col) for col in display_columns)
        separator = "-" * len(header)
        
        # Build rows
        table_rows = []
        for row in rows[:10]:
            values = []
            for col in display_columns:
                value = self._format_value(row.get(col), col)
                # Truncate long values
                if len(value) > 20:
                    value = value[:17] + "..."
                values.append(value)
            table_rows.append(" | ".join(values))
        
        # Combine
        if language == "id":
            intro = f"Hasil untuk '{query}':\n\n"
        else:
            intro = f"Results for '{query}':\n\n"
        
        table = "\n".join([header, separator] + table_rows)
        
        return intro + table
    
    def _make_readable(self, field_name: str) -> str:
        """
        Convert field name to readable format
        """
        # Remove prefixes
        field_name = re.sub(r'^(tbl_|t_|tb_)', '', field_name)
        
        # Replace underscores with spaces
        field_name = field_name.replace('_', ' ')
        
        # Capitalize words
        field_name = ' '.join(word.capitalize() for word in field_name.split())
        
        return field_name
    
    def _detect_language(self, text: str) -> str:
        """
        Simple language detection
        """
        text_lower = text.lower()
        
        # Indonesian indicators
        id_words = ['apa', 'siapa', 'dimana', 'kapan', 'berapa', 'bagaimana',
                    'yang', 'dan', 'atau', 'dengan', 'untuk', 'dari']
        
        # English indicators
        en_words = ['what', 'who', 'where', 'when', 'how', 'which',
                    'the', 'and', 'or', 'with', 'for', 'from']
        
        id_score = sum(1 for word in id_words if word in text_lower)
        en_score = sum(1 for word in en_words if word in text_lower)
        
        return "id" if id_score > en_score else "en"
    
    def _generate_no_data_response(self, query: str, language: str) -> str:
        """
        Generate response when no data is found
        """
        if language == "id":
            return (
                f"Tidak ditemukan data yang relevan untuk '{query}'. "
                "Pastikan data sudah tersinkronisasi atau coba dengan kata kunci yang berbeda."
            )
        else:
            return (
                f"No relevant data found for '{query}'. "
                "Please ensure the data is synchronized or try different keywords."
            )
    
    def _generate_error_response(self, query: str, error: str, language: str) -> str:
        """
        Generate response for errors
        """
        if language == "id":
            return (
                f"Terjadi kesalahan saat memproses '{query}'. "
                f"Detail: {error}. "
                "Silakan coba lagi atau hubungi administrator."
            )
        else:
            return (
                f"An error occurred while processing '{query}'. "
                f"Details: {error}. "
                "Please try again or contact the administrator."
            )
    
    def _generate_fallback_response(self, query: str, language: str) -> str:
        """
        Generate fallback response
        """
        if language == "id":
            return (
                f"Maaf, saya tidak dapat memproses pertanyaan '{query}' saat ini. "
                "Silakan coba dengan pertanyaan yang lebih sederhana."
            )
        else:
            return (
                f"Sorry, I cannot process the question '{query}' at this time. "
                "Please try with a simpler question."
            )
    
    async def generate_chart_suggestion(
        self,
        data: List[Dict[str, Any]],
        query: str
    ) -> Dict[str, Any]:
        """
        Suggest appropriate chart type for data visualization
        """
        if not data:
            return {"type": "none", "reason": "No data to visualize"}
        
        # Analyze data structure
        first_row = data[0]
        numeric_fields = []
        categorical_fields = []
        temporal_fields = []
        
        for key, value in first_row.items():
            if isinstance(value, (int, float, Decimal)):
                numeric_fields.append(key)
            elif isinstance(value, (datetime, date)):
                temporal_fields.append(key)
            elif isinstance(value, str):
                categorical_fields.append(key)
        
        # Determine best chart type
        if len(data) == 1:
            # Single value - use metric/KPI display
            return {
                "type": "metric",
                "fields": list(first_row.keys()),
                "reason": "Single value display"
            }
        
        elif temporal_fields and numeric_fields:
            # Time series data - use line chart
            return {
                "type": "line",
                "x_axis": temporal_fields[0],
                "y_axis": numeric_fields[:3],  # Max 3 lines
                "reason": "Time series data"
            }
        
        elif len(categorical_fields) == 1 and len(numeric_fields) >= 1:
            # Category vs values - use bar chart
            if len(data) <= 10:
                return {
                    "type": "bar",
                    "x_axis": categorical_fields[0],
                    "y_axis": numeric_fields[0],
                    "reason": "Categorical comparison"
                }
            else:
                # Too many categories - use horizontal bar
                return {
                    "type": "horizontal_bar",
                    "x_axis": numeric_fields[0],
                    "y_axis": categorical_fields[0],
                    "limit": 10,
                    "reason": "Many categories to compare"
                }
        
        elif len(numeric_fields) >= 2:
            # Multiple numeric values - could be scatter or grouped bar
            if len(data) > 20:
                return {
                    "type": "scatter",
                    "x_axis": numeric_fields[0],
                    "y_axis": numeric_fields[1],
                    "reason": "Correlation analysis"
                }
            else:
                return {
                    "type": "grouped_bar",
                    "categories": categorical_fields[0] if categorical_fields else "index",
                    "series": numeric_fields[:3],
                    "reason": "Multiple metrics comparison"
                }
        
        else:
            # Default to table
            return {
                "type": "table",
                "reason": "Complex data structure"
            }


# Global answer generator instance
answer_generator = AnswerGenerator()