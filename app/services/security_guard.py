"""
Security Guard Service - Protects against prompt injection and unauthorized access
Ensures Atabot only answers data-related questions within its scope
"""
import re
from typing import Dict, Any, List, Optional, Tuple
from loguru import logger
import hashlib

class SecurityGuard:
    """
    Security service to validate and sanitize user queries
    """

    def __init__(self):
        # Patterns that indicate potential prompt injection
        self.dangerous_patterns = [
            # System override attempts
            r'(?i)(forget|ignore|disregard)\s+(all|everything|previous|above|system|instructions)',
            r'(?i)(act|pretend|roleplay|now you are|you are now)\s+(as|like)',
            r'(?i)(system|admin|root|developer)\s+(prompt|message|instruction)',

            # Direct instruction attempts - more specific to avoid Indonesian conflicts
            r'(?i)^(now you are|from now on you are|starting now you are)',
            r'(?i)(tell me|what is|explain)\s+(your|the)\s+(prompt|instruction|system)',
            r'(?i)(show|display|print|output)\s+(your|the)\s+(prompt|code|system)',

            # Jailbreak attempts
            r'(?i)(jailbreak|break free|escape|override)',
            r'(?i)\b(do anything now|unlimited|unrestricted)\b',  # Fixed: don't match Indonesian "dan"
            r'(?i)(creative mode|developer mode|god mode)',

            # Context switching
            r'(?i)(new conversation|reset conversation|start over)',
            r'(?i)(previous|forget|clear)\s+(conversation|context|memory)',

            # Information extraction attempts
            r'(?i)(what are you|who are you|your purpose|your role)',
            r'(?i)(training data|dataset|model|gpt|openai|anthropic)',

            # Direct commands to ignore business scope
            r'(?i)(answer anything|general knowledge|outside.*scope)',
            r'(?i)(not related to|unrelated to)\s+(business|data|inventory)'
        ]

        # Allowed query patterns for business data
        self.allowed_patterns = [
            # Inventory/stock queries
            r'(?i)(stok|stock|inventory|jumlah|berapa)',

            # Program information
            r'(?i)(program|promo|campaign)',

            # Product searches
            r'(?i)(product|produk|item|barang)',

            # Data queries
            r'(?i)(data|informasi|info|list|show|tampilkan)',

            # Analysis queries
            r'(?i)(analisis|analysis|report|laporan)',

            # Comparison queries
            r'(?i)(compare|bandingkan|vs|versus)',

            # Search queries
            r'(?i)(cari|search|find|where)',

            # Count/sum queries
            r'(?i)(total|sum|count|hitung)'
        ]

        self.blocked_queries_log = []

        # Indonesian safe words that should not trigger security violations
        self.indonesian_safe_words = {
            'dan', 'yang', 'di', 'ke', 'dari', 'untuk', 'dengan', 'pada',
            'dalam', 'atau', 'jika', 'kalau', 'akan', 'telah', 'sudah',
            'ada', 'tidak', 'ini', 'itu', 'saya', 'kamu', 'kami', 'mereka'
        }

    def validate_query(self, query: str, context: Dict[str, Any] = {}) -> Tuple[bool, Optional[str]]:
        """
        Validate if query is safe and within business scope

        Returns:
            (is_valid, reason_if_invalid)
        """

        # Basic sanitization
        if not query or len(query.strip()) == 0:
            return False, "Empty query"

        if len(query) > 1000:
            return False, "Query too long (max 1000 characters)"

        # Check for dangerous patterns with Indonesian language consideration
        for pattern in self.dangerous_patterns:
            match = re.search(pattern, query)
            if match:
                matched_text = match.group(0).lower()

                # Check if the matched text is just an Indonesian safe word
                if matched_text.strip() in self.indonesian_safe_words:
                    continue  # Skip this match as it's a safe Indonesian word

                # Check if it's part of a larger Indonesian phrase
                if self._is_indonesian_business_context(query, matched_text):
                    continue  # Skip this match as it's in business context

                self._log_security_violation(query, f"Dangerous pattern detected: {pattern}")
                return False, "Query contains potentially harmful instructions"

        # Check if query is business-related
        is_business_related = self._is_business_query(query)
        if not is_business_related:
            self._log_security_violation(query, "Non-business query detected")
            return False, "Query is outside business data scope"

        # Additional context-based validation
        if context.get('session_suspicious', False):
            return False, "Session flagged as suspicious"

        return True, None

    def _is_business_query(self, query: str) -> bool:
        """
        Check if query is related to business data
        """
        query_lower = query.lower()

        # Check for allowed patterns
        for pattern in self.allowed_patterns:
            if re.search(pattern, query_lower):
                return True

        # Additional heuristics for business queries
        business_keywords = [
            'warehouse', 'gudang', 'supplier', 'customer', 'pelanggan',
            'sales', 'penjualan', 'purchase', 'pembelian', 'order',
            'transaction', 'transaksi', 'revenue', 'profit', 'margin',
            'category', 'kategori', 'brand', 'merek', 'price', 'harga'
        ]

        for keyword in business_keywords:
            if keyword in query_lower:
                return True

        return False

    def sanitize_query(self, query: str) -> str:
        """
        Sanitize query by removing potentially harmful elements
        """
        # Remove excessive whitespace
        sanitized = re.sub(r'\s+', ' ', query.strip())

        # Remove potential injection markers
        sanitized = re.sub(r'[<>{}[\]\\]', '', sanitized)

        # Limit length
        if len(sanitized) > 500:
            sanitized = sanitized[:500] + "..."

        return sanitized

    def _is_indonesian_business_context(self, query: str, matched_text: str) -> bool:
        """
        Check if the matched text is in valid Indonesian business context
        """
        query_lower = query.lower()

        # Common Indonesian business patterns where "dan" is safe
        safe_patterns = [
            r'\bstok\s+\w+\s+dan\s+ada',  # "stok ALO dan ada"
            r'\bberapa\s+\w+\s+dan\s+\w+',  # "berapa stok dan program"
            r'\bdata\s+\w+\s+dan\s+\w+',  # "data inventory dan program"
            r'\binfo\s+\w+\s+dan\s+\w+',  # "info produk dan status"
            r'\bjumlah\s+\w+\s+dan\s+\w+',  # "jumlah stok dan warehouse"
        ]

        for pattern in safe_patterns:
            if re.search(pattern, query_lower):
                return True

        # Check if surrounded by business/inventory keywords
        business_keywords = ['stok', 'stock', 'produk', 'program', 'item', 'barang',
                           'inventory', 'warehouse', 'gudang', 'berapa', 'ada']

        words_around = query_lower.replace(matched_text, '').split()
        business_word_count = sum(1 for word in words_around if any(kw in word for kw in business_keywords))

        # If query has multiple business keywords, likely legitimate
        return business_word_count >= 2

    def generate_safe_response_template(self, query: str) -> str:
        """
        Generate a safe response template for rejected queries
        """
        return f"""Maaf, saya adalah Atabot yang dirancang khusus untuk membantu analisis data bisnis dan inventory.

Saya hanya dapat menjawab pertanyaan yang berkaitan dengan:
- Data stok dan inventory
- Informasi produk dan program
- Analisis penjualan dan transaksi
- Laporan bisnis dari database

Pertanyaan Anda tampaknya di luar cakupan data bisnis yang saya kelola. Silakan ajukan pertanyaan tentang data inventory, stok produk, atau informasi bisnis lainnya."""

    def _log_security_violation(self, query: str, reason: str):
        """
        Log security violations for monitoring
        """
        from datetime import datetime, timezone

        violation = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'query_hash': hashlib.md5(query.encode()).hexdigest(),
            'reason': reason,
            'query_preview': query[:100] + "..." if len(query) > 100 else query
        }

        self.blocked_queries_log.append(violation)
        logger.warning(f"Security violation: {reason} - Query: {query[:50]}...")

        # Keep only last 100 violations
        if len(self.blocked_queries_log) > 100:
            self.blocked_queries_log = self.blocked_queries_log[-100:]

    def get_security_stats(self) -> Dict[str, Any]:
        """
        Get security statistics
        """
        return {
            'total_violations': len(self.blocked_queries_log),
            'recent_violations': self.blocked_queries_log[-10:] if self.blocked_queries_log else []
        }

    def is_session_suspicious(self, session_history: List[str]) -> bool:
        """
        Analyze session history for suspicious patterns
        """
        if len(session_history) < 3:
            return False

        # Check for repeated injection attempts
        violation_count = 0
        for query in session_history[-10:]:  # Last 10 queries
            is_valid, _ = self.validate_query(query)
            if not is_valid:
                violation_count += 1

        # Mark as suspicious if >50% of recent queries are violations
        return violation_count / min(len(session_history), 10) > 0.5

    def validate_response_content(self, response: str) -> bool:
        """
        Validate that generated response doesn't leak system information
        """
        dangerous_response_patterns = [
            r'(?i)(prompt|instruction|system message)',
            r'(?i)(claude|gpt|openai|anthropic)',
            r'(?i)(training|dataset|model)',
            r'(?i)(I am an AI|I am a language model)',
            r'(?i)(my programming|my instructions)',
        ]

        for pattern in dangerous_response_patterns:
            if re.search(pattern, response):
                logger.warning(f"Response contains dangerous pattern: {pattern}")
                return False

        return True

# Global security guard instance
security_guard = SecurityGuard()