class UsageTracker:
    def __init__(self):
        self.voyage_calls = 0
        self.poe_calls = 0
        self.cache_hits = 0
        
    def log_api_call(self, service: str, tokens: int = 0):
        if service == 'voyage':
            self.voyage_calls += 1
        elif service == 'poe':
            self.poe_calls += 1
            
    def get_stats(self):
        return {
            'voyage_api_calls': self.voyage_calls,
            'poe_api_calls': self.poe_calls,
            'cache_hit_rate': self.cache_hits / max(1, self.voyage_calls + self.poe_calls)
        }

usage_tracker = UsageTracker()