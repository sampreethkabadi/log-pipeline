#!/usr/bin/env python3
"""
Manual injection tool for testing.
Edits injection state in a running generator by signaling via a file.
For now, the simplest approach: modify the generator to inject on startup.
"""

print("For today, inject by editing INJECTION_STATE at the top of log_generator.py.")
print("Example: set INJECTION_STATE[\"auth\"][\"error_rate_multiplier\"] = 10")
print("Then restart the generator and watch error rate on auth-logs spike.")
