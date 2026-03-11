"""
IIoT Analytics Agent — Terminal Interface

Usage:
    python agent/agent.py

Commands:
    Type any natural language question about the sensor data.
    Type 'kql' to see the last generated KQL query.
    Type 'quit' or 'exit' to stop.

Example questions:
    - which plant had the most failures last week?
    - show me machines with tool wear above 200
    - what is the average torque on line 3?
    - how many heat failures happened per day this month?
    - which line has the highest failure rate?
"""

from openai_client import nl_to_kql
from kusto_client import run_query
import pandas as pd

# Terminal formatting
RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
GREY   = "\033[90m"


def print_header():
    print(f"\n{BOLD}{CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{BOLD}{CYAN}   IIoT Analytics Agent — Predictive Maintenance{RESET}")
    print(f"{BOLD}{CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{RESET}")
    print(f"{GREY}  Ask questions about your sensor telemetry data.{RESET}")
    print(f"{GREY}  Type 'kql' to see the last query. 'quit' to exit.{RESET}\n")


def print_results(df: pd.DataFrame):
    if df.empty:
        print(f"{YELLOW}  No results returned.{RESET}\n")
        return

    # Print table
    print(f"\n{GREEN}  Results ({len(df)} rows):{RESET}")
    print(df.to_string(index=False))
    print()


def run_agent():
    print_header()
    last_kql = None

    while True:
        try:
            user_input = input(f"{BOLD}You:{RESET} ").strip()

            if not user_input:
                continue

            if user_input.lower() in ("quit", "exit"):
                print(f"\n{GREY}Goodbye.{RESET}\n")
                break

            # Show last KQL query
            if user_input.lower() == "kql":
                if last_kql:
                    print(f"\n{GREY}Last KQL query:{RESET}")
                    print(f"{CYAN}{last_kql}{RESET}\n")
                else:
                    print(f"{YELLOW}No query run yet.{RESET}\n")
                continue

            # Step 1: Convert NL to KQL
            print(f"{GREY}  Generating KQL...{RESET}", end="\r")
            try:
                kql = nl_to_kql(user_input)
                last_kql = kql
            except Exception as e:
                print(f"{RED}  Error generating KQL: {e}{RESET}\n")
                continue

            print(f"{GREY}  KQL → {kql[:80]}{'...' if len(kql) > 80 else ''}{RESET}")

            # Step 2: Execute KQL
            print(f"{GREY}  Querying Kusto...{RESET}", end="\r")
            try:
                df = run_query(kql)
                print_results(df)
            except ValueError as e:
                print(f"{RED}  Query error: {e}{RESET}")
                print(f"{YELLOW}  Try rephrasing your question.{RESET}\n")

        except KeyboardInterrupt:
            print(f"\n\n{GREY}Interrupted. Goodbye.{RESET}\n")
            break


if __name__ == "__main__":
    run_agent()
