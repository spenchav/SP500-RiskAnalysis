# run_dbt.py is for testing only
import os
import sys
import subprocess
from dotenv import load_dotenv

# Load environment variables from .env file in the current directory
load_dotenv()

# Check if essential variables are loaded (optional but good practice)
required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'DB_PORT']
missing_vars = [var for var in required_vars if var not in os.environ]
if missing_vars:
    print(f"Error: Missing environment variables: {', '.join(missing_vars)}")
    print("Ensure they are defined in your .env file.")
    sys.exit(1)

# Get the dbt command arguments passed to this script
# e.g., if you run "python run_dbt.py debug", dbt_args will be ["debug"]
dbt_args = sys.argv[1:]

if not dbt_args:
    print("Usage: python run_dbt.py <dbt_command> [dbt_options]")
    print("Example: python run_dbt.py debug")
    sys.exit(1)

# Construct the full dbt command
command = ['dbt'] + dbt_args

print(f"Executing command: {' '.join(command)}")
print(f"Working directory: {os.path.join(os.getcwd(), 'dbt')}") # Show where dbt will run

# Run the dbt command from within the 'dbt' subdirectory
try:
    # Use shell=True on Windows if needed, but subprocess.run is generally preferred
    # Set cwd to ensure dbt runs in the correct project context
    process = subprocess.run(command, check=True, text=True, cwd='dbt',
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("\n--- dbt output ---")
    print(process.stdout)
    if process.stderr:
            print("\n--- dbt errors ---")
            print(process.stderr)
except subprocess.CalledProcessError as e:
    print(f"\n--- dbt failed (exit code {e.returncode}) ---")
    print(e.stdout)
    print(e.stderr)
    sys.exit(e.returncode)
except FileNotFoundError:
        print("Error: 'dbt' command not found. Is dbt installed and in your PATH?")
        sys.exit(1)

print("\n--- dbt execution finished ---")