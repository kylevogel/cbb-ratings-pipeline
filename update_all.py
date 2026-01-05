#!/usr/bin/env python3
"""
Main pipeline runner - executes all data collection and build steps.
Run this script to update all rankings data and rebuild the dashboard.

Usage:
    python update_all.py
"""

import subprocess
import sys
import os
from datetime import datetime

def run_script(script_name, description):
    """Run a Python script and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Script: {script_name}")
    print('='*60)
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout per script
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.returncode != 0:
            print(f"Warning: {script_name} returned non-zero exit code")
            if result.stderr:
                print(f"Stderr: {result.stderr}")
            return False
        
        return True
        
    except subprocess.TimeoutExpired:
        print(f"Timeout: {script_name} took too long")
        return False
    except Exception as e:
        print(f"Error running {script_name}: {e}")
        return False


def main():
    start_time = datetime.now()
    print(f"\n{'#'*60}")
    print(f"# CBB Rankings Pipeline")
    print(f"# Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    
    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Track results
    results = {}
    
    # Step 1: Fetch NET rankings
    results['net'] = run_script('update_net_rank.py', 'Fetching NET Rankings')
    
    # Step 2: Fetch KenPom rankings
    results['kenpom'] = run_script('update_kenpom_rank.py', 'Fetching KenPom Rankings')
    
    # Step 3: Fetch BPI rankings
    results['bpi'] = run_script('update_bpi_rank.py', 'Fetching ESPN BPI Rankings')
    
    # Step 4: Fetch AP Poll
    results['ap'] = run_script('update_ap_rank.py', 'Fetching AP Poll Rankings')
    
    # Step 5: Fetch SOS rankings
    results['sos'] = run_script('update_sos_rank.py', 'Fetching SOS Rankings')
    
    # Step 6: Fetch team records
    results['records'] = run_script('update_records.py', 'Fetching Team Records')
    
    # Step 7: Build site rankings and dashboard
    results['build'] = run_script('build_site_rankings.py', 'Building Dashboard')
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n{'#'*60}")
    print(f"# Pipeline Complete")
    print(f"# Duration: {duration:.1f} seconds")
    print(f"{'#'*60}")
    
    print("\nResults:")
    for step, success in results.items():
        status = "✓ Success" if success else "✗ Failed"
        print(f"  {step}: {status}")
    
    # Check if critical steps succeeded
    if results.get('build'):
        print("\n✓ Dashboard updated successfully!")
        print("  View at: docs/index.html")
    else:
        print("\n✗ Dashboard build failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
