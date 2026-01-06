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
            timeout=120
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
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    results = {}
    
    results['net'] = run_script('update_net_rank.py', 'Fetching NET Rankings')    
    results['kenpom'] = run_script('update_kenpom_rank.py', 'Fetching KenPom Rankings')
    results['bpi'] = run_script('update_bpi_rank.py', 'Fetching ESPN BPI Rankings')    
    results['ap'] = run_script('update_ap_rank.py', 'Fetching AP Poll Rankings')    
    results['sos'] = run_script('update_sos_rank.py', 'Fetching SOS Rankings')    
    results['records'] = run_script('update_records.py', 'Fetching Team Records')    
    results['build'] = run_script('build_site_rankings.py', 'Building Dashboard')
    
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
    
    if results.get('build'):
        print("\n✓ Dashboard updated successfully!")
        print("  View at: docs/index.html")
    else:
        print("\n✗ Dashboard build failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
