"""
Generate Team Alias Mappings

This script reads the suggested_team_aliases.csv and creates properly formatted
entries to add to team_alias.csv. It uses smart filtering to avoid bad suggestions.

Usage:
    python generate_aliases.py
"""

import pandas as pd
import os

def main():
    print("=" * 80)
    print("TEAM ALIAS GENERATOR")
    print("=" * 80)
    print()
    
    # Load suggestions
    suggestions_file = 'suggested_team_aliases.csv'
    if not os.path.exists(suggestions_file):
        print(f"✗ {suggestions_file} not found. Run diagnose_team_names.py first.")
        return
    
    df_suggestions = pd.read_csv(suggestions_file)
    print(f"Loaded {len(df_suggestions)} suggestions")
    print()
    
    # Load existing aliases to avoid duplicates
    existing_aliases = set()
    if os.path.exists('team_alias.csv'):
        df_existing = pd.read_csv('team_alias.csv')
        print(f"Loaded {len(df_existing)} existing aliases")
        # Assuming format: espn_name,alternate_name,source
        if 'alternate_name' in df_existing.columns:
            existing_aliases = set(df_existing['alternate_name'].str.lower())
        print()
    
    # Filter and create new aliases
    # Only accept suggestions with similarity >= 0.85 to avoid bad matches
    high_confidence = df_suggestions[df_suggestions['similarity'] >= 0.85].copy()
    
    print(f"Found {len(high_confidence)} high-confidence suggestions (similarity >= 0.85)")
    print()
    
    # Group by ranking_name to avoid duplicates
    new_aliases = []
    seen_ranking_names = set()
    
    for _, row in high_confidence.iterrows():
        ranking_name = row['ranking_name']
        espn_name = row['suggested_espn_name']
        source = row['source']
        similarity = row['similarity']
        
        # Skip if we've already processed this ranking name
        if ranking_name.lower() in seen_ranking_names:
            continue
        
        # Skip if already in existing aliases
        if ranking_name.lower() in existing_aliases:
            continue
        
        seen_ranking_names.add(ranking_name.lower())
        
        new_aliases.append({
            'espn_name': espn_name,
            'alternate_name': ranking_name,
            'source': source,
            'similarity': similarity
        })
    
    if not new_aliases:
        print("✓ No new aliases to add (all suggestions already exist or are low confidence)")
        return
    
    df_new = pd.DataFrame(new_aliases)
    
    # Sort by similarity (highest first)
    df_new = df_new.sort_values('similarity', ascending=False)
    
    print(f"Generated {len(df_new)} new alias mappings:")
    print()
    print(df_new.to_string(index=False))
    print()
    
    # Save to a file that can be appended to team_alias.csv
    output_file = 'new_aliases_to_add.csv'
    df_new[['espn_name', 'alternate_name', 'source']].to_csv(output_file, index=False)
    print(f"✓ Saved to: {output_file}")
    print()
    
    print("=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print()
    print("1. Review 'new_aliases_to_add.csv' to verify the mappings are correct")
    print("2. IMPORTANT: Manually check ambiguous ones like:")
    print("   - 'Army West Point' → Check if it really matches the suggested team")
    print("   - 'CSUN' → Verify the correct match")
    print("   - 'Boston University' vs 'Boston U' naming")
    print()
    print("3. To add these to team_alias.csv, you can either:")
    print("   a. Manually copy/paste the good ones")
    print("   b. Run this command (after verifying):")
    print("      tail -n +2 new_aliases_to_add.csv >> team_alias.csv")
    print()
    print("4. Re-run the pipeline: python update_all.py")
    print()
    
    # Also show some specific issues that need manual review
    print("=" * 80)
    print("MANUAL REVIEW NEEDED")
    print("=" * 80)
    print()
    
    # Load all suggestions to show low-confidence ones
    low_confidence = df_suggestions[df_suggestions['similarity'] < 0.85].copy()
    
    if len(low_confidence) > 0:
        print(f"These {len(low_confidence)} suggestions have low confidence and need manual review:")
        print()
        for _, row in low_confidence.iterrows():
            print(f"  {row['source']:10} | '{row['ranking_name']}' → '{row['suggested_espn_name']}' (similarity: {row['similarity']:.2f})")
        print()
        print("For these, you'll need to manually find the correct ESPN names in your game data")
        print("and add them to team_alias.csv in the format: espn_name,alternate_name,source")
    
    print()

if __name__ == "__main__":
    main()
