#!/usr/bin/env python3
"""
Merge separate launch config files from component directories into main launch.json

This script reads launch config files from:
- .vscode/fundamentals/edgar.json (EDGAR)
- .vscode/macro/fred.json (FRED)
- .vscode/macro/bls.json (BLS)
- .vscode/macro/eurostat.json (Eurostat)
- .vscode/macro/imf.json (IMF)
- .vscode/macro/bis.json (BIS)
- .vscode/markets/yfinance.json (YFinance)
- .vscode/markets/ishares.json (iShares)
- .vscode/markets/hyperliquid.json (Hyperliquid)
- .vscode/markets/finra.json (FINRA)
- .vscode/model/hmm.json (HMM Model)

And merges their configurations into .vscode/launch.json
"""

import json
import os
from pathlib import Path

def load_launch_config(file_path):
    """Load launch config (JSON with configurations) from a file"""
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    return None

def merge_launch_configs():
    """Merge all component launch config files into main launch.json"""
    # Get workspace root (parent of .vscode directory)
    script_path = Path(__file__).resolve()
    vscode_dir = script_path.parent
    workspace_root = vscode_dir.parent
    main_launch = vscode_dir / 'launch.json'

    # Component launch config files (one level under area: fundamentals/edgar.json, macro/fred.json, etc.)
    component_files = [
        vscode_dir / 'fundamentals' / 'edgar.json',
        vscode_dir / 'macro' / 'fred.json',
        vscode_dir / 'macro' / 'bls.json',
        vscode_dir / 'macro' / 'eurostat.json',
        vscode_dir / 'macro' / 'imf.json',
        vscode_dir / 'macro' / 'bis.json',
        vscode_dir / 'markets' / 'yfinance.json',
        vscode_dir / 'markets' / 'ishares.json',
        vscode_dir / 'markets' / 'hyperliquid.json',
        vscode_dir / 'markets' / 'finra.json',
        vscode_dir / 'model' / 'hmm.json',
    ]

    # Collect all configurations + inputs (by id)
    all_configs = []
    inputs_by_id = {}

    for component_file in component_files:
        config = load_launch_config(component_file)
        if not config:
            continue
        if 'inputs' in config and isinstance(config['inputs'], list):
            for inp in config['inputs']:
                if not isinstance(inp, dict):
                    continue
                inp_id = inp.get('id')
                if not inp_id:
                    continue
                # First one wins; component-specific files can override by ordering component_files.
                inputs_by_id.setdefault(inp_id, inp)
        if 'configurations' in config:
            all_configs.extend(config['configurations'])
            print(f"Loaded {len(config['configurations'])} configurations from {component_file.relative_to(vscode_dir)}")

    # Create merged launch.json
    merged_config = {"version": "0.2.0", "configurations": all_configs}
    if inputs_by_id:
        merged_config["inputs"] = list(inputs_by_id.values())

    # Write merged config to main launch.json
    with open(main_launch, 'w') as f:
        json.dump(merged_config, f, indent=4)

    print(f"\n✓ Merged {len(all_configs)} configurations into {main_launch.relative_to(vscode_dir)}")
    return len(all_configs)

if __name__ == "__main__":
    try:
        count = merge_launch_configs()
        print(f"\nSuccess! {count} total launch configurations available.")
    except Exception as e:
        print(f"Error merging launch configurations: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
