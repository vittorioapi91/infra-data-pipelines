# Launch Configuration Files

This directory contains separate `launch.json` files for each component, organized by component type.

## Structure

- `.vscode/fundamentals/edgar.json` - EDGAR downloader configurations
- `.vscode/macro/fred.json` - FRED data downloader configurations
- `.vscode/macro/bls.json` - BLS data downloader configurations
- `.vscode/macro/eurostat.json` - Eurostat data downloader configurations
- `.vscode/macro/imf.json` - IMF data downloader configurations
- `.vscode/macro/bis.json` - BIS data downloader configurations
- `.vscode/markets/yfinance.json` - YFinance configurations
- `.vscode/model/hmm.json` - HMM model configurations
- `.vscode/launch.json` - Main merged file (auto-generated)

## Usage

VS Code only reads launch configurations from `.vscode/launch.json` at the workspace root. To use the separate component files:

1. **Edit the component-specific files** in their respective directories
2. **Run the merge script** to update the main launch.json:
   ```bash
   python3 .vscode/merge_launch_configs.py
   ```

The merge script automatically combines all component launch.json files into the main `.vscode/launch.json` file that VS Code uses.

## Adding New Configurations

1. Edit the appropriate component's config file (e.g. `macro/fred.json`)
2. Run the merge script to update the main file
3. The new configuration will appear in VS Code's debug dropdown
