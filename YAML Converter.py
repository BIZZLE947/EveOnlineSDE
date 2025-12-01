import sys
import subprocess
import importlib
import os
import argparse
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

# --- 1. AUTO-DEPENDENCY CHECKER ---
def ensure_package(package_name, import_name=None):
    if import_name is None: import_name = package_name
    try:
        importlib.import_module(import_name)
    except ImportError:
        print(f"📦 Installing {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

ensure_package("pyyaml", "yaml")
ensure_package("pandas")
ensure_package("openpyxl")
ensure_package("tqdm")

import yaml
import pandas as pd
from tqdm import tqdm

# --- CONFIGURATION ---
NON_ENGLISH_LOCALES = ['de', 'fr', 'ja', 'ru', 'zh', 'ko', 'es', 'it']

# EVE Online Activity ID Mapping (Name -> ID)
ACTIVITY_MAP = {
    'manufacturing': 1,
    'research_time': 3,
    'research_material': 4,
    'copying': 5,
    'invention': 8,
    'reaction': 11,
    'simple_reactions': 11 
}

# Reverse Mapping for Filenames (ID -> Name)
REV_ACTIVITY_MAP = {
    1: 'manufacturing',
    3: 'research_time',
    4: 'research_material',
    5: 'copying',
    8: 'invention',
    11: 'reaction'
}

# --- SPECIAL LOGIC: BLUEPRINTS ---
def parse_blueprints_special(data):
    """
    Converts blueprints.yaml to Long Format.
    """
    rows = []
    for bp_id, bp_data in data.items():
        if 'activities' not in bp_data:
            continue

        for act_name, act_data in bp_data['activities'].items():
            act_id = ACTIVITY_MAP.get(act_name, 99)
            
            # Products
            products = act_data.get('products', [])
            prod_id = products[0].get('typeID') if products else None
            prod_qty = products[0].get('quantity') if products else None

            # Materials
            materials = act_data.get('materials', [])
            if materials:
                for mat in materials:
                    rows.append({
                        'BlueprintTypeID': bp_id,
                        'activityID': act_id,
                        'materialTypeID': mat.get('typeID'),
                        'quantity': mat.get('quantity'),
                        'ProductTypeID': prod_id,
                        'ProductQuantity': prod_qty
                    })
            else:
                rows.append({
                    'BlueprintTypeID': bp_id,
                    'activityID': act_id,
                    'materialTypeID': None,
                    'quantity': None,
                    'ProductTypeID': prod_id,
                    'ProductQuantity': prod_qty
                })
    return pd.DataFrame(rows)

# --- SPECIAL LOGIC: TYPE MATERIALS ---
def parse_typematerials_special(data):
    """
    Converts typeMaterials.yaml to Long Format with Randomization flags.
    """
    rows = []
    
    for item_id, attributes in data.items():
        if 'materials' in attributes:
            for mat in attributes['materials']:
                rows.append({
                    'root_id': item_id,
                    'MaterialTypeID': mat.get('materialTypeID'),
                    'MaterialQuantity': mat.get('quantity'),
                    'IsRandomized?': 'No'
                })

        if 'randomizedMaterials' in attributes:
            for mat in attributes['randomizedMaterials']:
                rows.append({
                    'root_id': item_id,
                    'MaterialTypeID': mat.get('materialTypeID'),
                    'MaterialQuantity': mat.get('quantity'),
                    'IsRandomized?': 'Yes'
                })
                
    if not rows:
        return pd.DataFrame(columns=['root_id', 'MaterialTypeID', 'MaterialQuantity', 'IsRandomized?'])

    return pd.DataFrame(rows)

# --- WORKER FUNCTION ---
def process_file_worker(args):
    file_path, output_dir, output_format, exclude_cols, keep_all_langs = args
    
    try:
        # 1. Fast Load
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = yaml.load(f, Loader=yaml.CSafeLoader)
            except (AttributeError, yaml.YAMLError):
                f.seek(0)
                data = yaml.safe_load(f)

        if not data:
            return False, f"{file_path.name}: Empty file", None

        # --- LOGIC BRANCHING ---
        filename = file_path.name.lower()
        is_blueprints = "blueprints" in filename

        if is_blueprints:
            df = parse_blueprints_special(data)
        elif "typematerials" in filename:
            df = parse_typematerials_special(data)
        else:
            if isinstance(data, list):
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                records = []
                for key, value in data.items():
                    if isinstance(value, dict):
                        value['root_id'] = key 
                        records.append(value)
                    else:
                        records.append({'root_id': key, 'value': value})
                df = pd.json_normalize(records)
            else:
                return False, f"{file_path.name}: Unknown structure", None
            
            # Standard cleanup
            df.columns = df.columns.astype(str)
            if not keep_all_langs:
                cols_to_drop = [c for c in df.columns if any(c.endswith(f".{loc}") for loc in NON_ENGLISH_LOCALES)]
                if cols_to_drop:
                    df.drop(columns=cols_to_drop, inplace=True)
                rename_map = {}
                for col in df.columns:
                    if col.endswith('.en'):
                        clean_name = col[:-3]
                        if clean_name not in df.columns:
                            rename_map[col] = clean_name
                if rename_map:
                    df.rename(columns=rename_map, inplace=True)

        # Exclusions
        if exclude_cols:
            df = df.drop(columns=exclude_cols, errors='ignore')

        schema_info = {
            "file": file_path.name,
            "columns": sorted(df.columns.tolist())
        }

        # --- SAVE MASTER FILE ---
        if output_format == 'csv':
            master_filename = file_path.with_suffix('.csv').name
            output_path = output_dir / master_filename
            df.to_csv(output_path, index=False, encoding='utf-8')
        elif output_format == 'excel':
            master_filename = file_path.with_suffix('.xlsx').name
            output_path = output_dir / master_filename
            if len(df) > 1000000:
                return False, f"{file_path.name}: Too many rows for Excel. Use CSV.", None
            df.to_excel(output_path, index=False)

        # --- SPECIAL OUTPUTS FOR BLUEPRINTS ---
        if is_blueprints:
            # 1. Activity Split Files (Detailed)
            for act_id, group_df in df.groupby('activityID'):
                act_name = REV_ACTIVITY_MAP.get(act_id, f"activity_{act_id}")
                
                split_filename = f"{file_path.stem}_{act_name}.{output_format}"
                split_path = output_dir / split_filename
                
                if output_format == 'csv':
                    group_df.to_csv(split_path, index=False, encoding='utf-8')
                else:
                    group_df.to_excel(split_path, index=False)

            # 2. Consolidated Product Map (All Activities)
            prod_cols = ['BlueprintTypeID', 'activityID', 'ProductTypeID', 'ProductQuantity']
            
            # Filter rows that have products
            products_df = df[prod_cols].dropna(subset=['ProductTypeID']).copy()
            
            # FORCE INTEGER TYPES on Keys to prevent Float/Int mismatch duplicates
            products_df['BlueprintTypeID'] = products_df['BlueprintTypeID'].astype('int64')
            products_df['activityID'] = products_df['activityID'].astype('int64')
            products_df['ProductTypeID'] = products_df['ProductTypeID'].astype('int64')
            
            # NUCLEAR OPTION: GroupBy + Head(1)
            # This forces exactly 1 row per unique key combination, guaranteed.
            products_df = products_df.groupby(['BlueprintTypeID', 'activityID', 'ProductTypeID']).head(1)
            
            prod_filename = f"{file_path.stem}_products.{output_format}"
            prod_path = output_dir / prod_filename
            
            if output_format == 'csv':
                products_df.to_csv(prod_path, index=False, encoding='utf-8')
            else:
                products_df.to_excel(prod_path, index=False)

        return True, None, schema_info

    except Exception as e:
        return False, f"{file_path.name}: {str(e)}", None

# --- MAIN EXECUTION ---
def main():
    parser = argparse.ArgumentParser(description="EVE SDE YAML Master Converter.")
    parser.add_argument("input_path", help="Path to file or folder")
    parser.add_argument("-f", "--format", choices=['csv', 'excel'], default='csv')
    parser.add_argument("-e", "--exclude", help="Comma-separated columns to remove")
    parser.add_argument("--keep-all-langs", action="store_true", help="Keep non-English text")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="Cores to use")

    args = parser.parse_args()

    exclude_list = []
    if args.exclude:
        exclude_list = [x.strip() for x in args.exclude.split(',')]

    target_path = Path(args.input_path)
    if not target_path.exists():
        print(f"Error: Path '{args.input_path}' not found.")
        sys.exit(1)

    files_to_process = []
    output_dir = None

    # --- RECURSIVE SCANNING ENABLED FOR SDE ---
    if target_path.is_dir():
        print(f"🔍 Scanning folder (Recursive): '{target_path}' ...")
        
        all_files = list(target_path.rglob("*.yaml")) + list(target_path.rglob("*.yml"))
        
        # FILTER: Exclude universe map data
        for f in all_files:
            if "universe" not in f.parts:
                files_to_process.append(f)

        output_dir = target_path / "converted_output"
        output_dir.mkdir(exist_ok=True)
    else:
        if target_path.suffix in ['.yaml', '.yml']:
            files_to_process.append(target_path)
            output_dir = target_path.parent

    total_files = len(files_to_process)
    if total_files == 0:
        print("No YAML files found.")
        sys.exit()

    print(f"🚀 Found {total_files} files. Engine: {args.workers} cores.")

    tasks = []
    for f in files_to_process:
        if "converted_output" in str(f): continue
        tasks.append((f, output_dir, args.format, exclude_list, args.keep_all_langs))

    success_count = 0
    errors = []
    master_schema = {}

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_file_worker, task) for task in tasks]
        
        for future in tqdm(as_completed(futures), total=len(futures), unit="file", colour='cyan'):
            success, msg, schema_data = future.result()
            if success:
                success_count += 1
                if schema_data:
                    master_schema[schema_data['file']] = schema_data['columns']
            else:
                errors.append(msg)

    if master_schema:
        schema_path = output_dir / "schema_map.json"
        try:
            with open(schema_path, 'w', encoding='utf-8') as jf:
                json.dump(master_schema, jf, indent=4)
        except Exception: pass

    print(f"\n✅ Completed: {success_count}/{len(tasks)}")
    if errors:
        log_path = output_dir / "conversion_errors.log"
        with open(log_path, "w", encoding="utf-8") as log:
            log.write("Errors:\n")
            for err in errors: log.write(f"- {err}\n")
        print(f"⚠️ {len(errors)} errors log saved.")

if __name__ == "__main__":
    main()