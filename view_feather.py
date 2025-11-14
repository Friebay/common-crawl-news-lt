import argparse
import os
from pathlib import Path
import pandas as pd
import pyarrow.feather as feather

# Excel row limit (xlsx)
EXCEL_MAX_ROWS = 1_048_576

def list_feather_files(folder):
    return [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".feather")]

def show_basic_info(df, max_rows=10, show_head=True):
    print("Shape:", df.shape)
    print("\nColumns and dtypes:")
    print(df.dtypes)
    print("\nMemory usage (deep):")
    try:
        print(df.memory_usage(deep=True))
    except Exception:
        print("Unable to compute deep memory usage for this dtype.")
    if show_head:
        print("\nHead:")
        print(df.head(max_rows))

def show_pyarrow_schema(path):
    tbl = feather.read_table(path)
    print("PyArrow schema:")
    print(tbl.schema)

def save_to_excel(df, out_path, maxrows=EXCEL_MAX_ROWS):
    pd_path = Path(out_path)
    pd_path.parent.mkdir(parents=True, exist_ok=True)
    nrows = len(df)
    if nrows > maxrows:
        # Save only the first maxrows rows to Excel to avoid overflow
        print(f"Warning: DataFrame has {nrows:,} rows. Excel limit is {maxrows:,}. Saving the first {maxrows:,} rows.")
        save_df = df.iloc[:maxrows]
    else:
        save_df = df

    try:
        save_df.to_excel(pd_path, index=False)
        print(f"Saved Excel file: {pd_path}")
    except Exception as e:
        print(f"Error saving to Excel ({pd_path}): {e}")
        # Try fallback of saving to CSV
        csv_path = pd_path.with_suffix(".csv")
        try:
            save_df.to_csv(csv_path, index=False)
            print(f"Saved CSV fallback: {csv_path}")
        except Exception as e2:
            print(f"CSV fallback also failed: {e2}")

def main(folder="data/metrics", sample_rows=10, verbose=False, columns=None,
         excel=False, excel_threshold=18, excel_folder="exports"):
    folder = Path(folder)
    if not folder.exists():
        raise SystemExit(f"Folder does not exist: {folder}")

    files = list_feather_files(str(folder))
    if not files:
        raise SystemExit(f"No .feather files found in {folder}")

    for fpath in files:
        print("="*80)
        print("File:", fpath)
        print("Size (bytes):", os.path.getsize(fpath))
        if verbose:
            print("PyArrow Schema:")
            # Print schema with pyarrow, useful for nested or arrow types
            try:
                show_pyarrow_schema(fpath)
            except Exception as e:
                print("Error reading schema using pyarrow:", e)

        # Read selected columns or the entire file
        try:
            if columns:
                df = pd.read_feather(fpath, columns=columns)
            else:
                df = pd.read_feather(fpath)
        except Exception as e:
            print("Error reading as pandas DataFrame:", e)
            print("Trying with pyarrow.feather.read_table for more detail...")
            try:
                tbl = feather.read_table(fpath)
                df = tbl.to_pandas()
            except Exception as e2:
                print("Error reading with pyarrow:", e2)
                continue

        # Decide whether to save to Excel: forced by --excel or triggered by threshold
        columns_count = df.shape[1]
        should_save_excel = excel or (columns_count >= excel_threshold)

        # If saving, do not print the full head; show only metadata
        if should_save_excel:
            print(f"Columns: {columns_count} >= excel-threshold ({excel_threshold}) -> saving to Excel instead of printing content.")
            # Build output filename
            infile = Path(fpath).stem
            out_dir = Path(excel_folder)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{infile}_view.xlsx"

            # Save DataFrame to Excel (or CSV fallback)
            save_to_excel(df, out_file)
            # Also print a short sample and types for quick verification
            show_basic_info(df, max_rows=min(sample_rows, 10), show_head=False)
        else:
            # Show info and a sample head
            show_basic_info(df, max_rows=sample_rows, show_head=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="View .feather files in a folder.")
    parser.add_argument("--folder", "-f", default="data/metrics", help="Folder with .feather files.")
    parser.add_argument("--rows", "-r", type=int, default=10, help="Number of rows shown in head.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show pyarrow schema for each file.")
    parser.add_argument("--columns", "-c", nargs="+", help="Read only these columns (memory-friendly).")
    parser.add_argument("--excel", "-x", action="store_true", help="Force saving DataFrame to Excel for each file.")
    parser.add_argument("--excel-threshold", type=int, default=3, help="If DataFrame columns >= threshold, save to Excel (default: 18).")
    parser.add_argument("--excel-folder", type=str, default="exports", help="Folder to save Excel files into (default: exports).")
    args = parser.parse_args()
    main(folder=args.folder, sample_rows=args.rows, verbose=args.verbose, columns=args.columns,
         excel=args.excel, excel_threshold=args.excel_threshold, excel_folder=args.excel_folder)