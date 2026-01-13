import os
import shutil
from pathlib import Path

def main():
    """
    Ensures the secure environment (ELT directory and its templates) is set up.
    This script will create missing directories and copy missing template files
    from 'secure_env_template' to '~/ELT' without overwriting existing files.
    """
    target_elt_path = "C:/ELT"

    # The 'secure_env_template' is expected to be a sibling directory to 'setup/'
    # Path(__file__).resolve().parent gives 'elt-monorepo/setup/'
    # .parent again gives 'elt-monorepo/'
    # / "secure_env_template" then points to the template folder.
    template_dir = Path(__file__).resolve().parent.parent / "secure_env_template"

    print(f"🔍 Ensuring secure environment structure at {target_elt_path}...")

    # --- Step 1: Ensure the base ELT directory exists ---
    # If the main ELT directory doesn't exist, create it.
    if not target_elt_path.exists():
        print(f"📁 Creating base ELT directory: {target_elt_path}...")
        try:
            os.makedirs(target_elt_path, exist_ok=True) # exist_ok=True prevents error if it somehow gets created concurrently
            print("✅ Base ELT directory created.")
        except Exception as e:
            print(f"❌ Error creating base ELT directory: {e}")
            return # Exit if base directory can't be created

    # --- Step 2: Iterate through the template and copy missing items ---
    copied_count = 0
    skipped_count = 0
    error_occurred = False

    try:
        # os.walk traverses the directory tree, yielding (dirpath, dirnames, filenames)
        for root, dirs, files in os.walk(template_dir):
            # Calculate the relative path from the template_dir to the current 'root'
            # e.g., if root is 'secure_env_template/.DSITConnections/TEST',
            # relative_path will be '.DSITConnections/TEST'
            relative_path = Path(root).relative_to(template_dir)

            # Determine the corresponding destination path in the ELT directory
            destination_root = target_elt_path / relative_path

            # Create any missing subdirectories in the destination
            for dir_name in dirs:
                dest_dir_path = destination_root / dir_name
                if not dest_dir_path.exists():
                    os.makedirs(dest_dir_path, exist_ok=True)
                    print(f"    - Created directory: {dest_dir_path.relative_to(target_elt_path)}")
                    copied_count += 1
                # else:
                #     # Optionally, uncomment this line if you want to see every skipped directory
                #     # print(f"    - Directory already exists: {dest_dir_path.relative_to(target_elt_path)} (skipped)")
                #     skipped_count += 1

            # Copy any missing files to the destination
            for file_name in files:
                src_file_path = Path(root) / file_name
                dest_file_path = destination_root / file_name

                if not dest_file_path.exists():
                    # shutil.copy2 preserves metadata (like modification times)
                    shutil.copy2(src_file_path, dest_file_path)
                    print(f"    - Copied file: {dest_file_path.relative_to(target_elt_path)}")
                    copied_count += 1
                else:
                    print(f"    - File already exists: {dest_file_path.relative_to(target_elt_path)} (skipped)")
                    skipped_count += 1

    except Exception as e:
        print(f"❌ An error occurred during the secure environment setup process: {e}")
        error_occurred = True # Set flag to indicate an error
    finally:
        # --- Step 3: Provide a summary and next steps ---
        if not error_occurred:
            if copied_count > 0:
                print(f"\n✅ Secure environment structure updated. ({copied_count} new items added, {skipped_count} existing items skipped)")
            elif skipped_count > 0:
                print(f"\n✅ Secure environment structure already in place. ({skipped_count} existing items skipped, no new items needed)")
            else:
                print("\nNo changes needed or found in template. Environment up-to-date.")

            print("\n📝 Important Next Steps:")
            print("- Please ensure the following files are reviewed and updated with your actual credentials:")
            print("    - `C:/ELT/.DSITConnections/TEST/.env`")
            print("    - `C:/ELT/.DSITConnections/TEST/dataBaseConnections.yaml`")
            print("    - `C:/ELT/.DSITConnections/PROD/.env`")
            print("    - `C:/ELT/.DSITConnections/PROD/dataBaseConnections.yaml`")
            print("- Review `C:/ELT/.early_alert/.env` for early email alert configurations.")
            print("- Check `C:/ELT/logs/early_logs/` for pre-initialization log output.")
        else:
            print("\nSetup incomplete due to errors. Please review the output above.")


if __name__ == "__main__":
    main()