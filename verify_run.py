import sys
import subprocess
import os

def check_deps():
    print("Checking dependencies...")
    try:
        import pandas
        import matplotlib
        import seaborn
        import wordcloud
        import gender_guesser
        import vobject
        print("Dependencies found.")
        return True
    except ImportError as e:
        print(f"Missing dependency: {e}")
        return False

def run_analysis():
    print("Running analysis...")
    cmd = [
        sys.executable, "main.py",
        "--msgstore", "msgstore.db",
        "--wa", "wa.db",
        "--vcf", "contacts.vcf"
    ]
    ret = subprocess.call(cmd)
    return ret == 0

if __name__ == "__main__":
    if check_deps():
        if run_analysis():
            print("Verification SUCCESS")
        else:
            print("Verification FAILED during execution")
            sys.exit(1)
    else:
        print("Please install dependencies: pip install pandas matplotlib seaborn wordcloud gender-guesser vobject attrs")
        sys.exit(1)
