import subprocess
import sys
import time
from pathlib import Path

def main():
    start_time = time.time()
    base_dir = Path(__file__).resolve().parent
    scripts_dir = base_dir / "scripts"

    print("\n" + "#" * 60)
    print("      EJECUTANDO PIPELINE DE TRANSFORMACIÓN (MEDALLION)")
    print("#" * 60)

    pipeline = ["bronze.py", "silver.py", "gold.py", "report.py"]

    for script in pipeline:
        script_path = scripts_dir / script
        if script_path.exists():
            print(f"\nPROCESANDO: {script}")
            subprocess.run([sys.executable, str(script_path)])
        else:
            print(f"⚠️ No se encontró: {script}")

    end_time = time.time()
    print(f"\nPIPELINE FINALIZADO EN {(end_time - start_time)/60:.2f} MINUTOS")

if __name__ == "__main__":
    main()