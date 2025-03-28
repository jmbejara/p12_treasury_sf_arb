import sys
import os
from os import environ
from pathlib import Path
import shutil
sys.path.append(os.path.abspath("./src"))

from settings import config


############################################
# Directories and Environment Configuration
############################################
DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
OS_TYPE = config("OS_TYPE")

# Prevent Jupyter from complaining about file path validations
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"

############################################
# Helper Functions for Jupyter Notebook Tasks
############################################
def jupyter_execute_notebook(notebook):
    """Execute a Jupyter notebook in-place, overwriting the original."""
    return (
        f"jupyter nbconvert --execute --to notebook "
        f"--ClearMetadataPreprocessor.enabled=True --log-level WARN "
        f"--inplace ./src/{notebook}.ipynb"
    )

def jupyter_to_html(notebook, output_dir=OUTPUT_DIR):
    """Convert a Jupyter notebook to an HTML file in OUTPUT_DIR."""
    return (
        f"jupyter nbconvert --to html --log-level WARN "
        f"--output-dir={output_dir} ./src/{notebook}.ipynb"
    )

def jupyter_clear_output(notebook):
    """Clear output cells and metadata from a Jupyter notebook."""
    return (
        f"jupyter nbconvert --log-level WARN --ClearOutputPreprocessor.enabled=True "
        f"--ClearMetadataPreprocessor.enabled=True --inplace ./src/{notebook}.ipynb"
    )

############################################
# Task Definitions
############################################

def task_clean_raw():
    """Run `clean_raw.ipynb` to generate necessary datasets in `_data`."""
    return {
        "actions": [
            jupyter_execute_notebook("clean_raw")
        ],
        "targets": [
            DATA_DIR / "treasury_df.csv",
            DATA_DIR / "ois_df.csv",
            DATA_DIR / "last_day_df.csv"
        ],
        "clean": True,
    }

def task_generate_reference():
    """Run generate_reference.py to create reference.csv"""
    return {
        "actions": ["python ./src/generate_reference.py"],
        "targets": [DATA_DIR / "reference.csv"],
        "clean": True,
    }

def task_calc_treasury_data():
    """Run `calc_treasury_data.py` which processes Treasury SF data."""
    return {
        "actions": ["python ./src/calc_treasury_data.py"],
        "file_dep": [
            DATA_DIR / "treasury_df.csv",
            DATA_DIR / "ois_df.csv",
            DATA_DIR / "last_day_df.csv"
        ],
        "targets": [DATA_DIR / "treasury_sf_output.csv"],
        "clean": True,
    }


def task_process_treasury_data_notebook():
    """Optionally run `process_treasury_data.ipynb` if needed."""
    return {
        "actions": [
            jupyter_execute_notebook("process_treasury_data")
        ],
        "file_dep": [DATA_DIR / "treasury_sf_output.csv"],
        "clean": True,
    }

def task_plot_exploration():
    """Run `01_explore_basis_trade_data_new.ipynb` for visualization."""
    return {
        "actions": [
            jupyter_execute_notebook("01_explore_basis_trade_data_new")
        ],
        "file_dep": [DATA_DIR / "treasury_sf_output.csv"],
        "clean": True,
    }

def task_test_calc_treasury_data():
    """Run unit tests for calc_treasury_data.py"""
    return {
        "actions": ["pytest ./src/test_calc_treasury_data.py"],
        "file_dep": [
            "./src/calc_treasury_data.py",
            DATA_DIR / "treasury_sf_output.csv",
            DATA_DIR / "reference.csv"
        ],
        "task_dep": ["generate_reference"],  # Ensure reference.csv exists
        "clean": True,
    }

def task_test_load_bases_data():
    """Run unit tests for `load_bases_data.py`."""
    return {
        "actions": ["pytest ./src/test_load_bases_data.py"],
        "file_dep": [
            "./src/load_bases_data.py",
            DATA_DIR / "reference.csv"
        ],
        "clean": True,
    }


def task_latex_to_document():
    return {
        'actions': ['python src/latex_to_document.py reports/Final_Report.tex'],
        'file_dep': ['src/latex_to_document.py', 'reports/Final_Report.tex'],
        'targets': ['Final_Report.pdf'],
        'clean': True,
    }

#  import sys
# from os import environ
# from pathlib import Path
# import shutil

# # Add our src/ directory to Python’s import path
# sys.path.insert(1, "./src/")

# # Local imports
# from settings import config

# ############################################
# # Directories and Environment Configuration
# ############################################

# DATA_DIR = Path(config("DATA_DIR"))
# OUTPUT_DIR = Path(config("OUTPUT_DIR"))
# OS_TYPE = config("OS_TYPE")

# # Prevent Jupyter from complaining about file path validations
# environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"

# ############################################
# # Helper Functions for Jupyter Notebook Tasks
# ############################################

# def jupyter_execute_notebook(notebook):
#     """
#     Execute a Jupyter notebook in-place (overwrites the same .ipynb).
#     Clears metadata to avoid spurious changes in version control.
#     """
#     return (
#         f"jupyter nbconvert --execute --to notebook "
#         f"--ClearMetadataPreprocessor.enabled=True --log-level WARN "
#         f"--inplace ./src/{notebook}.ipynb"
#     )

# def jupyter_to_html(notebook, output_dir=OUTPUT_DIR):
#     """
#     Convert a Jupyter notebook to an HTML file in the specified output directory.
#     """
#     return (
#         f"jupyter nbconvert --to html --log-level WARN "
#         f"--output-dir={output_dir} ./src/{notebook}.ipynb"
#     )

# def jupyter_to_md(notebook, output_dir=OUTPUT_DIR):
#     """
#     Convert a Jupyter notebook to a Markdown file (requires jupytext),
#     placing the output in the specified output directory.
#     """
#     return (
#         f"jupytext --to markdown --log-level WARN "
#         f"--output-dir={output_dir} ./src/{notebook}.ipynb"
#     )

# def jupyter_to_python(notebook, build_dir):
#     """
#     Convert a Jupyter notebook to a Python script. The script is named
#     _{notebook}.py and placed in build_dir.
#     """
#     return (
#         f"jupyter nbconvert --log-level WARN --to python ./src/{notebook}.ipynb "
#         f"--output _{notebook}.py --output-dir {build_dir}"
#     )

# def jupyter_clear_output(notebook):
#     """
#     Clear output cells and metadata from a Jupyter notebook, modifying it in place.
#     """
#     return (
#         f"jupyter nbconvert --log-level WARN "
#         f"--ClearOutputPreprocessor.enabled=True "
#         f"--ClearMetadataPreprocessor.enabled=True "
#         f"--inplace ./src/{notebook}.ipynb"
#     )

# def copy_file(origin_path, destination_path, mkdir=True):
#     """
#     Return an action function that copies a file from origin_path to destination_path.
#     If mkdir=True, it creates parent directories for the destination if needed.
#     """
#     def _copy_file():
#         origin = Path(origin_path)
#         dest = Path(destination_path)
#         if mkdir:
#             dest.parent.mkdir(parents=True, exist_ok=True)
#         shutil.copy2(origin, dest)
#     return _copy_file


# ############################################
# # PyDoit Tasks
# ############################################

# def task_config():
#     """
#     Create empty directories for data and output if they don't exist,
#     and run settings.py to confirm configuration is valid.
#     """
#     return {
#         "actions": ["ipython ./src/settings.py"],
#         "targets": [DATA_DIR, OUTPUT_DIR],
#         "file_dep": ["./src/settings.py"],
#     }



# def task_pull_fed_yield_curve():
#     """
#     Example of a data pulling task from WRDS (currently commented out).
#     This should pull CRSP treasury data and save it in parquet files.
#     """
#     file_dep = ["./src/pull_fed_yield_curve.py"]
#     file_output = [
#         "fed_yield_curve_all.parquet",
#         "fed_yield_curve.parquet",
#     ]
#     targets = [DATA_DIR / file for file in file_output]

#     return {
#         "actions": ["ipython ./src/pull_fed_yield_curve.py"],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": True,
#     }

# def task_pull_treasury_data():
#     """
#     Example of a data pulling task from WRDS (currently commented out).
#     This should pull CRSP treasury data and save it in parquet files.
#     """
#     file_dep = ["./src/pull_treasury_data.py"]
#     file_output = [
#         "TFZ_DAILY.parquet",
#         "TFZ_INFO.parquet",
#         "TFZ_consolidated.parquet",
#         "TFZ_with_runness.parquet",
#     ]
#     targets = [DATA_DIR / file for file in file_output]

#     return {
#         "actions": ["ipython ./src/pull_treasury_data.py"],
#         "targets": targets,
#         "file_dep": file_dep,
#         "clean": True,
#     }

# # if __name__ == "__main__":
# #     # 1) Pull daily quotes
# #     df = pull_CRSP_treasury_daily(
# #         start_date="1970-01-01",
# #         end_date="2023-12-31",
# #         wrds_username=WRDS_USERNAME,
# #     )
# #     path = DATA_DIR / "TFZ_DAILY.parquet"
# #     df.to_parquet(path)

# #     # 2) Pull issue metadata
# #     df = pull_CRSP_treasury_info(wrds_username=WRDS_USERNAME)
# #     path = DATA_DIR / "TFZ_INFO.parquet"
# #     df.to_parquet(path)

# #     # 3) Pull and merge quotes + metadata
# #     df = pull_CRSP_treasury_consolidated(wrds_username=WRDS_USERNAME)
# #     path = DATA_DIR / "TFZ_consolidated.parquet"
# #     df.to_parquet(path)

# #     # 4) Runness calculation
# #     df = calc_runness(df)
# #     path = DATA_DIR / "TFZ_with_runness.parquet"
# #     df.to_parquet(path)


# ############################################
# # Notebook Tasks
# ############################################

# notebook_tasks = {
#     "01_explore_basis_trade_data.ipynb": {
#         "file_dep": [],
#         "targets": [],
#     },
#     "02_intro_treasury_futures.ipynb": {
#         "file_dep": [],
#         "targets": [],
#     },
#     "03_replicate_pca_basis.ipynb": {
#         "file_dep": [],
#         "targets": [],
#     },
# }


# def task_convert_notebooks_to_scripts():
#     """
#     Convert notebooks to Python scripts to detect changes in notebook logic
#     rather than metadata. Also clear notebook outputs first.
#     """
#     build_dir = Path(OUTPUT_DIR)
#     for notebook in notebook_tasks.keys():
#         notebook_name = notebook.rsplit(".", 1)[0]
#         yield {
#             "name": notebook,
#             "actions": [
#                 jupyter_clear_output(notebook_name),
#                 jupyter_to_python(notebook_name, build_dir),
#             ],
#             "file_dep": [Path("./src") / notebook],
#             "targets": [OUTPUT_DIR / f"_{notebook_name}.py"],
#             "clean": True,
#             "verbosity": 0,
#         }


# def task_run_notebooks():
#     """
#     Execute Jupyter notebooks, convert them to HTML for review,
#     and copy the executed notebooks into the OUTPUT_DIR.
#     """
#     for notebook in notebook_tasks.keys():
#         notebook_name = notebook.rsplit(".", 1)[0]
#         yield {
#             "name": notebook,
#             "actions": [
#                 # Print start time
#                 """python -c "import sys; from datetime import datetime; print(f'Start """ 
#                 + notebook 
#                 + """: {datetime.now()}', file=sys.stderr)" """,

#                 jupyter_execute_notebook(notebook_name),
#                 jupyter_to_html(notebook_name),
#                 copy_file(
#                     Path("./src") / notebook,
#                     OUTPUT_DIR / notebook,
#                     mkdir=True
#                 ),
#                 jupyter_clear_output(notebook_name),

#                 # Print end time
#                 """python -c "import sys; from datetime import datetime; print(f'End """ 
#                 + notebook 
#                 + """: {datetime.now()}', file=sys.stderr)" """,
#             ],
#             "file_dep": [
#                 OUTPUT_DIR / f"_{notebook_name}.py",
#                 *notebook_tasks[notebook]["file_dep"],
#             ],
#             "targets": [
#                 OUTPUT_DIR / f"{notebook_name}.html",
#                 OUTPUT_DIR / notebook,
#                 *notebook_tasks[notebook]["targets"],
#             ],
#             "clean": True,
#         }


# ############################################
# # Sphinx Documentation
# ############################################

# # Pages generated by Sphinx that we consider final "targets":
# notebook_sphinx_pages = [
#     f"./_docs/_build/html/notebooks/{nb.split('.')[0]}.html"
#     for nb in notebook_tasks.keys()
# ]
# sphinx_targets = [
#     "./_docs/_build/html/index.html",
#     *notebook_sphinx_pages,
# ]


# def copy_docs_src_to_docs():
#     """
#     Copy all files/subdirectories from docs_src to _docs,
#     preserving directory structure. Also copies notebooks from OUTPUT_DIR to
#     _docs/notebooks, and any assets from src/assets to _docs/notebooks/assets.
#     """
#     src = Path("docs_src")
#     dst = Path("_docs")

#     # Ensure the destination directory exists
#     dst.mkdir(parents=True, exist_ok=True)

#     # Loop through all files and directories in docs_src
#     for item in src.rglob("*"):
#         relative_path = item.relative_to(src)
#         target = dst / relative_path
#         if item.is_dir():
#             target.mkdir(parents=True, exist_ok=True)
#         else:
#             target.parent.mkdir(parents=True, exist_ok=True)
#             shutil.copy2(item, target)

#     # Copy notebooks from OUTPUT_DIR to _docs/notebooks
#     docs_notebooks = Path("./_docs/notebooks")
#     docs_notebooks.mkdir(parents=True, exist_ok=True)
#     for notebook in notebook_tasks.keys():
#         notebook_path = OUTPUT_DIR / notebook
#         if notebook_path.exists():
#             shutil.copy2(notebook_path, docs_notebooks / notebook)

#     # Copy assets from src/assets to _docs/notebooks/assets
#     src_assets = Path("./src/assets")
#     docs_assets = Path("./_docs/notebooks/assets")
#     if src_assets.exists():
#         docs_assets.mkdir(parents=True, exist_ok=True)
#         for asset_item in src_assets.rglob("*"):
#             relative_path = asset_item.relative_to(src_assets)
#             target = docs_assets / relative_path
#             if asset_item.is_dir():
#                 target.mkdir(parents=True, exist_ok=True)
#             else:
#                 target.parent.mkdir(parents=True, exist_ok=True)
#                 shutil.copy2(asset_item, target)


# def copy_docs_build_to_docs():
#     """
#     Copy all files/subdirectories from _docs/_build/html to docs, preserving structure.
#     Create an empty .nojekyll file in the docs directory for GitHub Pages.
#     """
#     src = Path("_docs/_build/html")
#     dst = Path("docs")
#     dst.mkdir(parents=True, exist_ok=True)

#     for item in src.rglob("*"):
#         relative_path = item.relative_to(src)
#         target = dst / relative_path
#         if item.is_dir():
#             target.mkdir(parents=True, exist_ok=True)
#         else:
#             target.parent.mkdir(parents=True, exist_ok=True)
#             shutil.copy2(item, target)

#     # Create empty .nojekyll file
#     (dst / ".nojekyll").touch()


# def task_compile_sphinx_docs():
#     """
#     Compile Sphinx documentation. First copies doc sources,
#     builds the HTML, then copies the built files to the docs/ folder.
#     """
#     notebook_scripts = [
#         OUTPUT_DIR / ("_" + nb.split(".")[0] + ".py") for nb in notebook_tasks.keys()
#     ]
#     file_dep = [
#         "./docs_src/conf.py",
#         "./docs_src/index.md",
#         *notebook_scripts,
#     ]

#     return {
#         "actions": [
#             copy_docs_src_to_docs,
#             "sphinx-build -M html ./_docs/ ./_docs/_build",
#             copy_docs_build_to_docs,
#         ],
#         "targets": sphinx_targets,
#         "file_dep": file_dep,
#         "task_dep": ["run_notebooks"],
#         "clean": True,
#     }
