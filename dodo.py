import os
import sys
from os import environ
from pathlib import Path

sys.path.append(os.path.abspath("./src"))

from settings import config

############################################
# Directories and Environment Configuration
############################################

BASE_DIR = Path(config("BASE_DIR"))
DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
OS_TYPE = config("OS_TYPE")

# Prevent Jupyter from complaining about file path validations
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"


## Helpers for handling Jupyter Notebook tasks
# fmt: off
## Helper functions for automatic execution of Jupyter notebooks
def jupyter_execute_notebook(notebook):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --log-level WARN --inplace ./src/{notebook}.ipynb"
def jupyter_to_html(notebook, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --log-level WARN --output-dir={output_dir} ./src/{notebook}.ipynb"
def jupyter_to_md(notebook, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --log-level WARN --output-dir={output_dir} ./src/{notebook}.ipynb"
def jupyter_to_python(notebook, build_dir):
    """Convert a notebook to a python script"""
    return f"jupyter nbconvert --log-level WARN --to python ./src/{notebook}.ipynb --output _{notebook}.py --output-dir {build_dir}"
def jupyter_clear_output(notebook):
    return f"jupyter nbconvert --log-level WARN --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace ./src/{notebook}.ipynb"
# fmt: on


def copy_notebook_to_folder(notebook_stem, origin_folder, destination_folder):
    origin_path = Path(origin_folder) / f"{notebook_stem}.ipynb"
    destination_folder = Path(destination_folder)
    destination_folder.mkdir(parents=True, exist_ok=True)
    destination_path = destination_folder / f"{notebook_stem}.ipynb"
    if OS_TYPE == "nix":
        command = f"cp {origin_path} {destination_path}"
    else:
        command = f"copy  {origin_path} {destination_path}"
    return command


############################################
# Task Definitions
############################################


def task_clean_raw():
    """Run `clean_raw.ipynb` to generate necessary datasets in `_data`."""
    return {
        "actions": ["python ./src/clean_raw.py"],
        "file_dep": ["./src/clean_raw.py"],
        "targets": [
            DATA_DIR / "treasury_df.csv",
            DATA_DIR / "ois_df.csv",
            DATA_DIR / "last_day_df.csv",
        ],
        "clean": True,
    }


def task_generate_reference():
    """Run generate_reference.py to create reference.csv"""
    return {
        "actions": ["python ./src/generate_reference.py"],
        "file_dep": ["./src/generate_reference.py"],
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
            DATA_DIR / "last_day_df.csv",
        ],
        "targets": [DATA_DIR / "treasury_sf_output.csv"],
        "clean": True,
    }


def task_process_treasury_data_notebook():
    """Optionally run `process_treasury_data.ipynb` if needed."""
    return {
        "actions": ["python ./src/process_treasury_data.py"],
        "file_dep": [
            "./src/process_treasury_data.py",
            DATA_DIR / "treasury_sf_output.csv",
        ],
        "clean": True,
    }


notebook_tasks = {
    "01_explore_basis_trade_data_new.ipynb": {
        "file_dep": [DATA_DIR / "treasury_sf_output.csv"],
        "targets": [],
    },
}


def task_convert_notebooks_to_scripts():
    """Convert notebooks to script form to detect changes to source code rather
    than to the notebook's metadata.
    """
    build_dir = Path(OUTPUT_DIR)
    build_dir.mkdir(parents=True, exist_ok=True)

    for notebook in notebook_tasks.keys():
        notebook_name = notebook.split(".")[0]
        yield {
            "name": notebook,
            "actions": [
                # jupyter_execute_notebook(notebook_name),
                # jupyter_to_html(notebook_name),
                # copy_notebook_to_folder(notebook_name, Path("./src"), "./docs_src/notebooks/"),
                jupyter_clear_output(notebook_name),
                jupyter_to_python(notebook_name, build_dir),
            ],
            "file_dep": [Path("./src") / notebook],
            "targets": [OUTPUT_DIR / f"_{notebook_name}.py"],
            "clean": True,
            "verbosity": 0,
        }


# fmt: off
def task_run_notebooks():
    """Preps the notebooks for presentation format.
    Execute notebooks if the script version of it has been changed.
    """

    for notebook in notebook_tasks.keys():
        notebook_name = notebook.split(".")[0]
        yield {
            "name": notebook,
            "actions": [
                """python -c "import sys; from datetime import datetime; print(f'Start """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
                jupyter_execute_notebook(notebook_name),
                jupyter_to_html(notebook_name),
                copy_notebook_to_folder(
                    notebook_name, Path("./src"), "./_docs/notebooks/"
                ),
                jupyter_clear_output(notebook_name),
                # jupyter_to_python(notebook_name, build_dir),
                """python -c "import sys; from datetime import datetime; print(f'End """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
            ],
            "file_dep": [
                OUTPUT_DIR / f"_{notebook_name}.py",
                *notebook_tasks[notebook]["file_dep"],
            ],
            "targets": [
                OUTPUT_DIR / f"{notebook_name}.html",
                BASE_DIR / "_docs" / "notebooks" / f"{notebook_name}.ipynb",
                *notebook_tasks[notebook]["targets"],
            ],
            "clean": True,
            # "verbosity": 1,
        }
# fmt: on

# def task_test_calc_treasury_data():
#     """Run unit tests for calc_treasury_data.py"""
#     return {
#         "actions": ["pytest ./src/test_calc_treasury_data.py"],
#         "file_dep": [
#             "./src/calc_treasury_data.py",
#             DATA_DIR / "treasury_sf_output.csv",
#             DATA_DIR / "reference.csv",
#         ],
#         "task_dep": ["generate_reference"],  # Ensure reference.csv exists
#         "clean": True,
#     }


# def task_test_load_bases_data():
#     """Run unit tests for `load_bases_data.py`."""
#     return {
#         "actions": ["pytest ./src/test_load_bases_data.py"],
#         "file_dep": ["./src/load_bases_data.py", DATA_DIR / "reference.csv"],
#         "clean": True,
#     }


def task_latex_to_document():
    return {
        "actions": ["python src/latex_to_document.py reports/Final_Report.tex"],
        "file_dep": ["src/latex_to_document.py", "reports/Final_Report.tex"],
        "targets": ["Final_Report.pdf"],
        "clean": True,
    }
