r"""
You can test out the latex code in the following minimal working
example document:

\documentclass{article}
\usepackage{booktabs}
\begin{document}
First document. This is a simple example, with no 
extra parameters or packages included.

\begin{table}
\centering
YOUR LATEX TABLE CODE HERE
%\input{example_table.tex}
\end{table}
\end{document}

"""
import pandas as pd
import numpy as np
np.random.seed(100)

from settings import config
from pathlib import Path
DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))


## Suppress scientific notation and limit to 3 decimal places
# Sets display, but doesn't affect formatting to LaTeX
pd.set_option('display.float_format', lambda x: '%.2f' % x)
# Sets format for printing to LaTeX
float_format_func = lambda x: '{:.2f}'.format(x)


df = pd.DataFrame(np.random.random((5, 5)))
latex_table_string = df.to_latex(float_format=float_format_func)
print(latex_table_string)

path = OUTPUT_DIR / f'pandas_to_latex_simple_table1.tex'
with open(path, "w") as text_file:
    text_file.write(latex_table_string)






"""
This module loads the S&P 500 index, Dividend yields, and all active futures during 
the given period from Bloomberg. 

You must have a Bloomberg terminal open on this computer to run. You must
first install xbbg
"""

import pandas as pd
import settings
from pathlib import Path

DATA_DIR = settings.DATA_DIR
START_DATE = settings.START_DATE
END_DATE = settings.END_DATE

def pull_bbg_data(end_date=END_DATE):
    
    bbg_df = pd.DataFrame()
    bbg_df['dividend yield'] = blp.bdh("SPX Index","EQY_DVD_YLD_12m", START_DATE, end_date)[("SPX Index","EQY_DVD_YLD_12m")]
    
    bbg_df['index'] = blp.bdh("SPX Index","px_last", START_DATE, end_date)[("SPX Index","px_last")]
    
    bbg_df['futures'] = pd.concat([blp.bdh("SP1 Index","px_last", START_DATE, "1997-08-31")[("SP1 Index","px_last")],
                                    blp.bdh("ES1 Index","px_last", "1997-09-30", end_date)[("ES1 Index","px_last")]])
    
    bbg_df.index.name = 'Date'

    return bbg_df


if __name__ == "__main__":
    from xbbg import blp
    df = pull_bbg_data(end_date=END_DATE)
    path = Path(DATA_DIR) / "bloomberg.parquet"
    df.to_parquet(path)
