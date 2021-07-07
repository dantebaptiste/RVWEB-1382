import pandas as pd
import my_globals


def load_the_mini_panda_db_from_file():
    fileName = my_globals.str_dir4_pandas_miniDB_of_vids + my_globals.str_filename_pandas_miniDB_of_vids
    df = pd.read_csv(fileName, sep='\t')
    df.set_index('ID', drop=False, inplace=True)
    return df
# ------------------------ END FUNCTION ------------------------ #
