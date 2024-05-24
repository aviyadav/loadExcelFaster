import pandas as pd
import numpy as np
from joblib import Parallel, delayed
import time

def create_test_data():
    for file_number in range(10):
        values = np.random.uniform(size=(20000, 25))
        pd.DataFrame(values).to_csv(f"Dummy {file_number}.csv")
        pd.DataFrame(values).to_excel(f"Dummy {file_number}.xlsx")
        pd.DataFrame(values).to_pickle(f"Dummy {file_number}.pickle")

def loading_excel_files_pandas():
    start = time.time()
    df = pd.read_excel("Dummy 0.xlsx")
    for file_number in range (1, 10):
        df._append(pd.read_excel(f"Dummy {file_number}.xlsx"))
    end = time.time()

    print("Excel: ", end - start) # 48.90182542800903

def smarter_pandas_dataframes():
    start = time.time()

    df = []
    for file_number in range(10):
        temp = pd.read_csv(f"Dummy {file_number}.csv")
        df.append(temp)

    df = pd.concat(df, ignore_index=True)

    end = time.time()
    print("CSV2: ", end - start)  # 1.176011562347412


def parallelize_csv_imports_with_joblib():
    start = time.time()

    def loop(file_number):
        return pd.read_csv(f"Dummy {file_number}.csv")

    df = Parallel(n_jobs=-1, verbose=10)(delayed(loop)(file_number) for file_number in range(10))
    df = pd.concat(df, ignore_index=True)

    end = time.time()
    print("CSV//: ", end - start)  # 3.927673816680908


def simple_parallelization_python_with_joblib():
    start = time.time()

    def loop(file_number):
        return pd.read_csv(f"Dummy {file_number}.csv")

    df = Parallel(n_jobs=-1, verbose=10)(delayed(loop)(file_number) for file_number in range(10))

    # equivalent to
    df = [loop(file_number) for file_number in range(10)]

    end = time.time()
    print("simplified CSV//: ", end - start)  # 4.7370030879974365


def utilize_pickle_files():
    start = time.time()

    def loop(file_number):
        return pd.read_pickle(f"Dummy {file_number}.pickle")

    df = Parallel(n_jobs=-1, verbose=10)(delayed(loop)(file_number) for file_number in range(10))
    df = pd.concat(df, ignore_index=True)
    end = time.time()
    print("Pickle//:", end - start) # 3.655803680419922


def loading_excel_files_parallel():
    start = time.time()

    def loop(file_number):
        return pd.read_excel(f"Dummy {file_number}.xlsx")

    df = Parallel(n_jobs=-1, verbose=10)(delayed(loop)(file_number) for file_number in range(10))
    df = pd.concat(df, ignore_index=True)
    end = time.time()
    print("Excel//:", end - start) # 30.71082878112793


if __name__ == '__main__':
    # create_test_data()
    # loading_excel_files_pandas()
    # smarter_pandas_dataframes()
    # parallelize_csv_imports_with_joblib()
    # simple_parallelization_python_with_joblib()
    # utilize_pickle_files()
    loading_excel_files_parallel()

