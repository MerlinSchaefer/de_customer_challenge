import warnings
import pandas as pd

def pytest_sessionstart(session):
    warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
