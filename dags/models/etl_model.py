from mlflow.pyfunc import PythonModel
import pandas as pd


class ETL_Model(PythonModel):
    # Данный метод вычитывает csv файл, который был до обработки.
    # После чего сравнивает его по количеству строк с файлом после обработки.

    def predict(self, context, model_input) -> bool:
        old_df = pd.read_csv("input.csv", delimiter=';', header=None)
        old_df.columns = ['c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'c11']
        old_df_shape = old_df.dropna(subset=['c1']).shape

        return model_input.shape == old_df_shape
