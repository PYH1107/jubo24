from dataclasses import dataclass
from operator import itemgetter
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from pyod.models.iforest import IForest
from pyod.models.knn import KNN
from pyod.models.kde import KDE
from pyod.models.gmm import GMM
from pyod.models.lof import LOF
from pyod.models.ocsvm import OCSVM
from pyod.models.ecod import ECOD
from pyod.models.mad import MAD
from sktime.forecasting.arima import AutoARIMA
from linkinpark.lib.ai.bloodsugar.utils import Utils


@dataclass
class BollingerBand:
    window_size: int
    exponential: bool = False

    def fit(self, history):
        history = pd.Series(history.flatten())
        if self.exponential:
            alpha = 2 / (len(history) + 1)
            self.moving_average = history.ewm(
                alpha=alpha, min_periods=1).mean().values[-1]
            self.moving_std = history.ewm(
                alpha=alpha, min_periods=1).std().values[-1]
        else:
            self.moving_average = history.rolling(
                window=self.window_size, min_periods=1).mean().values[-1]
            self.moving_std = history.rolling(
                window=self.window_size, min_periods=1).std().values[-1]
        if self.moving_std == np.nan:  # moving_std is always nan for the first instance
            self.moving_std = 0

    def predict(self, lastest_data):
        return 0 if abs(lastest_data - self.moving_average) <= 2 * self.moving_std else 1


@dataclass
class AnomalyARIMA():
    def __post_init__(self):
        self.model = AutoARIMA(suppress_warnings=True)

    def fit(self, history):
        history = np.array(history)
        self.model.fit(history)

    def predict(self, latest_data):
        forecasted_value = self.model.predict(fh=[1])[0]
        residuals = self.model.predict_residuals(np.array(self.model._y))
        return 0 if abs(latest_data - forecasted_value) <= residuals.mean() + 2 * residuals.std() else 1


@dataclass
class AnomalyDetector:
    model_name: str = 'ecod'
    min_data: int = 5
    neighbor_service_url: str = "https://patient-knowledge-graph-ge6dae6qzq-de.a.run.app/neighbors"

    def __post_init__(self):
        self.utils = Utils(neighbor_service_url=self.neighbor_service_url)
        bollinger_band = BollingerBand(window_size=10)
        mad = MAD(threshold=2)
        arima = AnomalyARIMA()
        iforest = IForest()
        ocsvm = OCSVM()
        lof = LOF()
        ecod = ECOD(contamination=0.14)
        knn = KNN(n_neighbors=3)
        gmm = GMM()
        kde = KDE()

        self.standardization_required = [
            'mad', 'lof', 'iforest', 'ocsvm', 'ecod', 'knn', 'gmm', 'kde'
        ]
        name_mapping = {
            'bollinger_band': bollinger_band,
            'mad': mad,
            'arima': arima,
            'iforest': iforest,
            'ocsvm': ocsvm,
            'lof': lof,
            'ecod': ecod,
            'knn': knn,
            'gmm': gmm,
            'kde': kde
        }
        self.model = name_mapping[self.model_name]

    def rule_based_detect(self, has_diabetes, sugar_type, latest_data):
        low = {(False, 'AC'): 70, (True, 'AC'): 80,
               (False, 'PC'): 70, (True, 'PC'): 80}
        high = {(False, 'AC'): 99, (True, 'AC'): 130,
                (False, 'PC'): 140, (True, 'PC'): 180}
        return 0 if low[(has_diabetes, sugar_type)] <= latest_data <= high[(has_diabetes, sugar_type)] else 1

    def model_based_detect(self, history, latest_data):
        history = pd.DataFrame(history).values

        if self.model_name in self.standardization_required:
            scaler = StandardScaler()
            history = scaler.fit_transform(history)
            latest_data = scaler.transform([[latest_data]])

        self.model.fit(history)
        prediction = self.model.predict(latest_data)
        return int(prediction[-1]) if isinstance(prediction, np.ndarray) else int(prediction)

    def detect(self, instance):
        patient, sugar_type, latest_data = itemgetter(
            'patient', 'sugar_type', 'sugar_value')(instance)
        history = self.utils.get_history_data(patient, sugar_type)
        if len(history) < self.min_data:
            history = self.utils.get_similar_patients_data(patient, sugar_type)
            if len(history) < self.min_data:
                return self.rule_based_detect(self.utils.check_diabetes(patient), sugar_type, latest_data)
        return self.model_based_detect(history, latest_data)
