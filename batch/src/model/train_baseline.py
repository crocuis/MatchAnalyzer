from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier


def train_baseline_model(features, labels):
    estimator = HistGradientBoostingClassifier(random_state=7)
    model = CalibratedClassifierCV(estimator, method="isotonic", cv=3)
    model.fit(features, labels)
    return model
