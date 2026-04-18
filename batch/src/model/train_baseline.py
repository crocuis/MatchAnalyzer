from collections import Counter

from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier


def train_baseline_model(features, labels):
    class_counts = Counter(labels)
    if any(count < 3 for count in class_counts.values()):
        raise ValueError(
            "train_baseline_model requires at least 3 samples per class for isotonic calibration"
        )

    estimator = HistGradientBoostingClassifier(random_state=7)
    model = CalibratedClassifierCV(estimator, method="isotonic", cv=3)
    model.fit(features, labels)
    return model
