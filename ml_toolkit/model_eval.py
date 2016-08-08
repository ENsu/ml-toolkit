import numpy as np
import ml_metrics


# model selection
def add_noise(df, columns, noise_rate):
    size = int(len(df.index) * noise_rate)
    for c in columns:
        random_sample_index = np.random.choice(df.index, size, replace=False)
        index_tobe_shuffle = np.copy(random_sample_index)
        np.random.shuffle(index_tobe_shuffle)
        df.loc[random_sample_index, c] = df.loc[index_tobe_shuffle, c]
    return df


def get_predict_label_list(prob_matrix, class_label, len=5):
    result_list = []
    for v in prob_matrix:
        sort_indices = np.argsort(v)[::-1]
        result_list.append(class_label[sort_indices][:len])
    return np.array(result_list)


def mapk(truth, predict, k=5):
    count = 0
    sum = 0
    for i, v in enumerate(truth):
        sum = sum + ml_metrics.apk([v], predict[i], k)
        count = count + 1
    return float(sum)/count


def multiclass_log_loss_by_row(y_true, y_pred, eps=1e-15):
    predictions = np.clip(y_pred, eps, 1 - eps)

    # normalize row sums to 1
    predictions /= predictions.sum()

    actual = np.zeros(y_pred.shape)
    actual[y_true] = 1
    return -1.0 * np.dot(actual, np.log(predictions))


def multiclass_log_loss(y_true, y_pred, eps=1e-15):
    """Multi class version of Logarithmic Loss metric, ref from 
    https://www.kaggle.com/c/predict-closed-questions-on-stack-overflow/forums/t/2644/multi-class-log-loss-function

    Parameters
    ----------
    y_true : array, shape = [n_samples] the id of right samples
    y_pred : array, shape = [n_samples, n_classes]

    Returns
    -------
    loss : float
    
    test:
    multiclass_log_loss(np.array([0,1,2]),np.array([[1,0,0],[0,1,0],[0,0,1]]))
    >> 2.10942374679e-15
    multiclass_log_loss(np.array([0,1,2]),np.array([[1,1,1],[0,1,0],[0,0,1]]))
    >> 0.366204096223
    """
    predictions = np.clip(y_pred, eps, 1 - eps)

    # normalize row sums to 1
    predictions /= predictions.sum(axis=1)[:, np.newaxis]

    actual = np.zeros(y_pred.shape)
    rows = actual.shape[0]
    actual[np.arange(rows), y_true.astype(int)] = 1
    vsota = np.sum(actual * np.log(predictions))
    return -1.0 / rows * vsota
