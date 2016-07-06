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
