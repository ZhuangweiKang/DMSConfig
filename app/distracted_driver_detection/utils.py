import cv2
import numpy as np

def normal(file):
    # Color type: 1 - grey, 3 - rgb
    img_rows = 64
    img_cols = 64
    color_type = 1
    img = get_cv2_image(file, img_rows, img_cols, color_type)

    return img


def get_cv2_image(path, img_rows, img_cols, color_type=3):
    # Loading as Grayscale image
    if color_type == 1:
        img = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
    elif color_type == 3:
        img = cv2.imread(path, cv2.IMREAD_COLOR)
    # Reduce size
    img = cv2.resize(img, (img_rows, img_cols))
    return img


def process_metrics(data):
    if len(data) > 0:
        _min = np.min(data)
        _25th, _50th, _90th = np.quantile(data, [0.25, 0.5, 0.9])
        _mean = np.mean(data)
        _max = np.max(data)
        return [_min, _25th, _50th, _90th, _mean, _max]
    else:
        return []