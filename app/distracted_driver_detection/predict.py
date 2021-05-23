import os
import numpy as np
import cv2
from models import *


def load_model():
    model = create_model_v2()
    model.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['accuracy'])
    model_path = 'saved_models/weights_best_vanilla.hdf5'
    assert os.path.exists(model_path)
    # load weights from model
    model.load_weights(model_path)
    return model


def predict(model, img_matrix):
    img_rows = 64
    img_cols = 64
    color_type = 1
    batch_size = 40
    # nb_epoch = 10

    img_brute = cv2.resize(img_matrix, (img_rows, img_cols))
    new_img = img_brute.reshape(-1, img_rows, img_cols, color_type)
    y_prediction = model.predict(new_img, batch_size=batch_size, verbose=0)
    return y_prediction