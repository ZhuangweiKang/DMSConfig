import os
from glob import glob
import time

os.environ['KERAS_BACKEND'] = 'tensorflow'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # 3 = INFO, WARNING, and ERROR messages are not printed

from tqdm import tqdm
import pandas as pd
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings('ignore')
import matplotlib.image as mpimg

from sklearn.model_selection import train_test_split
from keras.utils import np_utils
from sklearn.metrics import log_loss
from keras.callbacks import ModelCheckpoint, EarlyStopping

from models import *
from utils import *

# Load the dataset previously downloaded from Kaggle
NUMBER_CLASSES = 10

activity_map = {'c0': 'Safe driving',
                'c1': 'Texting - right',
                'c2': 'Talking on the phone - right',
                'c3': 'Texting - left',
                'c4': 'Talking on the phone - left',
                'c5': 'Operating the radio',
                'c6': 'Drinking',
                'c7': 'Reaching behind',
                'c8': 'Hair and makeup',
                'c9': 'Talking to passenger'}


# Training
def load_train(img_rows, img_cols, color_type=3):
    start_time = time.time()
    train_images = []
    train_labels = []
    pwd = os.getcwd()
    # Loop over the training folder
    for classed in tqdm(range(NUMBER_CLASSES)):
        print('Loading directory c{}'.format(classed))
        files = glob(os.path.join('data', 'imgs', 'train', 'c' + str(classed), '*.jpg'))
        # print(files)
        for file in files:
            img = get_cv2_image(file, img_rows, img_cols, color_type)

            train_images.append(img)
            train_labels.append(classed)
    print("Data Loaded in {} second".format(time.time() - start_time))
    return train_images, train_labels


def read_and_normalize_train_data(img_rows, img_cols, color_type):
    X, labels = load_train(img_rows, img_cols, color_type)
    y = np_utils.to_categorical(labels, 10)
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    x_train = np.array(x_train, dtype=np.uint8).reshape(-1, img_rows, img_cols, color_type)
    x_test = np.array(x_test, dtype=np.uint8).reshape(-1, img_rows, img_cols, color_type)

    return x_train, x_test, y_train, y_test


# Validation
def load_test(size=200000, img_rows=64, img_cols=64, color_type=3):
    path = os.path.join('data', 'imgs', 'test', '*.jpg')
    files = sorted(glob(path))
    X_test, X_test_id = [], []
    total = 0
    files_size = len(files)
    for file in tqdm(files):
        if total >= size or total >= files_size:
            break
        file_base = os.path.basename(file)
        img = get_cv2_image(file, img_rows, img_cols, color_type)
        X_test.append(img)
        X_test_id.append(file_base)
        total += 1
    return X_test, X_test_id


def read_and_normalize_sampled_test_data(size, img_rows, img_cols, color_type=3):
    test_data, test_ids = load_test(size, img_rows, img_cols, color_type)

    test_data = np.array(test_data, dtype=np.uint8)
    test_data = test_data.reshape(-1, img_rows, img_cols, color_type)

    return test_data, test_ids


def plot_train_history(history):
    # Summarize history for accuracy
    plt.plot(history.history['accuracy'])
    plt.plot(history.history['val_accuracy'])
    plt.title('Model accuracy')
    plt.ylabel('accuracy')
    plt.xlabel('epoch')
    plt.legend(['train', 'test'], loc='upper left')
    plt.show()

    # Summarize history for loss
    plt.plot(history.history['loss'])
    plt.plot(history.history['val_loss'])
    plt.title('Model loss')
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(['train', 'test'], loc='upper left')
    plt.show()


def plot_test_class(model, test_files, image_number, color_type=1):
    img_brute = test_files[image_number]
    img_brute = cv2.resize(img_brute, (img_rows, img_cols))
    # plt.imshow(img_brute, cmap='gray')

    new_img = img_brute.reshape(-1, img_rows, img_cols, color_type)

    y_prediction = model.predict(new_img, batch_size=batch_size, verbose=1)
    print('Y prediction: {}'.format(y_prediction))
    print('Predicted: {}'.format(activity_map.get('c{}'.format(np.argmax(y_prediction)))))
    print(format(activity_map.get('c{}'.format(np.argmax(y_prediction)))))
    print('c{}'.format(np.argmax(y_prediction)))
    plt.show()
    print('Predicted: {}'.format(
        'c{}'.format(np.argmax(y_prediction)) + ' - ' + activity_map.get('c{}'.format(np.argmax(y_prediction)))))


if __name__ == '__main__':
    # import the .csv file to read the labels
    dataset = pd.read_csv('data/driver_imgs_list.csv')

    by_drivers = dataset.groupby('subject')
    unique_drivers = by_drivers.groups.keys()

    img_rows = 64
    img_cols = 64
    color_type = 1

    x_train, x_test, y_train, y_test = read_and_normalize_train_data(img_rows, img_cols, color_type)
    nb_test_samples = 200
    test_files, test_targets = read_and_normalize_sampled_test_data(nb_test_samples, img_rows, img_cols, color_type)

    image_count = 1
    BASE_URL = 'data/imgs/train/'
    for directory in os.listdir(BASE_URL):
        if directory[0] != '.':
            for i, file in enumerate(os.listdir(BASE_URL + directory)):
                if i == 1:
                    break
                else:
                    fig = plt.subplot(5, 2, image_count)
                    image_count += 1
                    image = mpimg.imread(BASE_URL + directory + '/' + file)

    batch_size = 40
    nb_epoch = 10

    models_dir = "saved_models"
    if not os.path.exists(models_dir):
        os.makedirs(models_dir)

    checkpointer = ModelCheckpoint(filepath='saved_models/weights_best_vanilla.hdf5',
                                   monitor='val_loss', mode='min',
                                   verbose=1, save_best_only=True)
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=2)
    callbacks = [checkpointer, es]
    model_v2 = create_model_v2()

    model_v2.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['accuracy'])
    # Training the Vanilla Model
    history_v2 = model_v2.fit(x_train, y_train, validation_data=(x_test, y_test), callbacks=callbacks, epochs=nb_epoch,
                              batch_size=batch_size, verbose=1)

    # load model weight
    model_v2.load_weights('saved_models/weights_best_vanilla.hdf5')

    score = model_v2.evaluate(x_test, y_test, verbose=1)

    # plot train and test accuracy and loss
    print('Accuracy: ')
    print(history_v2.history['accuracy'])
    print('Test accuracy: ')
    print(history_v2.history['val_accuracy'])
    print('Loss: ')
    print(history_v2.history['loss'])
    print('Test loss: ')
    print(history_v2.history['val_loss'])
    # predict one particular figure
    y_pred = model_v2.predict(x_test, batch_size=batch_size, verbose=1)
    score = log_loss(y_test, y_pred)

    plot_test_class(model_v2, test_files, 11)
