import os
from glob import glob
import random
import time
import tensorflow
import datetime
import sys

os.environ['KERAS_BACKEND'] = 'tensorflow'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' # 3 = INFO, WARNING, and ERROR messages are not printed

from tqdm import tqdm

import numpy as np
import pandas as pd
from IPython.display import FileLink
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
import seaborn as sns 
#%matplotlib inline
from IPython.display import display, Image
import matplotlib.image as mpimg
import cv2

from sklearn.model_selection import train_test_split
from sklearn.datasets import load_files       
from keras.utils import np_utils
from sklearn.utils import shuffle
from sklearn.metrics import log_loss

from keras.models import Sequential, Model
from keras.layers import Conv2D, MaxPooling2D, Flatten, Dense, Dropout, BatchNormalization, GlobalAveragePooling2D
from keras.preprocessing.image import ImageDataGenerator
from keras.preprocessing import image
from keras.callbacks import ModelCheckpoint, EarlyStopping
from keras.applications.vgg16 import VGG16



# Load the dataset previously downloaded from Kaggle
NUMBER_CLASSES = 10
# Color type: 1 - grey, 3 - rgb

def get_cv2_image(path, img_rows, img_cols, color_type=3):
    # Loading as Grayscale image
    if color_type == 1:
        img = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
    elif color_type == 3:
        img = cv2.imread(path, cv2.IMREAD_COLOR)
    # Reduce size
    img = cv2.resize(img, (img_rows, img_cols)) 
    return img

# Training
def load_train(img_rows, img_cols, color_type=3):
    start_time = time.time()
    train_images = [] 
    train_labels = []
    pwd = os.getcwd()
    # Loop over the training folder 
    for classed in tqdm(range(NUMBER_CLASSES)):
        print('Loading directory c{}'.format(classed))
        files = glob(os.path.join('data','imgs', 'train', 'c' + str(classed), '*.jpg'))
        #print(files)
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
    
    x_train = np.array(x_train, dtype=np.uint8).reshape(-1,img_rows,img_cols,color_type)
    x_test = np.array(x_test, dtype=np.uint8).reshape(-1,img_rows,img_cols,color_type)
    
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
    test_data = test_data.reshape(-1,img_rows,img_cols,color_type)
    
    return test_data, test_ids

def create_submission(predictions, test_id, info):
    result = pd.DataFrame(predictions, columns=['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9'])
    result.loc[:, 'img'] = pd.Series(test_id, index=result.index)
    
    now = datetime.datetime.now()
    
    if not os.path.isdir('kaggle_submissions'):
        os.mkdir('kaggle_submissions')

    suffix = "{}_{}".format(info,str(now.strftime("%Y-%m-%d-%H-%M")))
    sub_file = os.path.join('kaggle_submissions', 'submission_' + suffix + '.csv')
    
    result.to_csv(sub_file, index=False)
    
    return sub_file




def create_model_v2():
    # Optimised Vanilla CNN model
    model = Sequential()

    ## CNN 1
    model.add(Conv2D(32,(3,3),activation='relu',input_shape=(img_rows, img_cols, color_type)))
    model.add(BatchNormalization())
    model.add(Conv2D(32,(3,3),activation='relu',padding='same'))
    model.add(BatchNormalization(axis = 3))
    model.add(MaxPooling2D(pool_size=(2,2),padding='same'))
    model.add(Dropout(0.3))

    ## CNN 2
    model.add(Conv2D(64,(3,3),activation='relu',padding='same'))
    model.add(BatchNormalization())
    model.add(Conv2D(64,(3,3),activation='relu',padding='same'))
    model.add(BatchNormalization(axis = 3))
    model.add(MaxPooling2D(pool_size=(2,2),padding='same'))
    model.add(Dropout(0.3))

    ## CNN 3
    model.add(Conv2D(128,(3,3),activation='relu',padding='same'))
    model.add(BatchNormalization())
    model.add(Conv2D(128,(3,3),activation='relu',padding='same'))
    model.add(BatchNormalization(axis = 3))
    model.add(MaxPooling2D(pool_size=(2,2),padding='same'))
    model.add(Dropout(0.5))

    ## Output
    model.add(Flatten())
    model.add(Dense(512,activation='relu'))
    model.add(BatchNormalization())
    model.add(Dropout(0.5))
    model.add(Dense(128,activation='relu'))
    model.add(Dropout(0.25))
    model.add(Dense(10,activation='softmax'))

    return model

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
    img_brute = cv2.resize(img_brute,(img_rows,img_cols))
    #plt.imshow(img_brute, cmap='gray')

    new_img = img_brute.reshape(-1,img_rows,img_cols,color_type)

    y_prediction = model.predict(new_img, batch_size=batch_size, verbose=1)
    #print('Y prediction: {}'.format(y_prediction))
    #print('Predicted: {}'.format(activity_map.get('c{}'.format(np.argmax(y_prediction)))))
    #print(format(activity_map.get('c{}'.format(np.argmax(y_prediction)))))
    #print('c{}'.format(np.argmax(y_prediction)))
    #plt.show()
    print('Predicted: {}'.format('c{}'.format(np.argmax(y_prediction)) + ' - '+activity_map.get('c{}'.format(np.argmax(y_prediction)))))

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

    names = [item[17:19] for item in sorted(glob("../input/train/*/"))]
    test_files_size = len(np.array(glob(os.path.join('..', 'input', 'test', '*.jpg'))))
    x_train_size = len(x_train)
    categories_size = len(names)
    x_test_size = len(x_test)

    # Find the frequency of images per driver
    drivers_id = pd.DataFrame((dataset['subject'].value_counts()).reset_index())
    drivers_id.columns = ['driver_id', 'Counts']

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
    history_v2 = model_v2.fit(x_train, y_train, validation_data=(x_test, y_test), callbacks=callbacks, epochs=nb_epoch, batch_size=batch_size, verbose=1)
    
    

    # load model weight
    model_v2.load_weights('saved_models/weights_best_vanilla.hdf5')

    score = model_v2.evaluate(x_test, y_test, verbose=1)
    
    # plot train and test accuracy and loss
    print('Accuracy: ')
    print(history_v2.history['accuracy'])
    print('Test accuracy: ' )
    print(history_v2.history['val_accuracy'])
    print('Loss: ')
    print(history_v2.history['loss'])
    print('Test loss: ')
    print(history_v2.history['val_loss'])
    # predict one particular figure
    y_pred = model_v2.predict(x_test, batch_size=batch_size, verbose=1)
    score = log_loss(y_test, y_pred)
    
    plot_test_class(model_v2, test_files, 11)
