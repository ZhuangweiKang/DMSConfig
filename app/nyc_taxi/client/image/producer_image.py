import pickle
import zmq
import os
import cv2
import random



def main():

    broker_IP = "127.0.0.1"
    broker_Port = "5556"
    topic = "test"  # topic = "train"


    path_topic = os.path.join("./state-farm-distracted-driver-detection/imgs/",topic)
    files = os.listdir(path_topic)
    try:
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        addr = "tcp://" + broker_IP + ":" + broker_Port
        socket.connect(addr)

        for file in files:
            if not os.path.isdir(file):
                r = random.random()
                if r > 0.6:
                    f_path = path_topic+"/" + file
                    send_data = normal(f_path)
                    socket.send_multipart([topic.encode(), f_path.encode(), pickle.dumps(send_data)])
                    # socket.send_string("%s %s" % (topic,  str))


    except Exception as e:
        print(e)
        print("bring down zmq publisher")
    finally:
        pass
        socket.close()
        context.term()


def normal(file):
    # Color type: 1 - grey, 3 - rgb
    img_rows = 64
    img_cols = 64
    color_type = 1  # Can also be 3, but in this case grey is enough
    if color_type == 1:
        img = cv2.imread(file, cv2.IMREAD_GRAYSCALE)
    elif color_type == 3:
        img = cv2.imread(file, cv2.IMREAD_COLOR)

    img = cv2.resize(img, (img_rows, img_cols))
    return img






if __name__ == '__main__':
    main()




