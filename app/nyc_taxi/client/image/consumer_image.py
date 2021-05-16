
import zmq
import pickle

def main():
    broker_IP = "127.0.0.1"
    broker_Port = "5200"
    topic = ["test"]
    try:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        addr = "tcp://" + broker_IP + ":" + broker_Port
        socket.connect(addr)


        for key in topic:
            socket.setsockopt_string(zmq.SUBSCRIBE, key)

        while True:
            [topic, f_path, msg] = socket.recv_multipart()
            new_msg = pickle.loads(msg)
            print(new_msg)

    except Exception as e:
        print(e)
        print("bring down zmq subscriber")
    finally:
        pass
        socket.close()
        context.term()

if __name__ == '__main__':
    main()