import json
import operator
from matplotlib import pyplot as plt
pub_num = 3
publisher_core = 1
broker_core = 4
consumer_core = 4
zookeeper_core = 2



ret_dic = {}



def SecondMax (dic):
    max_value = max(dic.values())
    max2 = 0
    max2_id = None
    for v in dic:
        if max2 < dic[v] < max_value:
            max2 = dic[v]
            max2_id = v
    return max2_id

for i  in range (1, 25):

    with open('resources.json') as f:
        data = json.load(f)

    for key in data:
        data[key] -= 2

    pub_num = i

    Flag = True
    group_list = []
    while Flag:
        temp_dic = {}
        # Broker
        id = max(data.items(), key=operator.itemgetter(1))[0]
        temp_dic["kafka"] = id
        data[id] -= broker_core
        #consumer
        id = max(data.items(), key=operator.itemgetter(1))[0]
        if temp_dic["kafka"] == id:
            id = SecondMax(data)
        temp_dic["sub"] = id
        data[id] -= consumer_core
        #zookeeper
        id = max(data.items(), key=operator.itemgetter(1))[0]
        temp_dic["zk"] = id
        data[id] -= zookeeper_core

        #producer
        for i in range(0, pub_num):

            id = max(data.items(), key=operator.itemgetter(1))[0]
            if temp_dic["kafka"] == id:
                id = SecondMax(data)
                if id == None:
                    break
            name = "pub" + str(i)
            temp_dic[name] = id
            # print(data)
            # print(data[id])
            data[id] -= publisher_core


        for key in data:
            if data[key] < 0:
                Flag = False
        if Flag:
            group_list.append(temp_dic)
    # print(data)
    # print(len(group_list))
    # for key in group_list:
    #     print (key)
    # print(group_list)
    ret_dic[i] = group_list

x =[]
y =[]
for key in ret_dic:
    x.append(key+1),
    y.append(len(ret_dic[key]))




with open('resources_assignment.json', 'w') as json_file:
  json.dump(ret_dic, json_file)
print(x)
print(y)
plt.plot(x,y)
plt.xticks(x)
plt.grid()
plt.xlabel("#pubs")
plt.ylabel("#groups")
plt.savefig("resources_graph.png")
plt.show()