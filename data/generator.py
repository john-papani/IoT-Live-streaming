import sys
import json
import random
import numpy as np
from scipy.interpolate import interp1d
from fun import *

def new_day(num_day):
    
    timestamps = np.linspace(0,24,96)
    
    temps = np.minimum(35,fun(timestamps) + 2*np.random.rand(96) + num_day) #μέχρι 35 βαθμούς
    
    th1 = temps - np.random.uniform(0,1)
    
    th2 = temps - - np.random.uniform(3,4.5)
    
    #κλιματισμός
    
    hvac1 = [0,0]
    hvac2 = [0,0]

    for (i,j) in enumerate(temps):
        if i == 0 or i == 1:pass
        else:
            if temps[i-1] > 30 and temps[i-1] > 30:
                hvac1.append(100)
                hvac2.append(200)
            elif temps[i-1] > 27 and temps[i-2] > 27:
                hvac1.append(50)
                hvac2.append(100)
            else:
                hvac1.append(0)
                hvac2.append(0)
    
    #λοιπές ηλεκτρικές συσκευές
    
    miac1 = []
    miac2 = []
    night_rate1 = np.random.uniform(0,30)
    night_rate2 = np.random.uniform(0,40)
    ciel1 = np.random.uniform(30,120)
    ciel2 = np.random.uniform(40,120)

    bday = False
    bnight = False

    for (i,j) in enumerate(timestamps):
        if j >= 0 and j < 7:
            miac1.append(night_rate1)
            miac2.append(night_rate2)
    
        elif j >= 7 and j < 19 and bday == False:
            miac1.append(min(150,max(np.random.uniform(0,35),night_rate1)))
            miac2.append(min(200,max(np.random.uniform(0,45),night_rate2)))
            bday = True
    
        elif j >= 7 and j < 19 and bday == True:
            miac1.append(min(150,max(night_rate1,miac1[i-1]+miac1[i-1]*np.random.uniform(-1,1)/10)))
            miac2.append(min(200,max(night_rate2,miac2[i-1]+miac1[i-1]*np.random.uniform(-1,1)/10)))
    
        elif bnight == False:
            miac1.append(ciel1)
            miac2.append(ciel2)
            bnight = True
    
        else:
            miac1.append(min(150,max(ciel1,miac1[i-1]+miac1[i-1]*np.random.uniform(-1,1)/10)))
            miac2.append(min(200,max(ciel2,miac2[i-1]+miac1[i-1]*np.random.uniform(-1,1)/10)))
            
    #κατανάλωση νερού
    
    water_consumption = []
    for (i,j) in enumerate(timestamps):
        if j >= 0  and j < 7:
            water_consumption.append(0)
        elif j > 20 and j < 22:
            temp = np.random.random()
            if temp > 0.7:
                water_consumption.append(1-np.random.random()/10)
            else:
                water_consumption.append(0)
        elif j > 7 and j < 8:
            water_consumption.append(0.8-np.random.random()/10)
        else:
            if (np.random.random() > 0.7):
                water_consumption.append(0.2+np.random.random()/10)
            else:
                water_consumption.append(0)
    
    motion_detection = []
    #for each minute of the day
    for i in range(1440):
        if np.random.random() < 0.008:
            motion_detection.append(i)
            
    len_mov = len(motion_detection)

    return iter(th1), iter(th2), iter(hvac1), iter(hvac2), iter(miac1), iter(miac2), iter(water_consumption), \
iter(motion_detection), len_mov

def totals():
    
    energy = 873600
    energy_total = []
    for i in range(8):
        x = np.random.uniform(-1000,1000)
        change = 2600 * 24 + x
        energy += change
        energy_total.append(energy)
    
    water = 1540
    water_total = []
    for i in range(8):
        x = np.random.uniform(-10,10)
        change = 110 + x
        water += change
        water_total.append(water)
    
    return iter(energy_total), iter(water_total)

days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday']

state = {"th1" : -1 , 
         "th2" : -1 , 
         "hvac1" : -1,
         "hvac2" : -1, 
         "miac1" : -1,
         "miac2" : -1,
         "etot" : -1,
         "mov1" : 0,
         "w1" : -1,
         "wtot" : -1
        }

def print_timestamp(i,mins,hour):
    if(hour<10):
        print('"2024-11-',i+15, ' 0', hour,':', sep='', end='')
    else: 
        print('"2024-11-',i+15, ' ', hour,':', sep='', end='')
    if(mins==0):
        print('0',mins,':00":',sep='',end='')
    else:
        print(mins,':00":',sep='',end='')
        
#συνάρτηση για να μας δώσει τα late events ως μέσο όρο των 3 προηγούμενων τιμών
def favg(hist_state):
    if len(hist_state) == 0:
        return 0
    else:
        return ((hist_state[-1]["w1"] + hist_state[-2]["w1"] + hist_state[-3]["w1"])  / 3) + np.random.uniform(-0.5,0.5)
f = open("./events_int.json", "w")
sys.stdout = f

hist_state = []
print('{')

etot, wtot = totals()

late_count = 0
late_late_count = 0

for (i,day) in enumerate(days):
    
    th1, th2, hvac1, hvac2, miac1, miac2, w1, mov1, len_mov = new_day(i)
    temp = next(mov1)
    times_mov = 1
    state["etot"] = int(next(etot))
    state["wtot"] = int(next(wtot))
    for hour in range(24):
        for mins in range(0,60,15):
            
            hist_state.append(state)
            
            late_count += 1
            late_late_count += 1
            
            print_timestamp(i,mins,hour)
            
            state["th1"] = int(next(th1))
            state["th2"] = int(next(th2))
            state["hvac1"] = int(next(hvac1))
            state["hvac2"] = int(next(hvac2))
            state["miac1"] = int(next(miac1))
            state["miac2"] = int(next(miac2))
            state["w1"] = round(next(w1),2)
            
            if hour*60+mins > temp and times_mov < len_mov:
                state["mov1"] = 1
                temp = next(mov1)
                times_mov += 1
            else:
                state["mov1"] = 0
            
            if (i == 6 and hour==23 and mins==45):
                print(json.dumps(state),end='\n')
            else:
                print(json.dumps(state),end=',\n')
            
            state["mov1"] = 0
            
            #late events!
            #2 days prior
            if late_count == 20 and late_late_count != 120:
                late_count = 0
                print_timestamp(i-2,mins,hour)
                temp_dict = {}
                temp_dict["w1"] = max(round(favg(hist_state),2),0)
                
                if (i == 6 and hour==23 and mins==45):
                    print(json.dumps(temp_dict),end='\n')
                else:
                    print(json.dumps(temp_dict),end=',\n')
            
            #10 days prior
            elif late_late_count == 120:
                
                #20
                late_count = 0
                print_timestamp(i-2,mins,hour)
                temp_dict = {}
                temp_dict["w1"] = max(round(favg(hist_state),2),0)
                
                if (i == 6 and hour==23 and mins==45):
                    print(json.dumps(temp_dict),end='\n')
                else:
                    print(json.dumps(temp_dict),end=',\n')
                    
                #120
                late_late_count = 0
                print_timestamp(i-10,mins,hour)
                temp_dict = {}
                temp_dict["w1"] = max(round(favg(hist_state),2),0)
                
                if (i == 7 and hour==23 and mins==45):
                    print(json.dumps(temp_dict),end='\n')
                else:
                    print(json.dumps(temp_dict),end=',\n')
            else:
                pass
            

print('}')

f.close()