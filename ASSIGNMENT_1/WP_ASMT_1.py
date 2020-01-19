import random

studs=100 #ANY NUMBER OF STUDENTS CAN BE GIVEN

USN=['4JN18CS'+str(a) for a in ["%03d" % i for i in range(1,studs+1)]] #GENERATING USN

def get_items(File): #FOR FETCHING LINES OF A FILE IN LIST
  with open(File) as f: 
    return f.read().split('\n')

INDNAMES=get_items('IndianNames.txt') #FAKER CAN ALSO BE USED BUT WILL GENERATE HINDI NAMES AND NEEDS TO BE CONVERTED
NAMES=random.sample(INDNAMES,k=studs)
NAMES.sort() #SO THAT IT WILL BE COMPLYING WITH USN
NAMES=[x.upper() for x in NAMES] #CANNOT USE MAP AS MAP RETURNS MAP OBJECT

ELE_1=get_items('Elective_Group_1.txt')
ELE_2=get_items('Elective_Group_2.txt')

ELECTIVE_1=[random.choice(ELE_1) for n in range(0,studs)] #RANDOM ELECTIVE1
ELECTIVE_1=[x.upper() for x in ELECTIVE_1]
ELECTIVE_2=[random.choice(ELE_2) for n in range(0,studs)] #RANDOM ELECTIVE2
ELECTIVE_2=[x.upper() for x in ELECTIVE_2]

MARKS_1=[random.randrange(0,100) for n in range(0,studs)]
MARKS_2=[random.randrange(0,100) for n in range(0,studs)]

with open('ELEC_PERF.txt',"w") as f: #WRITE TO FINAL FILE
  for i in range(len(USN)):
    f.write(USN[i]+','+NAMES[i]+','+ELECTIVE_1[i]+','+ELECTIVE_2[i]+','+str(MARKS_1[i])+','+str(MARKS_2[i])+'\n')

#FINAL TEXT FILE IS SEPARATED BY COMMA SUCH THAT IT IS INTERCONVERTIBLE TO CSV JUST BY CHANGING EXTENSION
