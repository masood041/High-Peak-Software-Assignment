import pandas as pd
import numpy as np

studentid = []
sub = ['p', 'c', 'm', 'b']
subject = []
marks = []

for i in range(1, 7):
    for j in sub:
        studentid.append(i)
        subject.append(j)
        marks.append(np.random.randint(100))

list_of_tuples = list(zip(studentid, subject, marks))
df = pd.DataFrame(list_of_tuples, 
                  columns = ['studentId', 'marks','subject'])
df.to_csv("Marks.csv",index=False)
