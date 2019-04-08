import numpy as np
A=np.arange(21,30).reshape(3,3)
B=np.arange(31,40).reshape(3,3)
C=A.dot(B)
print(A,'\n\n',B,C)

