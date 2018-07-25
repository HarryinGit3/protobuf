import data.crimeAnalysis_pb2 as ca

path = "data/data.bin"
cd = ca.CrimeData()
f = open(path,"rb")
cd.ParseFromString(f.read())
print(cd)
f.close()