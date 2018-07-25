import psycopg2
from tqdm import tqdm
import calendar
import data.crimeAnalysis_pb2 as ca


# 获取数据 根据层级，起始时间，四至范围
def getDateFromPg(layer, startTime, stopTime, spaceX1, spaceX2, spaceY1,
                  spaceY2):
    conn = psycopg2.connect(
        database="crimeanalysis",
        user="crimeanalysis",
        password="crimeanalysis",
        host="132.1.11.158",
        port="5432")
    cur = conn.cursor()
    str1 = "select  geomatrix.xcget_rows(code) as rs,geomatrix.xcget_cols(code) as cs,hour,cnt from crime.xcget_real_time_crime(" + str(
        layer) + ",'" + str(startTime) + "','" + str(stopTime) + "'," + str(
            spaceX1) + "," + str(spaceX2) + "," + str(spaceY1) + "," + str(
                spaceY2) + ")"
    cur.execute(str1)
    rows = cur.fetchall()
    data = []
    # 元组不能修改，所以转到数组
    for row in rows:
        i = 0
        tempd = []
        tempd.append(int(row[0]))
        tempd.append(int(row[1]))
        tempd.append(row[2])
        tempd.append(row[3])
        i = i + 1
        data.append(tempd)
    return data, rows


# 获取时间和年份(set不重复)
def get_hours(data):
    t = set()
    year = set()
    for row in data:
        t.add(row[2])
        tempt = row[2]
        year.add(tempt[:4])
    return t, year


# 数据上时间填空，insert()
def fill_data(data, t, years, startTime, stopTime):
    # todo 需要添加时间点
    timecode = []
    for year in years:
        for month in range(1, 13):
            if month < 10:
                month = '0' + str(month)
            month = str(month)
            monthRange = calendar.monthrange(int(year), int(month))
            dayCount = monthRange[1] + 1
            for day in range(1, dayCount):
                if day < 10:
                    day = '0' + str(day)
                day = str(day)
                for hour in range(24):
                    if hour < 10:
                        hour = '0' + str(hour)
                    hour = str(hour)
                    timecode.append(year + month + day + hour)
    # 在时间范围内 判断时间是否在此范围内有缺漏，缺漏补上，行列号都为0，时间有时间
    for c in timecode:
        if c >= str(startTime) and c <= str(stopTime):
            if c in t:
                continue
            else:
                tempd = []
                tempd.append(int(-1))
                tempd.append(int(-1))
                tempd.append(c)
                tempd.append(int(-1))
                data.append(tempd)
    return data


def write_data(metaData, startTime, stopTime, spaceX1, spaceX2,
               spaceY1, spaceY2, newData,layer):
    cd = ca.CrimeData()
    time_extend = cd.Time_extend()
    time_extend.startTime = startTime
    time_extend.stopTime = stopTime
    extend = cd.Extend()
    extend.spaceX1 = spaceX1
    extend.spaceX2 = spaceX2
    extend.spaceY1 = spaceY1
    extend.spaceY2 = spaceY2
    grid = cd.Grid()
    grid.layer =layer
    length = len(newData)
    print(length)
    path = "data/data"+str(layer)+".bin"
    for i in tqdm(range(length)):
        d = cd.allData.add()
        d.gridRow = newData[i][0]
        d.gridColumn = newData[i][1]
        d.time = newData[i][2]
        d.crimeNum = newData[i][3]
    f = open(path, "wb")
    x = cd.SerializeToString()
    f.write(x)
    f.close()


if __name__ == '__main__':
    for layer in range(12,18):
        startTime = 2012010100
        stopTime = 2013103100
        spaceX1 = 110
        spaceX2 = 130
        spaceY1 = 10
        spaceY2 = 40
        # 根据输入时间范围和层级 从数据库返回数据 数据为二维数组格式 data
        data, data2 = getDateFromPg(layer, startTime, stopTime, spaceX1, spaceX2,
                                    spaceY1, spaceY2)
        # 获取所有的时间，为set格式
        t, year = get_hours(data)
        # 根据起始时间和时间表年份表，填充数据
        newData = fill_data(data, t, year, startTime, stopTime)
        write_data("案件",str(startTime),str(stopTime),spaceX1,spaceX2,spaceY1,spaceY2,newData,layer)
