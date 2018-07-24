import psycopg2
from tqdm import tqdm
import calendar


# 获取数据
def getDateFromPg(layer, startTime, stopTime):
    conn = psycopg2.connect(
        database="crimeanalysis",
        user="crimeanalysis",
        password="crimeanalysis",
        host="132.1.11.158",
        port="5432")
    cur = conn.cursor()
    str1 = "select  geomatrix.xcget_rows(code) as rs,geomatrix.xcget_cols(code) as cs,hour,cnt from crime.xcget_real_time_crime(" + str(
        layer) + ",'" + str(startTime) + "','" + str(stopTime) + "') limit 100"
    cur.execute(str1)
    rows = cur.fetchall()
    data = []
    # 元组不能修改，所以转到数组
    for row in rows:
        i = 0
        tempd = []
        tempd.append(row[0])
        tempd.append(row[1])
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
                tempd.append(0)
                tempd.append(0)
                tempd.append(c)
                tempd.append(0)
                data.append(tempd)
            print(c)
    return data


if __name__ == '__main__':
    startTime = 2012010100
    stopTime = 2012010700
    # 根据输入时间范围和层级 从数据库返回数据 数据为二维数组格式 data
    data, data2 = getDateFromPg(12, startTime, stopTime)
    # 获取所有的时间，为set格式
    t, year = get_hours(data)
    # 根据起始时间和时间表年份表，填充数据
    newData = fill_data(data2, t, year, startTime, stopTime)
