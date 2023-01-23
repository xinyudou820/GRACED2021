import pandas as pd
import os
import numpy as np
from netCDF4 import Dataset
import calendar
import re
from datetime import datetime as dz
import tarfile
import xarray as xr

# 所有路径
global_path = '/data3/kow/data/'

tool_path = os.path.join(global_path, 'tools')
edgar_path = os.path.join(tool_path, 'edgar')
cm_path = os.path.join(tool_path, 'cm')
map_path = os.path.join(tool_path, 'map')
no2_path = os.path.join(tool_path, 'NO2')
no2_daily_path = os.path.join(tool_path, 'NO2_daily')
first_path = os.path.join(global_path, 'first')
second_path = os.path.join(global_path, 'second')
final_path = os.path.join(global_path, 'total')
zip_path = os.path.join(global_path, 'zip')

# 参数
end_year = int(dz.now().strftime('%Y'))  # 当前年

country_list = ['Brazil', 'China', 'EU27 & UK', 'France', 'Germany', 'India', 'Italy', 'Japan', 'ROW', 'Russia',
                'Spain', 'US', 'UK']
sector_list = ['Power', 'Industry', 'Residential', 'Ground Transport', 'International Aviation',
               'International Shipping', 'Domestic Aviation']
new_sector_list = ['Power', 'Industry', 'Residential', 'GroundTransportation', 'InternationalAviation',
                   'InternationalShipping', 'DomesticAviation']

type_list = [['International Aviation', 'International Shipping'],
             ['Power', 'Industry', 'Residential', 'Ground Transport', 'Domestic Aviation']]
country_ind = [34, 50, np.nan, 81, 88, 106, 113, 115, np.nan, 183, 212, 239, 237]  # 对应cmreg国家列表在coun1中的编号
EU28_nodupl_ind = [16, 24, 38, 61, 64, 65, 66, 75, 80, 91, 104, 110, 124, 130, 131, 138, 157, 177, 178, 182, 204,
                   205, 217]  # 除去单独欧洲大国之后的欧洲国家
np.seterr(divide='ignore', invalid='ignore')
bigcountries = ['China', 'India', 'US', 'Europe', 'UK', 'Germany', 'France', 'Spain', 'Italy', 'Russia', 'Japan',
                'Brazil']

cmreg = ['Brazil', 'China', 'EU', 'France', 'Germany', 'India', 'Italy', 'Japan', 'ROW', 'Russia', 'Spain',
         'US', 'UK']


def main():
    process_first()  # 处理所有数据
    create_daily_no2()  # 根据CM数据拆分edgar月度数据
    fill_no2()  # 微调数据
    final_sum()  # 合并最后的结果
    # change_file_name()  # 需要重新命名一下文件名
    shrink_etc()  # 减小文件大小并添加属性
    file_to_zip()  # 压缩


def read_map():
    nc1 = Dataset(os.path.join(map_path, 'WorldCountries_01.nc'))
    coun1 = nc1.variables['countries'][:].filled(-999)
    nc2 = Dataset(os.path.join(map_path, 'China_bou.nc'))  # Map of China without political issues
    coun2 = nc2.variables['China'][:].filled(-999)
    coun1[coun2 == 0] = 50
    seamask = coun1 == -999
    return coun1, seamask


def search_file(file_path):  # 搜索特定路径下的所有文件
    import os
    import sys
    sys.dont_write_bytecode = True
    file_name = []
    for parent, surnames, filenames in os.walk(file_path):
        for fn in filenames:
            file_name.append(os.path.join(parent, fn))
    return file_name


def read_edgar():  # 读取所有edgar数据
    mon = [i for i in range(1, 13)]
    m_days = create_year_days(2019, mon)

    edgar2019m_part1 = np.load(os.path.join(edgar_path, 'new2_EDGAR_CO2_emission_2019_1-6.npy'),
                               allow_pickle=True, encoding='latin1')[:, :, ::-1]
    edgar2019m_part2 = np.load(os.path.join(edgar_path, 'new2_EDGAR_CO2_emission_2019_7-12.npy'),
                               allow_pickle=True, encoding='latin1')[:, :, ::-1]  # 这两个地方记得可以反过来
    # ton # CO2 -> kgC/h
    edgar2019m = np.hstack((edgar2019m_part1, edgar2019m_part2)) * 1e3 / 44. * 12. / m_days.reshape(1, -1, 1, 1) / 24.

    oldref = np.zeros((7, 12, 1800, 3600))

    # power
    oldref[0] = edgar2019m[0] + edgar2019m[1] + edgar2019m[2]
    # industry, cement is in this sector in CarbonMonitor
    oldref[1] = edgar2019m[3] + edgar2019m[4] + edgar2019m[5] + edgar2019m[10]
    # residential
    oldref[2] = edgar2019m[6] * 1.
    # ground transport
    oldref[3] = edgar2019m[7] * 1.
    # international aviation
    oldref[4] = edgar2019m[8] * 1.
    # shipping
    oldref[5] = edgar2019m[9] * 1.
    # domestic international
    oldref[6] = edgar2019m[11] * 1.

    ref = np.zeros((7, 12, 1800, 3600))

    for imon in range(0, 12):
        for isect in range(len(sector_list)):
            c_mon_EDGAR = oldref[isect, imon, :, :]
            c_mon_EDGAR[c_mon_EDGAR < 0] = 0
            ref[isect, imon, :, :] = c_mon_EDGAR
    return ref


def read_cm():  # 读取并处理CM数据
    file_name = search_file(cm_path)
    cm_file = max(file_name)
    df_cm = pd.read_csv(cm_file)  # 这个文件以后要问邓博要路径
    # 设定后续的截至日期
    year_range = int(pd.to_datetime(max(df_cm['date'])).strftime('%Y'))  # 截至年份
    month_range = int(pd.to_datetime(max(df_cm['date'])).strftime('%m')) + 1  # 截至月份+1

    df_cm = df_cm[df_cm['sector'] != 'Total'].reset_index(drop=True)
    df_cm['year'] = pd.to_datetime(df_cm['date']).dt.year
    df_cm['month_date'] = pd.to_datetime(df_cm['date']).dt.strftime('%m-%d')
    df_cm = pd.pivot_table(df_cm, index=['sector', 'year', 'country'], values='co2',
                           columns='month_date').reset_index().fillna(0)
    df_cm = df_cm.set_index(['sector', 'year', 'country']).stack().reset_index().rename(
        columns={'level_3': 'month_date', 0: 'co2'})

    year_list = df_cm['year'].unique()
    return year_list, df_cm, year_range, month_range


def process_cm(year):
    cm_em = np.zeros((len(country_list), len(sector_list), 366))
    shp2021 = np.zeros(366)
    avt2021 = np.zeros(366)
    df_cm = read_cm()[1]
    for t in type_list:  # 分国际处理方式
        if 'Power' in t:  # 其他部门处理方式
            df_temp = df_cm[
                (df_cm['sector'].isin(t)) & (df_cm['country'] != 'WORLD') & (df_cm['year'] == year)].reset_index(
                drop=True)
            for country in range(len(country_list)):
                temp_2 = df_temp[df_temp['country'] == country_list[country]].reset_index(drop=True)
                temp_2 = pd.pivot_table(temp_2, index=['sector', 'year'], values='co2',
                                        columns='month_date').reset_index()
                # 补全国际部门  也不知道为啥 虽然没有值
                missing_sector = pd.DataFrame(['International Aviation', 'International Shipping'], columns=['sector'])
                temp_2 = pd.concat([temp_2, missing_sector]).reset_index(drop=True).fillna(0)
                temp_2 = temp_2.set_index('sector').loc[sector_list].reset_index().drop(
                    columns=['sector', 'year'])  # 给部门排序
                rest_np = temp_2.values * 1e6 / 44. * 12. / 24
                cm_em[country] = rest_np
        else:  # 国际部门处理方式
            df_temp = df_cm[
                (df_cm['sector'].isin(t)) & (df_cm['country'] == 'WORLD') & (df_cm['year'] == year)].reset_index(
                drop=True)
            for s in t:
                temp_2 = df_temp[df_temp['sector'] == s].reset_index(drop=True)
                temp_2 = pd.pivot_table(temp_2, index=['sector'], values='co2',
                                        columns='month_date').reset_index().fillna(0).drop(columns=['sector'])
                if s == 'International Aviation':
                    avt2021 = temp_2.values[0] * 1e6 / 44. * 12. / 24  # 用原来的换单位 单位差了1000 不知道原因
                else:
                    shp2021 = temp_2.values[0] * 1e6 / 44. * 12. / 24  # 用原来的换单位 单位差了1000 不知道原因
    cm_em[2] -= cm_em[3] + cm_em[4] + cm_em[6] + cm_em[10] + cm_em[12]
    # 计算当年每月天数
    m_days = []
    for month in range(1, 13):
        m_days.append(calendar.monthrange(year, month)[1])
    m_days = np.array(m_days)

    return cm_em, avt2021, shp2021, m_days


def process_first():  # 还没想要名字 处理edgar和CM数据
    ref = read_edgar()  # 读取处理好的edgar数据
    coun1, seamask = read_map()  # 读取map数据
    year_list = read_cm()[0]  # 读取cm的年份数据
    # 截至日期
    year_range = read_cm()[2]
    month_range = read_cm()[3]

    for y in year_list:  # 这里从19年开始到末年
        cm_em, avt2021, shp2021, mdays = process_cm(y)
        if mdays[1] == 28:
            year_select = 1  # 非闰年
            mdays[1] = 29
        else:
            year_select = 2  # 闰年

        for imon in range(1, 13):  # 一年12个月
            if y == year_range and imon == month_range:
                break
            else:
                for isect in range(len(new_sector_list)):  # 所有部门
                    out_folder = os.path.join(first_path,
                                              'CarbonMonitor_%s_%s' % (isect, new_sector_list[isect]))
                    if not os.path.exists(out_folder):  # 没有的话就新建
                        os.mkdir(out_folder)
                    out_file = os.path.join(out_folder, 'CarbonMonitor_%s_%s_y%s_m%s.nc' % (
                        isect, new_sector_list[isect], y, str(imon).zfill(2)))
                    resultfile1 = Dataset(out_file, 'w')
                    resultfile1.createDimension('latitude', 1800)
                    resultfile1.createDimension('longitude', 3600)
                    if year_select == 1 and imon == 2:  # 非闰年2月
                        resultfile1.createDimension('nday', 28)
                    else:
                        resultfile1.createDimension('nday', mdays[imon - 1])
                    var = resultfile1.createVariable('latitude', 'd', 'latitude')
                    var[:] = np.arange(89.95, -90, -0.1)
                    var = resultfile1.createVariable('longitude', 'd', 'longitude')
                    var[:] = np.arange(-179.95, 180, 0.1)
                    emis2021 = resultfile1.createVariable('emission', 'd', ('nday', 'latitude', 'longitude'))
                    setattr(emis2021, 'units', "kgC/h")
                    tmp2021 = np.zeros((mdays[imon - 1], 1800, 3600))

                    if isect == 4:  # 国际航空
                        sf2021 = avt2021[np.sum(mdays[:imon - 1]):np.sum(mdays[:imon])] / np.sum(
                            ref[isect, imon - 1])  # 后面是一个值
                        tmp2021[:] = ref[isect, imon - 1] * sf2021.reshape(-1, 1, 1)

                    if isect == 5:  # 国际航运
                        sf2021 = shp2021[np.sum(mdays[:imon - 1]):np.sum(mdays[:imon])] / np.sum(
                            ref[isect, imon - 1, seamask])
                        tmp2021[:, seamask] = ref[isect, imon - 1, seamask] * sf2021.reshape(-1, 1)

                    if isect < 4 or isect == 6:
                        for ireg in range(len(country_list)):
                            if country_list[ireg] not in ['ROW', 'EU27 & UK']:
                                regmask = coun1 == country_ind[ireg]
                            if country_list[ireg] == 'EU27 & UK':
                                regmask = np.in1d(coun1, EU28_nodupl_ind).reshape(1800, 3600)
                            if country_list[ireg] == 'ROW':
                                regmask = np.logical_not(
                                    np.in1d(coun1, EU28_nodupl_ind) + np.in1d(coun1,
                                                                              country_ind) + seamask.flatten()).reshape(
                                    1800, 3600)

                            sf2021 = cm_em[ireg, isect, np.sum(mdays[:imon - 1]):np.sum(mdays[:imon])] / np.nansum(
                                ref[isect, imon - 1, regmask])
                            tmp2021[:, regmask] += ref[isect, imon - 1, regmask] * sf2021.reshape(-1, 1)
                    if year_select == 1 and imon == 2:
                        emis2021[:] = tmp2021[:28, :, :]
                    else:
                        emis2021[:] = tmp2021[:]

                    resultfile1.close()


def create_year_days(year, file_year):
    m_days = []
    for month in range(1, len(file_year) + 1):
        m_days.append(calendar.monthrange(year, month)[1])
    m_days = np.array(m_days)
    return m_days


def read_province_map():
    CHN_PROV = np.genfromtxt(os.path.join(map_path, 'CHN_PROV_01.asc'), dtype=float,
                             delimiter=' ', skip_header=6)
    CHN_PROV_NUM = (np.delete(np.unique(CHN_PROV), 0))

    nc3 = Dataset(os.path.join(map_path, 'world_provinces.nc'))
    WRD_PROV = nc3.variables['provinces'][:].filled(-999)

    nc4 = Dataset(os.path.join(map_path, 'WorldCountries_01.nc'))
    WRD_CTY = nc4.variables['countries'][:].filled(-999)

    India = (np.delete(np.unique(WRD_PROV[WRD_CTY == 106]), 0))
    US = (np.delete(np.unique(WRD_PROV[WRD_CTY == 239]), 0))
    UK = (np.delete(np.unique(WRD_PROV[WRD_CTY == 237]), 0))
    Germany = (np.delete(np.unique(WRD_PROV[WRD_CTY == 88]), 0))
    France = (np.delete(np.unique(WRD_PROV[WRD_CTY == 81]), 0))
    Spain = (np.delete(np.unique(WRD_PROV[WRD_CTY == 212]), 0))
    Italy = (np.delete(np.unique(WRD_PROV[WRD_CTY == 113]), 0))
    Japan = (np.delete(np.unique(WRD_PROV[WRD_CTY == 115]), 0))
    Brazil = (np.delete(np.unique(WRD_PROV[WRD_CTY == 34]), 0))
    Russia = []
    EU = []
    ROW = []

    WRD_PROV_NUM = [Brazil, CHN_PROV_NUM, EU, France, Germany, India, Italy, Japan, ROW, Russia, Spain, US, UK]
    return CHN_PROV, CHN_PROV_NUM, WRD_PROV, WRD_PROV_NUM


def prcoess_no2(year):
    days2019 = np.zeros((366, 1800, 3600))
    days2021 = np.zeros((366, 1800, 3600))
    R21 = np.zeros((366, 1800, 3600))
    R = np.zeros((366, 1800, 3600))  # 一月有31天
    no2_file = search_file(no2_path)
    CHN_PROV, CHN_PROV_NUM, WRD_PROV, WRD_PROV_NUM = read_province_map()  # 读取map数据
    file_name = [no2_file[i] for i, x in enumerate(no2_file) if x.find('%s' % year) != -1]  # 文件长度 也就是那一年的数据月份有几个
    # 汇总当前年每月的天数
    mdays = create_year_days(year, file_name)
    if mdays[1] == 28:
        year_select = 1
        mdays[1] = 29
    else:
        year_select = 2
    for imon in range(1, len(file_name) + 1):
        nc_obj19 = Dataset(os.path.join(no2_path, 'NO2_2019%2.2i_14d.nc' % imon))
        nc_obj21 = Dataset(os.path.join(no2_path, 'NO2_%s%2.2i_14d.nc' % (year, imon)))

        for iday in range(mdays[imon - 1]):  # range(0, mdays[imon - 1]):   # test for the first day
            idays = np.sum(mdays[0:imon - 1]) + iday
            if year_select == 1:  # 如果不是闰年
                if imon == 2 and iday == 28:
                    days2019[idays, :, :] = np.zeros((1800, 3600))  # 将2019.2.29的NO2设为0值
                    days2021[idays, :, :] = np.zeros((1800, 3600))  # 将2021.2.29的NO2设为0值
                else:
                    days2019[idays, :, :] = nc_obj19.variables[u'NO2'][iday, :, :]
                    days2021[idays, :, :] = nc_obj21.variables[u'NO2'][iday, :, :]
            if year_select == 2:  # 闰年的话
                if imon == 2 and iday == 28:
                    days2019[idays, :, :] = nc_obj21.variables[u'NO2'][iday, :, :]
                else:
                    days2019[idays, :, :] = nc_obj19.variables[u'NO2'][iday, :, :]
                days2021[idays, :, :] = nc_obj21.variables[u'NO2'][iday, :, :]
            currentday2019 = days2019[idays, :, :]
            currentday2021 = days2021[idays, :, :]
            R_1day = np.zeros((1800, 3600))
            R_2day = np.zeros((1800, 3600))

            for icountry in range(len(WRD_PROV_NUM)):
                prov_curday2019 = np.zeros((len(WRD_PROV_NUM[icountry]), 1800, 3600))
                prov_curday2021 = np.zeros((len(WRD_PROV_NUM[icountry]), 1800, 3600))
                NO2_19prov = np.zeros((mdays[imon - 1], len(WRD_PROV_NUM[icountry])))  # iday, iprov
                NO2_21prov = np.zeros((mdays[imon - 1], len(WRD_PROV_NUM[icountry])))

                if icountry == 0:  # for China
                    for iprov in range(0, len(CHN_PROV_NUM)):
                        tmp = prov_curday2019[iprov, :, :]
                        tmp21 = prov_curday2021[iprov, :, :]
                        tmp[CHN_PROV == CHN_PROV_NUM[iprov]] = currentday2019[
                            CHN_PROV == CHN_PROV_NUM[iprov]]
                        tmp21[CHN_PROV == CHN_PROV_NUM[iprov]] = currentday2021[
                            CHN_PROV == CHN_PROV_NUM[iprov]]

                        '''只留下top5%的值，其他都设置为0'''
                        tmp[tmp < 0] = 0  # negative vaues --> 0
                        tmp[tmp21 < 0] = 0
                        tmp[tmp < np.percentile(tmp, 95)] = 0  # below top5% vaues --> 0

                        NO2_19prov[iday, iprov] = np.average(tmp[tmp > 0])  # 2019年省级平均的NO2值
                        NO2_21prov[iday, iprov] = np.average(tmp21[tmp > 0])  # 2021年省级平均的NO2值

                        R_1day[CHN_PROV == CHN_PROV_NUM[iprov]] = NO2_19prov[iday, iprov]
                        R_2day[CHN_PROV == CHN_PROV_NUM[iprov]] = NO2_21prov[iday, iprov]

                else:
                    for iprov in range(0, len(WRD_PROV_NUM[icountry])):  # for all other big countries' provinces
                        tmp = prov_curday2019[iprov, :, :]
                        tmp21 = prov_curday2021[iprov, :, :]
                        tmp[WRD_PROV == WRD_PROV_NUM[icountry][iprov]] = currentday2019[
                            WRD_PROV == WRD_PROV_NUM[icountry][iprov]]
                        tmp21[WRD_PROV == WRD_PROV_NUM[icountry][iprov]] = currentday2021[
                            WRD_PROV == WRD_PROV_NUM[icountry][iprov]]
                        tmp[tmp < 0] = 0  # negative vaues --> 0
                        tmp[tmp21 < 0] = 0
                        tmp[tmp < np.percentile(tmp, 95)] = 0  # below top5% vaues --> 0

                        NO2_19prov[iday, iprov] = np.average(tmp[tmp > 0])
                        NO2_21prov[iday, iprov] = np.average(tmp21[tmp > 0])

                        R_1day[WRD_PROV == WRD_PROV_NUM[icountry][iprov]] = NO2_19prov[iday, iprov]
                        R_2day[WRD_PROV == WRD_PROV_NUM[icountry][iprov]] = NO2_21prov[iday, iprov]

            R[idays, :, :] = R_1day[:, :]
            R21[idays, :, :] = R_2day[:, :]
    return R, R21


# 读取no2 daily文件 # 感觉可以和上一步合起来
def read_no2_2019():
    R_2019 = np.zeros((366, 1800, 3600))
    for iday in range(366):
        Rindex = Dataset(os.path.join(no2_daily_path, 'R%s_d%2.3i.nc' % (2019, iday + 1)))
        R_2019[iday, :, :] = Rindex.variables['R%s' % 2019][:, :]
    return R_2019


def read_no2_rest(year):
    no2_file = search_file(no2_path)
    if year != 2019:
        file_name = [no2_file[i] for i, x in enumerate(no2_file) if x.find('%s' % year) != -1]  # 文件长度 也就是那一年的数据月份有几个
        mdays = create_year_days(year, file_name)
        mdays[1] = 29
        R_rest = np.zeros((mdays.sum(), 1800, 3600))
        for iday in range(mdays.sum()):
            Rindex = Dataset(os.path.join(no2_daily_path, 'R%s_d%2.3i.nc' % (year, iday + 1)))
            R_rest[iday, :, :] = Rindex.variables['R%s' % year][:, :]
        return R_rest


def final_sum():
    for y in range(2019, end_year + 1):
        file_name = search_file(first_path)
        year_file = [file_name[i] for i, x in enumerate(file_name) if x.find('_y%s_' % y) != -1]
        for s in range(8):
            sector_file = [year_file[i] for i, x in enumerate(year_file) if x.find('_%s_' % s) != -1]
            mdays = create_year_days(y, sector_file)
            for imon in range(len(sector_file)):
                days2022 = np.zeros((mdays[imon], 1800, 3600))
                for isect in range(len(new_sector_list)):
                    out_path = os.path.join(first_path,
                                            'CarbonMonitor_%i_%s' % (isect, new_sector_list[isect]),
                                            'CarbonMonitor_%i_%s_y%s_m%2.2i.nc' % (
                                                isect, new_sector_list[isect], y, imon + 1))
                    nc_obj = Dataset(out_path)

                    obj2022 = nc_obj.variables[u'emission'][:, :, :]
                    for iday in range(mdays[imon]):
                        obj2022[iday, :, :] = np.nan_to_num(obj2022[iday, :, :])
                        days2022[iday, :, :] += obj2022[iday, :, :]
                final_output = os.path.join(final_path, 'CarbonMonitor_total_y%s_m%2.2i.nc' % (y, imon + 1))

                resultfile1 = Dataset(final_output, 'w')

                var = resultfile1.createDimension('latitude', 1800)
                var = resultfile1.createDimension('longitude', 3600)
                var = resultfile1.createDimension('nday', mdays[imon])
                var = resultfile1.createVariable('latitude', 'd', 'latitude')
                var[:] = np.arange(89.95, -90, -0.1)
                var = resultfile1.createVariable('longitude', 'd', 'longitude')
                var[:] = np.arange(-179.95, 180, 0.1)
                total2022 = resultfile1.createVariable('emission', 'd', ('nday', 'latitude', 'longitude'))
                setattr(total2022, 'units', "kgC/h")
                total2022[:] = days2022[:]
                resultfile1.close()


def change_file_name():
    old_sector = ['Power', 'Industry', 'Residential', 'Ground Transport', 'International Aviation',
                  'International Shipping', 'Domestic Aviation']

    new_sector = ['Power', 'Industry', 'Residential', 'GroundTransportation', 'InternationalAviation',
                  'InternationalShipping', 'DomesticAviation']
    file_name = search_file(first_path)
    for s, k in zip(old_sector, new_sector):
        if ' ' in s:
            name = [file_name[i] for i, x in enumerate(file_name) if x.find('%s_' % s) != -1]
            for n in name:
                new_name = n.replace('%s_' % s, '%s_' % k)
                os.rename(n, new_name)


def shrink_etc():
    first_part = search_file(first_path)
    second_part = search_file(final_path)
    all_part = first_part + second_part
    name = re.compile(r'CarbonMonitor.*?_y(?P<year>.*?)_m(?P<month>.*?).nc')
    all_part = [all_part[i] for i, x in enumerate(all_part) if x.find('.nc') != -1]
    for a in all_part:
        with xr.open_dataset(a, engine='netcdf4') as data:
            ds = data.load()
            # 添加attributes
            ds.attrs['unit'] = 'kgC/h'
            ds.attrs['website'] = 'https://carbonmonitor-graced.com/'
            ds.attrs['contact_info_1'] = 'zhuliu@tsinghua.edu.cn'
            ds.attrs['contact_info_2'] = 'douxy19@mails.tsinghua.edu.cn'
            ds.attrs['citation_1'] = 'Dou, X., Wang, Y., Ciais, P., et al. Near-real-time global gridded daily CO2 ' \
                                     'emissions. The Innovation 3(1), 100182 (' \
                                     '2022).https://doi.org/10.1016/j.xinn.2021.100182 '
            ds.attrs['citation_2'] = 'Liu, Z., Ciais, P., Deng, Z. et al. Near-real-time monitoring of global CO2 ' \
                                     'emissions reveals the effects of the COVID-19 pandemic. Nat Commun 11, ' \
                                     '5172 (2020).https://doi.org/10.1038/s41467-020-18922-7 '
            ds.attrs[
                'citation_3'] = 'Liu, Z., Ciais, P., Deng, Z. et al. Carbon Monitor, a near-real-time daily dataset ' \
                                'of global CO2 emission from fossil fuel and cement production. Sci Data 7, ' \
                                '392 (2020).https://doi.org/10.1038/s41597-020-00708-7 '

            # 改为年月日日期格式
            year = name.findall(a)[0][0]
            month = name.findall(a)[0][1]
            date = int('%s%s01' % (year, month))
            ds = ds.assign(nday=lambda x: pd.to_datetime(x.nday + date, format='%Y%m%d'))
            # 重新输出
            data.close()
            ds.to_netcdf(a, encoding={"emission": {"dtype": 'f4'}})


def file_to_zip():
    # 找到所有文件
    first_part = search_file(first_path)
    second_part = search_file(final_path)
    all_part = first_part + second_part
    all_part = [all_part[i] for i, x in enumerate(all_part) if x.find('.nc') != -1]
    for a in all_part:
        # 改下名
        final_name = re.findall(r'([^/]*)$', a)[0].replace('CarbonMonitor', 'GRACED')
        final_zip = os.path.join(zip_path, '%s.tar.gz' % final_name)
        # 压缩
        with tarfile.open(final_zip, "w:gz") as tar:
            tar.add(a, arcname=os.path.basename(a))


if __name__ == '__main__':
    main()
