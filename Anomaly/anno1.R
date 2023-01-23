library(dplyr)
library(ggplot2)
library(tidyverse)
library(openxlsx)
library(stringr)
#######################################################
fns = Sys.glob("./ANNO/*.csv")

fnk = c("Domestic_Aviation",
        "Ground_Transportation",
        "Industry",
        "International_Aviation",
        "Shipping",
        "Power",
        "Residential",
        "Total")

cc = 0
data_part_full2 = data.frame()
for (f in fns[2:9]) {

  cc = cc + 1
  data = fread(f)
  data_part_full = data.frame()
  contkey = unique(data$cont)

  for (k in contkey) {

    if(nrow(data_part_full) == 0) {

      data_part = data %>%
        dplyr::filter(cont == k) %>%
        dplyr::mutate(month = sprintf("%02d",month)) %>%
        dplyr::mutate(day = sprintf("%02d",day)) %>%
        dplyr::mutate(dtDate = paste0(year,"-",month,"-",day, " 00:00:00")) %>%
        dplyr::mutate(sDate = paste0(year,month,day)) %>%
        dplyr::mutate(key = fnk[cc]) %>%
        dplyr::select(dtDate,sDate,year,key,sumVal)

      data_part_full = data_part
      colnames(data_part_full)[ncol(data_part_full)] = k


    } else {

      data_part = data %>%
        dplyr::filter(cont == k) %>%
        dplyr::mutate(month = sprintf("%02d",month)) %>%
        dplyr::mutate(day = sprintf("%02d",day)) %>%
        dplyr::mutate(dtDate = paste0(year,"-",month,"-",day, " 00:00:00")) %>%
        dplyr::mutate(sDate = paste0(year,month,day)) %>%
        dplyr::mutate(key = fnk[cc]) %>%
        dplyr::select(dtDate,sDate,year,key,sumVal)

      data_part_full = cbind(data_part_full,data_part$sumVal)
      colnames(data_part_full)[ncol(data_part_full)] = k


    }

  }

  data_part_full = data_part_full %>%
    dplyr::mutate(sea = V1) %>%
    dplyr::mutate(land = Africa+Antarctica+Asia+Australia+Europe+NorthAmerica+SouthAmerica) %>%
    dplyr::select(dtDate,sDate,year,key,land,sea,Africa,Antarctica,Asia,Australia,Europe,NorthAmerica,SouthAmerica)


  data_part_full2 = rbind(data_part_full2,data_part_full)


}

fwrite(data_part_full2,paste0("./ANNO/ALL_RES.csv"),row.names = F)
#######################################################
data = fread("./ANNO/ALL_RES.csv")

colnames(data)
key_list = unique(data$key)
sDtae_list = c("2019","2020","2021")
week_list = c("Mon","Tue","Wed","Thi","Fri","Sat","Sun")
cont_list = colnames(data)[5:13]

dataL1 = data %>%
  dplyr::mutate(yyyy = str_sub(sDate,1,4)) %>%
  #dplyr::filter(yyyy == 2021) %>%
  dplyr::mutate(mm = str_sub(sDate,5,6)) %>%
  dplyr::mutate(dtDate = as.Date(dtDate,origin="1900-01-01"))  %>%
  dplyr::mutate(week = weekdays(dtDate)) %>%
  dplyr::group_by(week,key) %>%
  dplyr::summarise(land = sum(land,na.rm = T) * 24.0,
                   sea = sum(sea,na.rm = T) * 24.0 ,
                   Africa = sum(Africa,na.rm = T) * 24.0,
                   Antarctica = sum(Antarctica,na.rm = T) * 24.0,
                   Asia = sum(Asia,na.rm = T) * 24.0,
                   Australia = sum(Australia,na.rm = T) * 24.0,
                   Europe = sum(Europe,na.rm = T) * 24.0,
                   NorthAmerica = sum(NorthAmerica,na.rm = T) * 24.0,
                   SouthAmerica = sum(SouthAmerica,na.rm = T) * 24.0) %>%
  # dplyr::mutate(monthName = case_when(
  #   mm == "01" ~ "J",
  #   mm == "02" ~ "F",
  #   mm == "03" ~ "M",
  #   mm == "04" ~ "A",
  #   mm == "05" ~ "M",
  #   mm == "06" ~ "J",
  #   mm == "07" ~ "J",
  #   mm == "08" ~ "A",
  #   mm == "09" ~ "S",
  #   mm == "10" ~ "O",
#   mm == "11" ~ "N",
#   mm == "12" ~ "D",
# )) %>%
dplyr::mutate(weekname = case_when(
  week == "Mon" ~ "M",
  week == "Tue" ~ "T",
  week == "Wed" ~ "W",
  week == "Thi" ~ "T",
  week == "Fri" ~ "F",
  week == "Sat" ~ "S",
  week == "Sun" ~ "S"
)) 

anomaly_res = data.frame()

for (i in key_list) {
  
  dataL2 = dataL1 %>%
    dplyr::filter(key == i) 
  
  for (j in 2021) {
    
    dataL3 = dataL2 
      #dplyr::filter(year == j) 
    
    for (k in cont_list) {
      
      dataL4 = dataL3 %>%
        dplyr::select(k,"week","weekname") 
      
      
      
     
      dataL4[[k]] = dataL4[[k]] 
      
      
      sds = sd(dataL4[[k]])
      
      
      means = mean(dataL4[[k]])
      
      
      anomalys = ((dataL4[[k]] - means)/means)*100
      #anomalys = (dataL4[[k]] - means) / sds
      
      
      anomaly_res_part = data.frame(
                                    week = dataL4$week,
                                    key = i,
                                    anomaly = anomalys,
                                    weekname = dataL4$weekname,
                                    contin_name = rep(k,nrow(dataL4)))
      
      
      anomaly_res_part = anomaly_res_part %>%
        dplyr::mutate(weeknum = case_when(
          week == "Mon" ~ 1,
          week == "Tue" ~ 2,
          week == "Wed" ~ 3,
          week == "Thu" ~ 4,
          week == "Fri" ~ 5,
          week == "Sat" ~ 6,
          week == "Sun" ~ 0
        )) %>%
        dplyr::arrange(weeknum)
      
      
      
      if(nrow(anomaly_res) == 0) {
        anomaly_res = anomaly_res_part
      } else {
        anomaly_res = rbind(anomaly_res,anomaly_res_part)
      }
      
    }
    
  }
  
}

anomaly_res = anomaly_res %>%
  dplyr::mutate(anomaly = ifelse(is.na(anomaly),0,anomaly))


write.csv(anomaly_res,"./anomalyforweek_2019-2021.csv",row.names=F)




