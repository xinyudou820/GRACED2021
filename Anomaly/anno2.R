library(dplyr)
library(ggplot2)
library(tidyverse)
library(openxlsx)
library(stringr)

data = openxlsx::read.xlsx("./ANNO/ALL_RES.csv")

colnames(data)
key_list = unique(data$key)
sDtae_list = c("2019","2020","2021")
cont_list = colnames(data)[5:13]

dataL1 = data %>%
  dplyr::mutate(yyyy = str_sub(sDate,1,4)) %>%
  dplyr::mutate(mm = str_sub(sDate,5,6)) %>%
  #dplyr::mutate(dtDate = as.Date(dtDate,origin="1900-01-01") - 2) 
  dplyr::group_by(mm,key) %>%
  dplyr::summarise(land = sum(land,na.rm = T) * 24.0,
                   sea = sum(sea,na.rm = T) * 24.0 ,
                   Africa = sum(Africa,na.rm = T) * 24.0,
                   Antarctica = sum(Antarctica,na.rm = T) * 24.0,
                   Asia = sum(Asia,na.rm = T) * 24.0,
                   Australia = sum(Australia,na.rm = T) * 24.0,
                   Europe = sum(Europe,na.rm = T) * 24.0,
                   NorthAmerica = sum(NorthAmerica,na.rm = T) * 24.0,
                   SouthAmerica = sum(SouthAmerica,na.rm = T) * 24.0) %>%
  dplyr::mutate(monthName = case_when(
    mm == "01" ~ "J",
    mm == "02" ~ "F",
    mm == "03" ~ "M",
    mm == "04" ~ "A",
    mm == "05" ~ "M",
    mm == "06" ~ "J",
    mm == "07" ~ "J",
    mm == "08" ~ "A",
    mm == "09" ~ "S",
    mm == "10" ~ "O",
    mm == "11" ~ "N",
    mm == "12" ~ "D",
  ))

anomaly_res = data.frame()

for (i in key_list) {
  
  dataL2 = dataL1 %>%
    dplyr::filter(key == i) 
  
  for (j in 1:1) {
    
    dataL3 = dataL2 
      #dplyr::filter(yyyy == j) 
    
    for (k in cont_list) {
      
      dataL4 = dataL3 %>%
        dplyr::mutate(yyyy = 9999) %>%
        dplyr::select("yyyy","mm",k,"monthName") 
      
      
     
      
      
      dataL4[[k]] = dataL4[[k]] 
      
      
      sds = sd(dataL4[[k]])
      
      
      means = mean(dataL4[[k]])
      sds/means
      
      anomalys = ((dataL4[[k]] - means)/means)*100
      
      
      anomaly_res_part = data.frame(yyyy = dataL4$yyyy, 
                                    mm = dataL4$mm,
                                    date = as.Date(paste0(dataL4$yyyy,"-",dataL4$mm,"-01")),
                                    key = i,
                                    anomaly = anomalys,
                                    month_name = dataL4$monthName,
                                    contin_name = rep(k,nrow(dataL4)))
      
      
      
      if(nrow(anomaly_res) == 0) {
        anomaly_res = anomaly_res_part
      } else {
        anomaly_res = rbind(anomaly_res,anomaly_res_part)
      }
      
    }
    
  }
  
}

anomaly_res = anomaly_res %>%
  dplyr::mutate(anomaly = ifelse(is.na(anomaly),0,anomaly)) %>%
  dplyr::filter(yyyy == 2019)

anomaly_res2020 = anomaly_res %>%
  dplyr::mutate(anomaly = ifelse(is.na(anomaly),0,anomaly)) %>%
  dplyr::filter(yyyy == 2020)

anomaly_res2021 = anomaly_res %>%
  dplyr::mutate(anomaly = ifelse(is.na(anomaly),0,anomaly)) %>%
  dplyr::filter(yyyy == 2021)




write.csv(anomaly_res2021,"./anomalyForMonth_2021.csv",row.names = F)




