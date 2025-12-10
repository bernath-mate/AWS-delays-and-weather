library(dplyr)
library(readr)
library(stringr)
library(jsonlite)

## READ-IN STATION_IDs from LOOKUP JSON
stations_lookup <- fromJSON("stations_ids.json")

# DEFINE MAIN PART AS FUNCTION
process_delay_data <- function(input_file, start_date, end_date, output_file) {
  print(paste("processing:", start_date, "to", end_date))
  
  raw_delay_data <- readRDS(input_file)
  
  # Filter by date range
  raw_delay_data <- raw_delay_data[
    raw_delay_data$Datum >= start_date & 
    raw_delay_data$Datum <= end_date, 
  ]
  
  # Extract station names based on Tipus
  raw_delay_data <- raw_delay_data %>%
    mutate(
      station_name = case_when(
        Tipus == "Szakasz" ~ NA_character_,
        Tipus %in% c("InduloAllomas", "KozbensoAllomas") ~ Indulo,
        Tipus == "ZaroSzakasz" ~ Erkezo,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(station_name)) %>%
    filter(station_name != "")
  
  # Rename and clean columns
  raw_delay_data <- raw_delay_data %>%
    rename(date = Datum, delay = Keses) %>%
    select(date, station_name, delay) %>%
    mutate(
      date = as.Date(date),
      station_name = str_trim(str_replace_all(station_name, '"', "")),
      delay = as.numeric(delay)
    )
  
  # FILTER: Keep ONLY positive delays (delay >= 0)
  total_before <- nrow(raw_delay_data)
  raw_delay_data <- raw_delay_data %>%
    filter(delay >= 0)
  total_after <- nrow(raw_delay_data)
  removed <- total_before - total_after
  
  print(paste("Removed", removed, "early arrivals (delay < 0)"))
  print(paste("Kept", total_after, "on-time or delayed records"))
  print(paste("Percentage kept:", round(100 * total_after / total_before, 1), "%"))
  
  # Aggregate by date and station
  delay_aggregated <- raw_delay_data %>%
    group_by(date, station_name) %>%
    summarise(
      total_delay_minutes = sum(delay, na.rm = TRUE),
      train_count = n(),
      avg_delay_minutes = mean(delay, na.rm = TRUE),
      .groups = 'drop'
    ) %>%
    arrange(date, station_name)
  
  # Join with station lookup
  delay_aggregated <- delay_aggregated %>%
    left_join(stations_lookup, by = "station_name") %>%
    select(
      date,
      station_id,
      station_name,
      total_delay_minutes,
      train_count,
      avg_delay_minutes
    ) %>%
    arrange(date, station_name)
  
  write_csv(delay_aggregated, output_file, na = "")
  print(paste("success:", output_file))
  print(paste("Total rows written:", nrow(delay_aggregated)))
}

# RUNS
process_delay_data("ProcData.rds", "2025-06-01", "2025-11-23", "delay_data_initial.csv")
process_delay_data("ProcData.rds", "2025-11-24", "2025-11-30", "delay_data_2025-11-24_2025-11-30.csv")
process_delay_data("ProcData.rds", "2025-12-01", "2025-12-07", "delay_data_2025-12-01_2025-12-07.csv")