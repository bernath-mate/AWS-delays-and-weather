# LIBRARY & READ RDS DATA ---------------------------------------------------
library(jsonlite)
library(dplyr)
library(stringr)
library(readr)

stations_df <- readRDS("allomaskoord.rds")

# CLEAN NAME
stations_df <- stations_df %>%
  rename(station_name = Allomas) %>%
  mutate(
    station_name = str_trim(str_replace_all(station_name, '"', ""))
  )

# CREATE station_id FOR ALL STATIONS
stations_df <- stations_df %>%
  arrange(station_name) %>%
  mutate(station_id = row_number())

# RENAME AND SELECT COLUMNS
names(stations_df)[names(stations_df) == "Allomas"] <- "station_name"

# DEFINE COUNTY BOUNDING BOXES (LAT/LON RANGES) ------------------------------
COUNTY_BOUNDS <- list(
  "Budapest" = list(lat_min = 47.37, lat_max = 47.63, lon_min = 18.92, lon_max = 19.28),
  "Borsod-Abaúj-Zemplén" = list(lat_min = 47.8, lat_max = 48.7, lon_min = 20.2, lon_max = 21.5),
  "Heves" = list(lat_min = 47.3, lat_max = 48.2, lon_min = 19.7, lon_max = 20.8),
  "Nógrád" = list(lat_min = 47.7, lat_max = 48.7, lon_min = 18.8, lon_max = 20.2),
  "Hajdú-Bihar" = list(lat_min = 46.9, lat_max = 47.8, lon_min = 20.6, lon_max = 22.2),
  "Jász-Nagykun-Szolnok" = list(lat_min = 46.7, lat_max = 47.5, lon_min = 19.8, lon_max = 21.0),
  "Szabolcs-Szatmár-Bereg" = list(lat_min = 47.5, lat_max = 48.6, lon_min = 21.1, lon_max = 22.9),
  "Bács-Kiskun" = list(lat_min = 45.8, lat_max = 47.0, lon_min = 18.6, lon_max = 20.3),
  "Békés" = list(lat_min = 46.1, lat_max = 47.2, lon_min = 20.5, lon_max = 21.9),
  "Csongrád-Csanád" = list(lat_min = 46.1, lat_max = 47.0, lon_min = 19.8, lon_max = 20.9),
  "Fejér" = list(lat_min = 46.8, lat_max = 47.4, lon_min = 18.0, lon_max = 19.3),
  "Komárom-Esztergom" = list(lat_min = 47.3, lat_max = 47.9, lon_min = 17.5, lon_max = 18.9),
  "Veszprém" = list(lat_min = 46.6, lat_max = 47.8, lon_min = 17.0, lon_max = 18.6),
  "Győr-Moson-Sopron" = list(lat_min = 47.2, lat_max = 48.1, lon_min = 16.3, lon_max = 18.0),
  "Vas" = list(lat_min = 46.7, lat_max = 47.8, lon_min = 15.9, lon_max = 17.3),
  "Zala" = list(lat_min = 45.8, lat_max = 47.2, lon_min = 15.9, lon_max = 17.4),
  "Baranya" = list(lat_min = 45.8, lat_max = 46.6, lon_min = 17.2, lon_max = 18.9),
  "Somogy" = list(lat_min = 45.8, lat_max = 47.0, lon_min = 17.0, lon_max = 18.5),
  "Tolna" = list(lat_min = 46.2, lat_max = 46.9, lon_min = 18.2, lon_max = 19.4),
  "Pest" = list(lat_min = 47.0, lat_max = 48.0, lon_min = 18.6, lon_max = 20.2)
)

# DEFINE ASSIGNMENT FUNCTIONS ------------------------------------------------
assign_county <- function(lat, lon) {
  for (county_name in names(COUNTY_BOUNDS)) {
    bounds <- COUNTY_BOUNDS[[county_name]]
    if (lat >= bounds$lat_min & lat <= bounds$lat_max &
        lon >= bounds$lon_min & lon <= bounds$lon_max) {
      return(county_name)
    }
  }
  return(NA)
}

assign_region <- function(county) {
  region_map <- list(
    "Borsod-Abaúj-Zemplén" = "Észak-Magyarország",
    "Heves" = "Észak-Magyarország",
    "Nógrád" = "Észak-Magyarország",
    "Hajdú-Bihar" = "Észak-Alföld",
    "Jász-Nagykun-Szolnok" = "Észak-Alföld",
    "Szabolcs-Szatmár-Bereg" = "Észak-Alföld",
    "Bács-Kiskun" = "Dél-Alföld",
    "Békés" = "Dél-Alföld",
    "Csongrád-Csanád" = "Dél-Alföld",
    "Fejér" = "Közép-Dunántúl",
    "Komárom-Esztergom" = "Közép-Dunántúl",
    "Veszprém" = "Közép-Dunántúl",
    "Győr-Moson-Sopron" = "Nyugat-Dunántúl",
    "Vas" = "Nyugat-Dunántúl",
    "Zala" = "Nyugat-Dunántúl",
    "Baranya" = "Dél-Dunántúl",
    "Somogy" = "Dél-Dunántúl",
    "Tolna" = "Dél-Dunántúl",
    "Pest" = "Pest",
    "Budapest" = "Budapest"
  )
  region_map[[county]] %||% NA
}

assign_location_id <- function(region) {
  location_id_map <- list(
    "Észak-Magyarország" = 0,
    "Észak-Alföld"       = 1,
    "Dél-Alföld"         = 2,
    "Közép-Dunántúl"     = 3,
    "Nyugat-Dunántúl"    = 4,
    "Dél-Dunántúl"       = 5,
    "Budapest"          = 6,
    "Pest"              = 7
  )
  location_id_map[[region]] %||% NA
}

## ADD COUNTY, REGION, LOCATION_ID (AND STATION_ID ALREADY JOINED) ----------
stations_df$county      <- mapply(assign_county, stations_df$lat, stations_df$lon)
stations_df$region      <- mapply(assign_region, stations_df$county)
stations_df$location_id <- mapply(assign_location_id, stations_df$region)

## CHECK NAs
stations_df$station_name[is.na(stations_df$county)]
stations_df$station_name[is.na(stations_df$station_id)]

## EXPORT TO CSV -------------------------------------------------------------
write_csv(
  stations_df[, c("station_name", "lat", "lon", "county", "region", "location_id", "station_id")],
  "stations_regions.csv"
)
print("successful transformation, CSV exported")

# SAVE LOOKUP JSON FOR DELAY PREPROCESSOR
stations_lookup <- stations_df %>%
  select(station_id, station_name)

write_json(stations_lookup, "stations_ids.json", pretty = TRUE, auto_unbox = TRUE)
print("lookup JSON exported")