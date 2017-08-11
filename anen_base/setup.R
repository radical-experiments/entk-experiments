# set up file for ENTK and AnEN
#
# this file configures the initial arguments for the AnEn executable
#
init.num.pixels <- 10
nrows <- 100
ncols <- 100
stations.ID <- sample.int(nrows * ncols, init.num.pixels)

# make sure that the points at the four corners are included
if (!(0 %in% stations.ID)) {
  stations.ID[1] <- 0
}
if (!(99 %in% stations.ID)) {
  stations.ID[2] <- 99
}
if (!(9900 %in% stations.ID)) {
  stations.ID[3] <- 9900
}
if (!(9999 %in% stations.ID)) {
  stations.ID[4] <- 9999
}

test.ID.start <- 700
test.ID.end <- 799
train.ID.start <- 0
train.ID.end <- 699
members.size <- 20
cores <- 16
output.file <- paste('res_pixels_', init.num.pixels, '.nc', sep = '')

command <- '~/github/CAnalogsV2/build/canalogs -N -p --forecast-nc ~/geolab_storage_V2/data/NAM12KM/Forecasts_NAM_C.nc --observation-nc ~/geolab_storage_V2/data/NAM12KM/Analysis_NAM_R.nc --stations-ID [pixels-ID] --test-ID-start [test-ID-start] --test-ID-end [test-ID-end] --train-ID-start [train-ID-start] --train-ID-end [train-ID-end] --number-of-cores [number-of-cores] --members-size [members-size] -o [output-file] >> log.txt'
command <- gsub('\\[pixels-ID\\]', paste(stations.ID, collapse = ' '), command)
command <- gsub('\\[test-ID-start\\]', test.ID.start, command)
command <- gsub('\\[test-ID-end\\]', test.ID.end, command)
command <- gsub('\\[train-ID-start\\]', train.ID.start, command)
command <- gsub('\\[train-ID-end\\]', train.ID.end, command)
command <- gsub('\\[members-size\\]', members.size, command)
command <- gsub('\\[number-of-cores\\]', cores, command)
command <- gsub('\\[output-file\\]', output.file, command)

# save the index numbers of computed pixels
save(stations.ID, file = paste('computed_pixels_stage_1.rdata', sep = ''))
