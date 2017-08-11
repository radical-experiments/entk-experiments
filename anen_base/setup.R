# set up file for ENTK and AnEN
#
# this file configures the initial arguments for the AnEn executable
# the function will be called from the master.py script
#
initial_config <- function (verbose = F) {
  current_stage <- 1
  init.num.pixels <- 10
  nrows <- 100
  ncols <- 100
  
  file.forecast <- '/home1/04672/tg839717/data/Temperature_NAM/Forecasts_NAM_C.nc'
  file.observation <- '/home1/04672/tg839717/data/Temperature_NAM/Analysis_NAM_R.nc'
  
  test.ID.start <- 700
  test.ID.end <- 799
  train.ID.start <- 0
  train.ID.end <- 699
  members.size <- 20
  rolling <- -2
  cores <- 16
  
  output.prefix <- paste('/home1/04672/tg839717/data/Temperature_NAM/stage_', current_stage, '_', sep = '')
  output.AnEn <- paste(output.prefix, 'anen.nc', sep = '')
  #output.computed.pixels <- paste(output.prefix, 'computed_pixels.rdata', sep = '')
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
  
  # save the index numbers of computed pixels
  #save(stations.ID, file = output.computed.pixels)
  
  list.init.config <- list(file.forecast = file.forecast,
                           file.observation = file.observation,
                           output.AnEn = output.AnEn,
                           stations.ID = stations.ID,
                           test.ID.start = test.ID.start,
                           test.ID.end = test.ID.end,
                           train.ID.start = train.ID.start,
                           train.ID.end = train.ID.end,
                           rolling = rolling,
                           members.size = members.size,
                           cores = cores)
  
  if (verbose) {
    print(list.init.config)
  }
  
  return(list.init.config)
}
