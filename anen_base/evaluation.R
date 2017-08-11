# evaluation file for ENTK and AnEn
#
# This file does the following steps for the current stage
# - read observations (analysis in this case) as a raster
# - read AnEn results from the current stage
# - interpolate the results to a 100 * 100 raster
# - compute errors for the current AnEn results
# - determine the pixels to compute the next stage
#
if (!require(sp)) {
  install.packages('sp')
  library(sp)
}
if (!require(ncdf4)) {
  install.packages('ncdf4')
  library(ncdf4)
}
if (!require(raster)) {
  install.packages('raster')
  library(raster)
}

current.stage <- 1
test.ID.start <- 700
test.ID.end <- 705
nflts <- 8
nrows <- 100
ncols <- 100
rast.base <- raster(nrows = nrows, ncols = ncols, xmn = 0.5, xmx = ncols+.5, ymn = 0.5, ymx = nrows+.5)
number.neighbors <- 2
threshold.RMSE <- 2


#######################
# function definition #
#######################
nearest.neighbor.interpolation <- function( x,y,z, rast.base, lonlat=F, n=5 ) {
  # This function use the Nearest neighbour interpolation to project a set of
  # points in x,y,z format to a raster.  It returns the raster with the interpolated
  # values.  The value n is the number of closet points that will be used for the
  # interpolation.
  #
  # Source: http://rspatial.org/analysis/rst/4-interpolation.html
  #
  rast.res  <- rast.base
  
  # control points.  These are the points on the raster that we want to 
  # interpolate to.  Each of the control point will be assigned a value according to 
  # the nearest x,y point in the input
  #
  cp <- rasterToPoints( rast.base )
  
  # Distance matrix
  #
  d  <- pointDistance(cp[, 1:2], cbind(x,y), lonlat=lonlat)
  
  # Get the values of the closest n points
  #
  ngb <- t(apply(d, 1, function(x) order(x)[1:n]))
  
  pairs  <- cbind(rep(1:nrow(ngb), n), as.vector(ngb))
  values <- z[pairs[,2]]
  pn     <- tapply(values, pairs[,1], mean)
  
  values(rast.res)  <- as.vector(pn)
  
  return(rast.res)
}

compute.triangle.errors <- function(control.coords.x, control.coords.y,
                                    rast.obs, rast.pred) {
  # this function evaluate the prediction confidence for a triangle area
  # defined by the control points and Delaunay triangulation. The confidence
  # is measured by the absolute RMSE and the difference betweent the RMSE of
  # the control points of each triangle
  #
  df <- data.frame(x = control.coords.x, y = control.coords.y)
  
  # need a boundary window
  W <- ripras(df, shape="rectangle") 
  
  # create point pattern
  # compute Dirichlet triangles
  # convert to SpatialPolygons
  #
  polys <- as(delaunay(as.ppp(df, W=W)), "SpatialPolygons")
  
  # compute triangle errors
  mat.errors <- matrix(NA, nrows = length(polys), ncol = 2)
  colnames(mat.errors) <- c('RMSE', 'RMSE.diff')
  for (i in 1 : length(polys)) {
    
    # get coordinates of the control points
    control.coords <- polys[i]@polygons[[1]]@Polygons[[1]]@coords
    
    # get rid of the last row because it is the same with the first row
    control.coords <- control.coords[1 : (dim(control.coords)[1] - 1), ]
    
    # get values at the control points
    control.obs <- rast.obs[control.coords[, 1], control.coords[, 2]]
    control.pred <- rast.pred[control.coords[, 1], control.coords[, 2]]
    
    # compute errors
    mat.errors[i, 'RMSE'] = sqrt( mean( (control.obs - control.pred)^2, na.rm = T) )
    mat.errors[i, 'RMSE.diff'] = mean( abs( control.obs - control.pred), na.rm = T)
  }
  
  return(mat.errors)
}

##################
# compute errors #
##################
#
# read observations
file.observation <- 'Analysis_NAM_R.nc'
nc <- nc_open(file.observation)
obs <- ncvar_get(nc, 'Data')
nc_close(nc)

# read AnEn results
file.AnEn <- paste('AnEn_result_stage_', current.stage, '.nc', sep = '')
nc <- nc_open(file.AnEn)
analogs <- ncvar_get(nc, 'Data')
nc_close(nc)

# read computed pixel index numbers
file.pixels.computed <- paste('computed_pixels_stage_', current.stage, '.rdata', sep = '')
load(file.pixels.computed)

# compute the actual coordinates of computed pixels
x <- (stations.ID %% ncols) + 1
y <- ceiling(stations.ID / ncols) + 1

# compute averaged values across all members
num.computed.pixels <- dim(analogs)[2]
analogs <- apply(analogs, c(1, 2, 3), mean, na.rm = T)

# loop through days and flts to compute errors
mat.RMSE <- matrix(NA, nrow = test.ID.end - test.ID.start + 1, ncol = nflts)
rownames(mat.RMSE) <- paste(test.ID.start : test.ID.end)
colnames(mat.RMSE) <- paste(1 : nflts)
for (day in test.ID.start : test.ID.end) {
  for (flt in 1 : nflts) {
    
    # get observations for the day and flt
    coords   <- expand.grid(1 : nrow(rast.base), 1 : ncol(rast.base))
    rast.obs <- rasterize(coords, rast.base, field = obs[, day, flt])
    
    # interpolate the AnEn prediction for the day and flt
    if (num.computed.pixels < nrows * ncols) {
      rast.int <- nearest.neighbor.interpolation(x, y, analogs[, day, flt], rast.base, n = number.neighbors)
    } else {
      rast.int = rasterize(cbind(x, y), rast.base, analogs[, day, flt])
    }
    
    # compute error
    mat.RMSE[as.character(day), as.character(flt)] <- sqrt( mean( values( (rast.obs - rast.int)^2), na.rm=T) )
  }
}


###################
# evaluate errors #
###################
if (mean(mat.RMSE) > threshold.RMSE) {
  # compute more pixels
  stations.ID <- sample.int(nrows * ncols, num.computed.pixels * 2)
  
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
  
  # return pixel index numbers here #
  
} else {
  # stop
  # return empty list of pixel index numbers
}