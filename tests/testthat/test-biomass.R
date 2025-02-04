# Test biomass() ----------------------------------------------------------

skip_on_cran()

data(fiaRI)
data(countiesRI)

fiaRI_mr <- clipFIA(fiaRI)

# Test 1 ------------------------------
# Return aboveground biomass for all forestland by county and return spatial object
out <- biomass(db = fiaRI_mr, polys = countiesRI, returnSpatial = TRUE)
plot.out <- plotFIA(out, CARB_ACRE)
test_that("out is correct", {
  expect_s3_class(out, 'sf')
})
test_that('plot.out is a ggplot', {
  expect_s3_class(plot.out, 'gg')
})

# Test 2 ------------------------------
# Biomass by component for most recent survey on timberland for growing stock trees
out <- biomass(db = fiaRI_mr, byComponent = TRUE, landType = 'timber', 
               treeType = 'gs')
test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})
# Check to make sure foliar carbon is 0
foliar.c <- out$CARB_ACRE[out$COMPONENT == 'FOLIAGE']
test_that('foliar C is set to 0', {
  expect_equal(foliar.c, 0)
})

# Test 3 ------------------------------
# Biomass for multiple components for multiple species
out <- biomass(db = fiaRI_mr, bySpecies = TRUE, component = c('ROOT', 'FOLIAGE'))
test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})

# Test 4 ------------------------------
# AG biomass (including foliage) on timberland for AGS species, by plot
out <- biomass(db = fiaRI_mr, landType = 'timber', treeType = 'gs', component = c('AG', 'FOLIAGE'),
               byPlot = TRUE)
test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})

# Test 5 ------------------------------
# Belowground (i.e., coarse roots) and stump biomass only 
out <- biomass(db = fiaRI, component = c('ROOT', 'STUMP'))

test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})

# Test 6 ------------------------------
# AGB by size-class
out <- biomass(db = fiaRI, bySizeClass = TRUE)

test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})
