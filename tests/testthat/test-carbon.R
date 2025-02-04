# Test biomass() ----------------------------------------------------------

skip_on_cran()

data(fiaRI)
data(countiesRI)

fiaRI_mr <- clipFIA(fiaRI)

# Test 1 ------------------------------
# Return carbon for all forestland by county and return spatial object
out <- carbon(db = fiaRI_mr, polys = countiesRI, returnSpatial = TRUE, byPool = FALSE)
plot.out <- plotFIA(out, CARB_ACRE)
test_that("out is correct", {
  expect_s3_class(out, 'sf')
})
test_that('plot.out is a ggplot', {
  expect_s3_class(plot.out, 'gg')
})

# Test 2 ------------------------------
# Carbon by pool and component for most recent survey on timberland
out <- carbon(db = fiaRI_mr, byPool = TRUE, byComponent = TRUE, landType = 'timber')
test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})

# Test 3 ------------------------------
# Carbon on all land by pool 
out <- carbon(db = fiaRI_mr, byPool = TRUE, landType = 'all')
test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})

# Test 4 ------------------------------
# carbon on timberland by plot 
out <- carbon(db = fiaRI_mr, landType = 'timber', byPlot = TRUE)
test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})

# Test 5 ------------------------------
out <- carbon(db = fiaRI)

test_that('out is correct', {
  expect_s3_class(out, 'tbl_df')
})

# Test 6 ------------------------------
# Over time with method = 'LMA'
out <- carbon(db = fiaRI, method = 'LMA', polys = countiesRI)
