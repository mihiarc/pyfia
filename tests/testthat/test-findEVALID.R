# Test findEVALID() -------------------------------------------------------

skip_on_cran()

data(fiaRI)
data(countiesRI)

# Test 1 ------------------------------
# Most recent for all evaluation types
out <- findEVALID(db = fiaRI, mostRecent = TRUE, state = 'Rhode Island', type = 'ALL')

test_that("out is correct", {
  expect_equal(out, 441800)
})

# Test 2 ------------------------------
# 2014 EVALID for RI for VOL 
out <- findEVALID(db = fiaRI, mostRecent = FALSE, state = 'Rhode Island', type = 'VOL', year = 2014)

test_that("out is correct", {
  expect_equal(out, 441401)
})

