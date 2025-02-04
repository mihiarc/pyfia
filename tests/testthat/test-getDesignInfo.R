# Test volume() -----------------------------------------------------------

skip_on_cran()

data(fiaRI)
data(countiesRI)

# Test 1 ------------------------------
# Most recent for all evaluation types
out <- getDesignInfo(db = fiaRI, mostRecent = TRUE)

test_that("out is of class tbl_df", {
  expect_s3_class(out, "tbl_df")
})

# Test 2 ------------------------------
# All years for all VOL evaluation type
out <- getDesignInfo(db = fiaRI, mostRecent = FALSE, type = 'VOL')

test_that("out is of class tbl_df", {
  expect_s3_class(out, "tbl_df")
})

# Test 3 ------------------------------
# A specific EVALID
out <- getDesignInfo(db = fiaRI, mostRecent = FALSE, evalid = '441800')

test_that("out is of class tbl_df", {
  expect_s3_class(out, "tbl_df")
})

# Test 4 ------------------------------
# Get an error
test_that('gives error', {
  expect_error(getDesignInfo(db = fiaRI, type = 'blah'))
})
