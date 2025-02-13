# Test tpa() --------------------------------------------------------------

skip_on_cran()

data(fiaRI)
data(countiesRI)

# Most recent subset
fiaRI_mr <- clipFIA(fiaRI)

# Test 1 ------------------------------
out <- fsi(db = fiaRI_mr, scaleBy = FORTYPCD)

test_that("out is of class tbl_df", {
  expect_s3_class(out, "tbl_df")
})

# Test 2 ------------------------------
out <- fsi(db = fiaRI_mr, scaleBy = FORTYPCD,
           byPlot = TRUE)

test_that("out is of class tbl_df", {
  expect_s3_class(out, "tbl_df")
})

# Test 3 ------------------------------
results <- fsi(db = fiaRI_mr,
               scaleBy = FORTYPCD,
               returnBetas = TRUE)

test_that("results is a tbl_df", {
  expect_s3_class(results$results, "tbl_df")
})

test_that("results has two things", {
  expect_equal(length(results), 2)
})

# Test 4 ------------------------------
out <- fsi(fiaRI_mr,
           scaleBy = SITECLCD,
           treeType = 'live',
           treeDomain = SPCD == 129 & DIA > 12,
           areaDomain = PHYSCLCD %in% 21:29) 
test_that("out is of class tbl_df", {
  expect_s3_class(out, "tbl_df")
})
