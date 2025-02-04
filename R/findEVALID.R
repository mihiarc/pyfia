findEVALID <- function(db = NULL,
                       mostRecent = FALSE,
                       state = NULL,
                       year = NULL,
                       type = NULL){

  # Join POP_EVAL with POP_EVAL_TYP to get the evaluation type for each EVALID
  ids <- db$POP_EVAL %>%
    dplyr::left_join(dplyr::select(db$POP_EVAL_TYP, c('EVAL_CN', 'EVAL_TYP')), 
                     by = c('CN' = 'EVAL_CN')) %>%
    dplyr::mutate(place = stringr::str_to_upper(LOCATION_NM))

  # Only grab EVALIDs from states of interest
  if (!is.null(state)){
    state <- stringr::str_to_upper(state)
    evalGrp <- intData$EVAL_GRP %>%
      dplyr::select(STATECD, STATE) %>%
      dplyr::mutate(STATECD = as.numeric(STATECD)) %>% 
      unique()
    # Join state abbs with state codes in popeval
    ids <- dplyr::left_join(ids, evalGrp, by = 'STATECD')
    # Check if any specified are missing from db
    if (any(unique(state) %in% evalGrp$STATE == FALSE)){
      missStates <- state[state %in% evalGrp$STATE == FALSE]
      fancyName <- unique(intData$EVAL_GRP$STATE[intData$EVAL_GRP$STATECD %in% missStates])
      stop(paste('States: ', toString(fancyName) , 'not found in db.', sep = ''))
    }
    ids <- dplyr::filter(ids, STATE %in% state)
  }
  # Only grab EVALIDs from years of interest
  if (!is.null(year)){
    ids <- dplyr::filter(ids, END_INVYR %in% year)
  }
  # Only grab EVALIDs from evaluation types of interest
  if (!is.null(type)){
    ids <- dplyr::filter(ids, EVAL_TYP %in% paste0('EXP', type))
  }
  # Only grab most recent EVALIDs if desired
  if (mostRecent) {
    # Grouped filter wasn't working as intended, use filtering join
    maxYear <- ids %>%
      ## Remove TX, do it seperately
      dplyr::filter(!(STATECD %in% 48)) %>%
      dplyr::mutate(place = stringr::str_to_upper(LOCATION_NM)) %>%
      dplyr::group_by(place, EVAL_TYP) %>%
      dplyr::summarize(END_INVYR = max(END_INVYR, na.rm = TRUE),
                       LOCATION_NM = dplyr::first(LOCATION_NM))

    # Texas coding standards are very bad
    # Name two different inventory units with 5 different names
    # Due to that, only use inventories for the ENTIRE state, sorry
    if (any(ids$STATECD %in% 48)){

      # Will require manual updates if Texas fixes this.
      txIDS <- ids %>%
        dplyr::filter(STATECD %in% 48) %>%
        dplyr::filter(END_INVYR < 2017) %>%
        dplyr::filter(END_INVYR > 2006) %>%
        # Removing any inventory that references east or west, sorry
        dplyr::filter(stringr::str_detect(stringr::str_to_upper(EVAL_DESCR), 'EAST', negate = TRUE) &
                      stringr::str_detect(stringr::str_to_upper(EVAL_DESCR), 'WEST', negate = TRUE)) %>%
        dplyr::mutate(place = stringr::str_to_upper(LOCATION_NM)) %>%
        dplyr::group_by(place, EVAL_TYP) %>%
        dplyr::summarize(END_INVYR = max(END_INVYR, na.rm = TRUE),
                         LOCATION_NM = dplyr::first(LOCATION_NM))

      maxYear <- dplyr::bind_rows(maxYear, txIDS)
    }

    ids <- dplyr::left_join(maxYear, dplyr::select(ids, c('place', 'EVAL_TYP', 'END_INVYR', 'EVALID')),
                            by = c('place', 'EVAL_TYP', 'END_INVYR'))
  }

  # Output as vector
  ID <- unique(ids$EVALID)

  return(ID)
}

