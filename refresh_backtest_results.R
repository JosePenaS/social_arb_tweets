#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(stringr)
  library(lubridate)
  library(tidyr)
  library(glue)
  library(jsonlite)
  library(zoo)
  library(TTR)
})

# -----------------------------------------------------------------------------
# 0) helpers
# -----------------------------------------------------------------------------

`%||%` <- function(x, y) if (is.null(x)) y else x

as_logicalish <- function(x) {
  if (is.logical(x)) return(x)

  y <- tolower(trimws(as.character(x)))

  case_when(
    is.na(y) ~ NA,
    y %in% c("true", "t", "1", "yes", "y") ~ TRUE,
    y %in% c("false", "f", "0", "no", "n") ~ FALSE,
    TRUE ~ NA
  )
}

normalize_symbol_for_yahoo <- function(sym) {
  sym <- toupper(sym)
  gsub("\\.", "-", sym)
}

date_to_unix <- function(x, end_of_day = FALSE) {
  x <- as.POSIXct(as.Date(x), tz = "UTC")
  if (end_of_day) x <- x + 86399
  as.integer(x)
}

connect_supabase <- function() {
  host <- Sys.getenv("SUPABASE_POOLER_HOST")
  port <- as.integer(Sys.getenv("SUPABASE_PORT", "5432"))
  db   <- Sys.getenv("SUPABASE_DB", "postgres")
  user <- Sys.getenv("SUPABASE_USER")
  pwd  <- Sys.getenv("SUPABASE_PWD")

  if (!nzchar(host) || !nzchar(user) || !nzchar(pwd)) {
    stop("Missing one or more required env vars: SUPABASE_POOLER_HOST, SUPABASE_USER, SUPABASE_PWD")
  }

  dbConnect(
    RPostgres::Postgres(),
    host     = host,
    port     = port,
    dbname   = db,
    user     = user,
    password = pwd,
    sslmode  = "require"
  )
}

# -----------------------------------------------------------------------------
# 1) yahoo fetch + cache
# -----------------------------------------------------------------------------

fetch_yahoo_chart_syscurl <- function(sym, from, to, dest_json, retries = 4, debug = FALSE) {
  if (is.na(from) || is.na(to) || !nzchar(sym)) return(FALSE)

  sym <- normalize_symbol_for_yahoo(sym)

  period1 <- date_to_unix(from, end_of_day = FALSE)
  period2 <- date_to_unix(to, end_of_day = TRUE)

  url <- paste0(
    "https://query1.finance.yahoo.com/v8/finance/chart/", sym,
    "?period1=", period1,
    "&period2=", period2,
    "&interval=1d&includeAdjustedClose=true"
  )

  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) stop("curl not found on system")

  args <- c(
    "--http1.1",
    "--ipv4",
    "-L",
    "--compressed",
    "--silent",
    "--show-error",
    "--user-agent", "Mozilla/5.0",
    "--header", "Accept:application/json",
    "--output", dest_json,
    url
  )

  for (k in seq_len(retries)) {
    if (file.exists(dest_json)) unlink(dest_json)

    out <- tryCatch(
      system2(curl_bin, args = args, stdout = TRUE, stderr = TRUE),
      error = function(e) e
    )

    status <- if (inherits(out, "error")) 1L else attr(out, "status")
    if (is.null(status)) status <- 0L

    if (debug) {
      cat("Attempt:", k, "\n")
      cat("URL:", url, "\n")
      cat("Status:", status, "\n")
      cat("File exists:", file.exists(dest_json), "\n")
      if (file.exists(dest_json)) cat("File size:", file.info(dest_json)$size, "\n")
    }

    if (
      identical(as.integer(status), 0L) &&
      file.exists(dest_json) &&
      file.info(dest_json)$size > 0
    ) {
      txt <- paste(readLines(dest_json, warn = FALSE, encoding = "UTF-8"), collapse = "\n")

      if (
        grepl('"chart"', txt, fixed = TRUE) &&
        !grepl("Too Many Requests", txt, fixed = TRUE)
      ) {
        return(TRUE)
      }
    }

    Sys.sleep(min(2^k, 8))
  }

  FALSE
}

read_yahoo_chart_json <- function(path) {
  if (!file.exists(path)) return(tibble())

  txt <- tryCatch(
    paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n"),
    error = function(e) ""
  )

  if (!nzchar(txt)) return(tibble())

  js <- tryCatch(jsonlite::fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)

  if (is.null(js) || is.null(js$chart) || !is.null(js$chart$error)) return(tibble())

  res <- js$chart$result
  if (is.null(res) || length(res) == 0) return(tibble())

  res1 <- res[[1]]
  timestamps <- res1$timestamp
  quote_obj  <- res1$indicators$quote[[1]]

  if (is.null(timestamps) || is.null(quote_obj)) return(tibble())

  ts_vec    <- unlist(timestamps)
  open_vec  <- unlist(quote_obj$open)
  high_vec  <- unlist(quote_obj$high)
  low_vec   <- unlist(quote_obj$low)
  close_vec <- unlist(quote_obj$close)
  vol_vec   <- unlist(quote_obj$volume)

  min_len <- min(
    length(ts_vec), length(open_vec), length(high_vec),
    length(low_vec), length(close_vec), length(vol_vec)
  )

  if (min_len == 0) return(tibble())

  tibble(
    date   = as.Date(as.POSIXct(ts_vec[seq_len(min_len)], origin = "1970-01-01", tz = "UTC")),
    open   = as.numeric(open_vec[seq_len(min_len)]),
    high   = as.numeric(high_vec[seq_len(min_len)]),
    low    = as.numeric(low_vec[seq_len(min_len)]),
    close  = as.numeric(close_vec[seq_len(min_len)]),
    volume = as.numeric(vol_vec[seq_len(min_len)])
  ) %>%
    filter(!is.na(date)) %>%
    arrange(date)
}

get_px_via_syscurl_yahoo <- function(sym, from, to, debug = FALSE) {
  tmp_json <- tempfile(fileext = ".json")

  ok <- fetch_yahoo_chart_syscurl(
    sym = sym,
    from = from,
    to = to,
    dest_json = tmp_json,
    debug = debug
  )

  if (!ok) return(tibble())

  read_yahoo_chart_json(tmp_json) %>%
    filter(date >= as.Date(from), date <= as.Date(to)) %>%
    arrange(date)
}

get_px_cached <- function(sym, from, to, cache_dir = "cache_px", fetch_fn = get_px_via_syscurl_yahoo) {
  if (is.na(from) || is.na(to)) return(tibble())

  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)

  fname <- file.path(
    cache_dir,
    paste0(gsub("[^A-Za-z0-9_.-]", "_", sym), ".rds")
  )

  cached <- if (file.exists(fname)) {
    tryCatch(readRDS(fname), error = function(e) tibble())
  } else {
    tibble()
  }

  if (!is.data.frame(cached) || nrow(cached) == 0) {
    cached <- tibble()
  } else {
    cached <- cached %>%
      mutate(date = as.Date(date)) %>%
      filter(!is.na(date)) %>%
      arrange(date)
  }

  need_from <- as.Date(from)
  need_to   <- as.Date(to)

  need_fetch <- (
    nrow(cached) == 0 ||
      min(cached$date, na.rm = TRUE) > need_from ||
      max(cached$date, na.rm = TRUE) < need_to
  )

  if (need_fetch) {
    fresh <- tryCatch(
      fetch_fn(sym = sym, from = need_from, to = need_to),
      error = function(e) {
        message(sprintf("Fetch failed for %s [%s to %s]: %s", sym, need_from, need_to, e$message))
        tibble()
      }
    )

    if (nrow(fresh) > 0) {
      cached <- bind_rows(cached, fresh) %>%
        distinct(date, .keep_all = TRUE) %>%
        arrange(date)

      saveRDS(cached, fname)
    }
  }

  if (nrow(cached) == 0) return(tibble())

  cached %>%
    filter(date >= need_from, date <= need_to) %>%
    arrange(date)
}

# -----------------------------------------------------------------------------
# 2) strategy logic
# -----------------------------------------------------------------------------

pick_entry_from_px <- function(px, tweet_ts_et) {
  tweet_ts_et <- with_tz(as.POSIXct(tweet_ts_et, tz = "America/New_York"), "America/New_York")
  tweet_date  <- as.Date(tweet_ts_et)
  tweet_time  <- format(tweet_ts_et, "%H:%M:%S")

  available_dates <- px$date
  same_day_exists <- tweet_date %in% available_dates

  if (!same_day_exists) {
    next_date <- suppressWarnings(min(available_dates[available_dates > tweet_date], na.rm = TRUE))
    if (is.infinite(next_date)) next_date <- as.Date(NA)
    return(list(entry_date = next_date, entry_at = "open"))
  }

  if (tweet_time < "09:00:00") {
    return(list(entry_date = tweet_date, entry_at = "open"))
  }

  if (tweet_time < "15:30:00") {
    return(list(entry_date = tweet_date, entry_at = "close"))
  }

  next_date <- suppressWarnings(min(available_dates[available_dates > tweet_date], na.rm = TRUE))
  if (is.infinite(next_date)) next_date <- as.Date(NA)

  list(entry_date = next_date, entry_at = "open")
}

simulate_trade <- function(ticker,
                           tweet_ts_et,
                           side,
                           px,
                           lookback = 30,
                           lockup = 30,
                           sma_len = 10,
                           target_pct = 0.05,
                           stop_pct = 0.07,
                           as_of_date = NULL) {

  if (!is.data.frame(px) || nrow(px) == 0) return(tibble())
  if (is.na(ticker) || ticker == "" || is.na(tweet_ts_et)) return(tibble())

  side <- tolower(side %||% "long")
  is_short <- identical(side, "short")

  if (is.null(as_of_date) || is.na(as_of_date)) {
    as_of_date <- max(px$date, na.rm = TRUE)
  }

  as_of_date <- as.Date(as_of_date)

  px_full <- px %>%
    filter(date <= as_of_date) %>%
    arrange(date) %>%
    mutate(
      close = as.numeric(close),
      open  = as.numeric(open),
      high  = as.numeric(high),
      low   = as.numeric(low),
      close = zoo::na.locf(close, na.rm = FALSE),
      open  = zoo::na.locf(open,  na.rm = FALSE),
      high  = zoo::na.locf(high,  na.rm = FALSE),
      low   = zoo::na.locf(low,   na.rm = FALSE),
      sma_val = zoo::rollmean(close, k = sma_len, fill = NA_real_, align = "right")
    ) %>%
    filter(!is.na(date), !is.na(close), !is.na(open), !is.na(high), !is.na(low))

  if (nrow(px_full) < sma_len) return(tibble())
  if (max(px_full$date, na.rm = TRUE) < as.Date(tweet_ts_et)) return(tibble())

  data_through_date <- max(px_full$date, na.rm = TRUE)

  entry_pick <- pick_entry_from_px(px_full, tweet_ts_et)
  entry_date <- entry_pick$entry_date
  entry_at   <- entry_pick$entry_at

  if (is.na(entry_date)) return(tibble())

  entry_row <- px_full %>%
    filter(date == entry_date) %>%
    slice_head(n = 1)

  if (nrow(entry_row) == 0) return(tibble())

  rows_before_entry <- sum(px_full$date <= entry_date)
  non_na_closes_before_entry <- sum(!is.na(px_full$close[px_full$date <= entry_date]))

  entry_price <- if (entry_at == "open") entry_row$open[[1]] else entry_row$close[[1]]
  entry_sma   <- entry_row$sma_val[[1]]

  if (is.na(entry_sma)) {
    last_n <- px_full %>%
      filter(date <= entry_date) %>%
      slice_tail(n = sma_len)

    if (nrow(last_n) == sma_len && all(!is.na(last_n$close))) {
      entry_sma <- mean(last_n$close)
    }
  }

  entry_below_sma10 <- if_else(!is.na(entry_sma), entry_price < entry_sma, NA)

  # ------------------------------------------------------------------
  # Individual stock features known before/at entry
  # ------------------------------------------------------------------

  entry_idx <- match(entry_date, px_full$date)

  enough_sma10_history <- !is.na(entry_idx) && entry_idx > 10
  enough_5d_history    <- !is.na(entry_idx) && entry_idx > 5
  enough_20d_history   <- !is.na(entry_idx) && entry_idx > 20
  young_stock_flag     <- !is.na(entry_idx) && entry_idx <= 20

  sma10_prior <- if (enough_sma10_history) {
    mean(px_full$close[(entry_idx - 10):(entry_idx - 1)], na.rm = TRUE)
  } else {
    NA_real_
  }

  dist_sma10 <- if (!is.na(sma10_prior) && sma10_prior > 0) {
    entry_price / sma10_prior - 1
  } else {
    NA_real_
  }

  ret_5d_prior <- if (enough_5d_history) {
    px_full$close[[entry_idx - 1]] / px_full$close[[entry_idx - 6]] - 1
  } else {
    NA_real_
  }

  ret_20d_prior <- if (enough_20d_history) {
    px_full$close[[entry_idx - 1]] / px_full$close[[entry_idx - 21]] - 1
  } else {
    NA_real_
  }

  daily_ret <- px_full$close / dplyr::lag(px_full$close) - 1

  volatility_20d <- if (enough_20d_history) {
    sd(daily_ret[(entry_idx - 20):(entry_idx - 1)], na.rm = TRUE)
  } else {
    NA_real_
  }

  prev_close <- if (!is.na(entry_idx) && entry_idx > 1) {
    px_full$close[[entry_idx - 1]]
  } else {
    NA_real_
  }

  gap_from_prev_close <- if (entry_at == "open" && !is.na(prev_close) && prev_close > 0) {
    entry_price / prev_close - 1
  } else {
    NA_real_
  }

  directional_ret_5d <- if (is_short) {
    -ret_5d_prior
  } else {
    ret_5d_prior
  }

  directional_ret_20d <- if (is_short) {
    -ret_20d_prior
  } else {
    ret_20d_prior
  }

  directional_dist_sma10 <- if (is_short) {
    -dist_sma10
  } else {
    dist_sma10
  }

  # ------------------------------------------------------------------
  # Strategy exit logic
  # ------------------------------------------------------------------

  px_scan <- px_full %>%
    filter(date >= (as.Date(tweet_ts_et) - days(lookback)))

  min_exit_d <- entry_date + days(lockup)

  tgt_px  <- if (is_short) entry_price * (1 - target_pct) else entry_price * (1 + target_pct)
  stop_px <- if (is_short) entry_price * (1 + stop_pct) else entry_price * (1 - stop_pct)

  scan_start <- if (entry_at == "close") entry_date + days(1) else entry_date

  daily <- px_scan %>%
    filter(date >= scan_start, date <= min_exit_d) %>%
    mutate(
      take_hit = if (is_short) low <= tgt_px else high >= tgt_px,
      stop_hit = if (is_short) high >= stop_px else low <= stop_px
    )

  first_event <- daily %>%
    filter(take_hit | stop_hit) %>%
    slice_head(n = 1)

  part_exit <- FALSE
  stop_exit <- FALSE
  completed <- TRUE
  trade_status <- "completed"
  completion_reason <- NA_character_
  exit_reason <- NA_character_

  mtm_row <- px_full %>% slice_tail(n = 1)
  mtm_date <- mtm_row$date[[1]]
  mtm_price <- mtm_row$close[[1]]

  if (nrow(first_event) == 0) {

    if (data_through_date < min_exit_d) {
      part_exit     <- FALSE
      stop_exit     <- FALSE
      completed     <- FALSE
      trade_status  <- "open_no_exit_yet"
      completion_reason <- "within_lockup_no_exit_yet"
      exit_reason   <- "open_no_exit_yet"

      first_exit_d  <- as.Date(NA)
      first_exit_px <- NA_real_
      final_exit_d  <- mtm_date
      final_exit_px <- mtm_price

    } else {
      part_exit     <- FALSE
      stop_exit     <- FALSE
      completed     <- TRUE
      trade_status  <- "completed"
      completion_reason <- "time_exit_lockup"
      exit_reason   <- "time_exit_lockup"

      day_row <- px_full %>%
        filter(date >= min_exit_d) %>%
        slice_head(n = 1)

      if (nrow(day_row) == 0) day_row <- dplyr::last(px_full)

      first_exit_d  <- day_row$date
      first_exit_px <- day_row$close
      final_exit_d  <- first_exit_d
      final_exit_px <- first_exit_px
    }

  } else if (isTRUE(first_event$stop_hit[[1]])) {

    part_exit     <- FALSE
    stop_exit     <- TRUE
    completed     <- TRUE
    trade_status  <- "completed"
    completion_reason <- "stop_first"
    exit_reason   <- "stop_first"

    first_exit_d  <- first_event$date
    first_exit_px <- stop_px
    final_exit_d  <- first_exit_d
    final_exit_px <- first_exit_px

  } else {

    part_exit     <- TRUE
    stop_exit     <- FALSE

    first_exit_d  <- first_event$date
    first_exit_px <- tgt_px

    post_target_px <- px_full %>% filter(date > first_exit_d)

    if (nrow(post_target_px) == 0) {
      completed     <- FALSE
      trade_status  <- "open_runner"
      completion_reason <- "runner_no_post_target_data"
      exit_reason   <- "open_runner"
      final_exit_d  <- mtm_date
      final_exit_px <- mtm_price

    } else if (!is_short) {

      if (!isTRUE(entry_below_sma10)) {
        trail_exit <- post_target_px %>%
          filter(close < sma_val) %>%
          slice_head(n = 1)

        if (nrow(trail_exit) > 0) {
          completed     <- TRUE
          trade_status  <- "completed"
          completion_reason <- "runner_trail_exit"
          exit_reason   <- "runner_trail_exit"
          final_exit_d  <- trail_exit$date
          final_exit_px <- trail_exit$close
        } else {
          completed     <- FALSE
          trade_status  <- "open_runner"
          completion_reason <- "runner_no_trail_exit_yet"
          exit_reason   <- "open_runner"
          final_exit_d  <- mtm_date
          final_exit_px <- mtm_price
        }

      } else {
        armed_rows <- post_target_px %>%
          mutate(above_sma = close > sma_val)

        first_reclaim <- armed_rows %>%
          filter(above_sma) %>%
          slice_head(n = 1)

        pre_reclaim_stop <- armed_rows %>%
          { if (nrow(first_reclaim) > 0) filter(., date <= first_reclaim$date) else . } %>%
          filter(low <= stop_px) %>%
          slice_head(n = 1)

        if (nrow(pre_reclaim_stop) > 0) {
          completed     <- TRUE
          trade_status  <- "completed"
          completion_reason <- "pre_reclaim_stop"
          exit_reason   <- "pre_reclaim_stop"
          final_exit_d  <- pre_reclaim_stop$date
          final_exit_px <- stop_px

        } else if (nrow(first_reclaim) == 0) {
          completed     <- FALSE
          trade_status  <- "open_runner"
          completion_reason <- "runner_waiting_for_sma_reclaim"
          exit_reason   <- "open_runner"
          final_exit_d  <- mtm_date
          final_exit_px <- mtm_price

        } else {
          trail_exit <- armed_rows %>%
            filter(date > first_reclaim$date, close < sma_val) %>%
            slice_head(n = 1)

          if (nrow(trail_exit) > 0) {
            completed     <- TRUE
            trade_status  <- "completed"
            completion_reason <- "runner_trail_exit"
            exit_reason   <- "runner_trail_exit"
            final_exit_d  <- trail_exit$date
            final_exit_px <- trail_exit$close
          } else {
            completed     <- FALSE
            trade_status  <- "open_runner"
            completion_reason <- "runner_no_trail_exit_yet"
            exit_reason   <- "open_runner"
            final_exit_d  <- mtm_date
            final_exit_px <- mtm_price
          }
        }
      }

    } else {

      if (isTRUE(entry_below_sma10)) {
        trail_exit <- post_target_px %>%
          filter(close > sma_val) %>%
          slice_head(n = 1)

        if (nrow(trail_exit) > 0) {
          completed     <- TRUE
          trade_status  <- "completed"
          completion_reason <- "runner_trail_exit"
          exit_reason   <- "runner_trail_exit"
          final_exit_d  <- trail_exit$date
          final_exit_px <- trail_exit$close
        } else {
          completed     <- FALSE
          trade_status  <- "open_runner"
          completion_reason <- "runner_no_trail_exit_yet"
          exit_reason   <- "open_runner"
          final_exit_d  <- mtm_date
          final_exit_px <- mtm_price
        }

      } else {
        armed_rows <- post_target_px %>%
          mutate(below_sma = close < sma_val)

        first_reclaim <- armed_rows %>%
          filter(below_sma) %>%
          slice_head(n = 1)

        pre_reclaim_stop <- armed_rows %>%
          { if (nrow(first_reclaim) > 0) filter(., date <= first_reclaim$date) else . } %>%
          filter(high >= stop_px) %>%
          slice_head(n = 1)

        if (nrow(pre_reclaim_stop) > 0) {
          completed     <- TRUE
          trade_status  <- "completed"
          completion_reason <- "pre_reclaim_stop"
          exit_reason   <- "pre_reclaim_stop"
          final_exit_d  <- pre_reclaim_stop$date
          final_exit_px <- stop_px

        } else if (nrow(first_reclaim) == 0) {
          completed     <- FALSE
          trade_status  <- "open_runner"
          completion_reason <- "runner_waiting_for_sma_reclaim"
          exit_reason   <- "open_runner"
          final_exit_d  <- mtm_date
          final_exit_px <- mtm_price

        } else {
          trail_exit <- armed_rows %>%
            filter(date > first_reclaim$date, close > sma_val) %>%
            slice_head(n = 1)

          if (nrow(trail_exit) > 0) {
            completed     <- TRUE
            trade_status  <- "completed"
            completion_reason <- "runner_trail_exit"
            exit_reason   <- "runner_trail_exit"
            final_exit_d  <- trail_exit$date
            final_exit_px <- trail_exit$close
          } else {
            completed     <- FALSE
            trade_status  <- "open_runner"
            completion_reason <- "runner_no_trail_exit_yet"
            exit_reason   <- "open_runner"
            final_exit_d  <- mtm_date
            final_exit_px <- mtm_price
          }
        }
      }
    }
  }

  final_return <- if (part_exit) {
    if (is_short) {
      (2 / 3) * ((entry_price - first_exit_px) / entry_price) +
        (1 / 3) * ((entry_price - final_exit_px) / entry_price)
    } else {
      (2 / 3) * ((first_exit_px - entry_price) / entry_price) +
        (1 / 3) * ((final_exit_px - entry_price) / entry_price)
    }
  } else {
    if (is_short) {
      (entry_price - final_exit_px) / entry_price
    } else {
      (final_exit_px - entry_price) / entry_price
    }
  }

  tibble(
    ticker = ticker,
    tweet_ts_et = tweet_ts_et,
    side = side,
    entry_date = entry_date,
    entry_at = entry_at,
    entry_price = entry_price,
    entry_sma = entry_sma,
    has_entry_sma = !is.na(entry_sma),
    entry_below_sma10 = entry_below_sma10,

    enough_sma10_history = enough_sma10_history,
    enough_5d_history = enough_5d_history,
    enough_20d_history = enough_20d_history,
    young_stock_flag = young_stock_flag,
    sma10_prior = sma10_prior,
    dist_sma10 = dist_sma10,
    ret_5d_prior = ret_5d_prior,
    ret_20d_prior = ret_20d_prior,
    volatility_20d = volatility_20d,
    gap_from_prev_close = gap_from_prev_close,
    directional_ret_5d = directional_ret_5d,
    directional_ret_20d = directional_ret_20d,
    directional_dist_sma10 = directional_dist_sma10,

    rows_before_entry = rows_before_entry,
    non_na_closes_before_entry = non_na_closes_before_entry,
    px_start = min(px_full$date, na.rm = TRUE),
    px_end = max(px_full$date, na.rm = TRUE),
    first_exit_date = first_exit_d,
    first_exit_price = first_exit_px,
    final_exit_date = final_exit_d,
    final_exit_price = final_exit_px,
    data_through_date = data_through_date,
    holding_days = as.integer(final_exit_d - entry_date),
    part_exit = part_exit,
    stop_exit = stop_exit,
    exit_reason = exit_reason,
    completed = completed,
    trade_status = trade_status,
    completion_reason = completion_reason,
    target_pct = target_pct,
    stop_pct = stop_pct,
    total_return = final_return
  )
}

simulate_trade_full <- function(row,
                                lookback = 30,
                                lockup = 30,
                                sma_len = 10,
                                target_pct = 0.05,
                                stop_pct = 0.07,
                                cache_dir = "cache_px") {

  px <- get_px_cached(
    sym = row$ticker[[1]],
    from = as.Date(row$tweet_ts_et[[1]]) - days(250),
    to = Sys.Date(),
    cache_dir = cache_dir
  )

  out <- simulate_trade(
    ticker = row$ticker[[1]],
    tweet_ts_et = row$tweet_ts_et[[1]],
    side = row$side[[1]],
    px = px,
    lookback = lookback,
    lockup = lockup,
    sma_len = sma_len,
    target_pct = target_pct,
    stop_pct = stop_pct,
    as_of_date = Sys.Date()
  )

  if (nrow(out) == 0) return(tibble())

  bind_cols(row, out %>% select(-ticker, -side, -tweet_ts_et))
}

add_spy_regime <- function(results_tbl,
                           cache_dir = "cache_px",
                           spy_symbol = "SPY",
                           spy_from = as.Date("2009-01-01"),
                           use_previous_day = TRUE) {
  if (!is.data.frame(results_tbl) || nrow(results_tbl) == 0) return(results_tbl)

  results_tbl <- results_tbl %>%
    mutate(
      tweet_ts_et       = as.POSIXct(tweet_ts_et, tz = "America/New_York"),
      entry_date        = as.Date(entry_date),
      px_start          = as.Date(px_start),
      px_end            = as.Date(px_end),
      first_exit_date   = as.Date(first_exit_date),
      final_exit_date   = as.Date(final_exit_date),
      data_through_date = as.Date(data_through_date)
    ) %>%
    select(-any_of(c(
      "date",
      "spy_regime_date",
      "spy_close",
      "spy_sma50",
      "spy_sma200",
      "market_regime_50",
      "market_regime_200"
    )))

  spy_to <- max(results_tbl$data_through_date, na.rm = TRUE)

  spy_px <- get_px_cached(
    sym       = spy_symbol,
    from      = spy_from,
    to        = spy_to,
    cache_dir = cache_dir,
    fetch_fn  = get_px_via_syscurl_yahoo
  )

  if (nrow(spy_px) == 0 || !"date" %in% names(spy_px)) {
    message("No SPY price data available; returning results without regime columns.")
    return(results_tbl)
  }

  spy_regime <- spy_px %>%
    arrange(date) %>%
    mutate(
      spy_sma50  = TTR::SMA(close, n = 50),
      spy_sma200 = TTR::SMA(close, n = 200),
      market_regime_50 = case_when(
        is.na(spy_sma50)  ~ NA_character_,
        close > spy_sma50 ~ "bullish",
        close < spy_sma50 ~ "bearish",
        TRUE              ~ "neutral"
      ),
      market_regime_200 = case_when(
        is.na(spy_sma200)  ~ NA_character_,
        close > spy_sma200 ~ "bullish",
        close < spy_sma200 ~ "bearish",
        TRUE               ~ "neutral"
      )
    ) %>%
    transmute(
      spy_regime_date = date,
      spy_close = close,
      spy_sma50,
      spy_sma200,
      market_regime_50,
      market_regime_200
    )

  if (use_previous_day) {
    results_tbl %>%
      arrange(entry_date) %>%
      left_join(
        spy_regime %>% arrange(spy_regime_date),
        by = join_by(closest(entry_date > spy_regime_date))
      )
  } else {
    results_tbl %>%
      arrange(entry_date) %>%
      left_join(
        spy_regime %>% arrange(spy_regime_date),
        by = join_by(closest(entry_date >= spy_regime_date))
      )
  }
}

run_backtest_incremental <- function(input_tbl,
                                     cache_dir = "cache_px",
                                     add_market_regime = TRUE) {
  if (!is.data.frame(input_tbl) || nrow(input_tbl) == 0) return(tibble())

  n <- nrow(input_tbl)
  pb <- txtProgressBar(min = 0, max = n, style = 3)
  on.exit(close(pb), add = TRUE)

  results_list <- vector("list", n)

  for (i in seq_len(n)) {
    row_i <- input_tbl[i, , drop = FALSE]

    message(sprintf("[%d/%d] Processing: %s | %s", i, n, row_i$username[[1]], row_i$ticker[[1]]))

    results_list[[i]] <- tryCatch(
      simulate_trade_full(row_i, cache_dir = cache_dir),
      error = function(e) {
        message(sprintf("Error on %s: %s", row_i$ticker[[1]], e$message))
        tibble()
      }
    )

    setTxtProgressBar(pb, i)
  }

  out <- bind_rows(results_list)

  if (add_market_regime && nrow(out) > 0) {
    out <- add_spy_regime(
      results_tbl = out,
      cache_dir = cache_dir,
      spy_symbol = "SPY",
      spy_from = as.Date("2009-01-01"),
      use_previous_day = TRUE
    )
  }

  out
}

# -----------------------------------------------------------------------------
# 3) pull source tables
# -----------------------------------------------------------------------------

con <- connect_supabase()
on.exit(dbDisconnect(con), add = TRUE)

signals_raw <- dbReadTable(con, DBI::Id(schema = "public", table = "twitter_trade_signals"))

backtest_exists <- dbExistsTable(con, DBI::Id(schema = "public", table = "twitter_backtest_results"))

backtest_raw <- if (backtest_exists) {
  dbReadTable(con, DBI::Id(schema = "public", table = "twitter_backtest_results"))
} else {
  tibble()
}

# -----------------------------------------------------------------------------
# 4) standardize inputs
# -----------------------------------------------------------------------------

signals_tbl <- signals_raw %>%
  mutate(
    created_at        = as.POSIXct(created_at, tz = "UTC"),
    tweet_ts_et       = with_tz(created_at, "America/New_York"),
    ticker            = str_to_upper(ticker),
    side              = str_to_lower(direction),
    conversation_id   = as.character(conversation_id),
    conviction        = as.numeric(conviction),
    buy_zone          = as_logicalish(buy_zone),
    sentiment_score   = as.numeric(sentiment_score),
    evidence_score    = as.numeric(evidence_score),
    specificity_score = as.numeric(specificity_score),
    has_quant_data    = as_logicalish(has_quant_data)
  ) %>%
  transmute(
    ticker,
    tweet_ts_et,
    side,
    conviction,
    buy_zone,
    sentiment_score,
    rationale,
    username,
    conversation_id,
    evidence_score,
    specificity_score,
    has_quant_data,
    evidence_types,
    evidence_snippet,
    signal_key = paste(
      username,
      conversation_id,
      ticker,
      format(tweet_ts_et, "%Y%m%d%H%M%S"),
      side,
      sep = "_"
    )
  ) %>%
  filter(!is.na(tweet_ts_et), !is.na(ticker), ticker != "") %>%
  distinct(signal_key, .keep_all = TRUE)

backtest_tbl <- if (nrow(backtest_raw) == 0) {
  tibble()
} else {
  backtest_raw %>%
    mutate(
      tweet_ts_et       = as.POSIXct(tweet_ts_et, tz = "America/New_York"),
      ticker            = str_to_upper(ticker),
      side              = str_to_lower(side),
      conversation_id   = as.character(conversation_id),
      completed         = as_logicalish(completed),
      conviction        = as.numeric(conviction),
      buy_zone          = as_logicalish(buy_zone),
      sentiment_score   = as.numeric(sentiment_score),
      evidence_score    = as.numeric(evidence_score),
      specificity_score = as.numeric(specificity_score),
      has_quant_data    = as_logicalish(has_quant_data),
      trade_id          = as.character(trade_id),
      signal_key = paste(
        username,
        conversation_id,
        ticker,
        format(tweet_ts_et, "%Y%m%d%H%M%S"),
        side,
        sep = "_"
      )
    )
}

# -----------------------------------------------------------------------------
# 5) choose what to rerun
# -----------------------------------------------------------------------------

existing_keys <- if (nrow(backtest_tbl) == 0) character() else unique(backtest_tbl$signal_key)

new_signals <- signals_tbl %>%
  filter(!signal_key %in% existing_keys) %>%
  mutate(trade_id = NA_character_)

incomplete_rows <- if (nrow(backtest_tbl) == 0) {
  tibble()
} else {
  backtest_tbl %>%
    filter(is.na(completed) | completed == FALSE) %>%
    transmute(
      trade_id,
      ticker,
      tweet_ts_et,
      side,
      conviction,
      buy_zone,
      sentiment_score,
      rationale,
      username,
      conversation_id,
      evidence_score,
      specificity_score,
      has_quant_data,
      evidence_types,
      evidence_snippet,
      signal_key
    )
}

todo_tbl <- bind_rows(incomplete_rows, new_signals) %>%
  arrange(desc(!is.na(trade_id)), tweet_ts_et) %>%
  distinct(signal_key, .keep_all = TRUE)

message("Incomplete rows to refresh: ", nrow(incomplete_rows))
message("New signals to backtest: ", nrow(new_signals))
message("Total rows to process: ", nrow(todo_tbl))

if (nrow(todo_tbl) == 0) {
  message("Nothing to process. Exiting.")
  quit(save = "no", status = 0)
}

# -----------------------------------------------------------------------------
# 6) rerun + add regime
# -----------------------------------------------------------------------------

batch_results <- run_backtest_incremental(todo_tbl)

if (nrow(batch_results) == 0) {
  message("Backtest batch returned 0 rows. Exiting.")
  quit(save = "no", status = 0)
}

# -----------------------------------------------------------------------------
# 7) prepare upload batch
# -----------------------------------------------------------------------------

batch_upload <- batch_results %>%
  mutate(
    tweet_ts_et       = as.POSIXct(tweet_ts_et, tz = "America/New_York"),
    entry_date        = as.Date(entry_date),
    px_start          = as.Date(px_start),
    px_end            = as.Date(px_end),
    first_exit_date   = as.Date(first_exit_date),
    final_exit_date   = as.Date(final_exit_date),
    data_through_date = as.Date(data_through_date),
    spy_regime_date   = as.Date(spy_regime_date),
    conversation_id   = as.character(conversation_id)
  ) %>%
  mutate(
    existing_trade_id = if_else(!is.na(trade_id) & nzchar(trade_id), trade_id, NA_character_),
    trade_id_base_new = if_else(
      is.na(existing_trade_id),
      paste(
        username,
        conversation_id,
        ticker,
        format(tweet_ts_et, "%Y%m%d%H%M%S"),
        entry_at,
        sep = "_"
      ),
      NA_character_
    )
  ) %>%
  group_by(trade_id_base_new) %>%
  mutate(
    trade_id = case_when(
      !is.na(existing_trade_id) ~ existing_trade_id,
      is.na(trade_id_base_new) ~ NA_character_,
      n() == 1 ~ trade_id_base_new,
      TRUE ~ paste0(trade_id_base_new, "_", row_number())
    )
  ) %>%
  ungroup() %>%
  select(-existing_trade_id, -trade_id_base_new, -signal_key) %>%
  mutate(loaded_at = Sys.time()) %>%
  relocate(trade_id, .before = ticker) %>%
  distinct(trade_id, .keep_all = TRUE)

# -----------------------------------------------------------------------------
# 8) upsert into Supabase, keep existing rows
# -----------------------------------------------------------------------------

qid <- function(x) as.character(DBI::dbQuoteIdentifier(con, x))

target_tbl   <- DBI::Id(schema = "public", table = "twitter_backtest_results")
target_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, target_tbl))

if (dbExistsTable(con, target_tbl)) {
  dbExecute(con, "
    ALTER TABLE public.twitter_backtest_results
    ADD COLUMN IF NOT EXISTS spy_regime_date date,
    ADD COLUMN IF NOT EXISTS enough_sma10_history boolean,
    ADD COLUMN IF NOT EXISTS enough_5d_history boolean,
    ADD COLUMN IF NOT EXISTS enough_20d_history boolean,
    ADD COLUMN IF NOT EXISTS young_stock_flag boolean,
    ADD COLUMN IF NOT EXISTS sma10_prior double precision,
    ADD COLUMN IF NOT EXISTS dist_sma10 double precision,
    ADD COLUMN IF NOT EXISTS ret_5d_prior double precision,
    ADD COLUMN IF NOT EXISTS ret_20d_prior double precision,
    ADD COLUMN IF NOT EXISTS volatility_20d double precision,
    ADD COLUMN IF NOT EXISTS gap_from_prev_close double precision,
    ADD COLUMN IF NOT EXISTS directional_ret_5d double precision,
    ADD COLUMN IF NOT EXISTS directional_ret_20d double precision,
    ADD COLUMN IF NOT EXISTS directional_dist_sma10 double precision;
  ")
}

if (!dbExistsTable(con, target_tbl)) {
  dbWriteTable(
    con,
    target_tbl,
    batch_upload,
    overwrite = FALSE,
    row.names = FALSE
  )

  dbExecute(con, "CREATE UNIQUE INDEX IF NOT EXISTS idx_tbr_trade_id ON public.twitter_backtest_results(trade_id);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_tbr_entry_date ON public.twitter_backtest_results(entry_date);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_tbr_ticker ON public.twitter_backtest_results(ticker);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_tbr_username ON public.twitter_backtest_results(username);")

} else {
  temp_name  <- paste0("tmp_tbr_", format(Sys.time(), "%Y%m%d%H%M%S"))
  temp_tbl   <- DBI::Id(table = temp_name)
  temp_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, temp_tbl))

  dbWriteTable(
    con,
    temp_tbl,
    batch_upload,
    temporary = TRUE,
    overwrite = TRUE,
    row.names = FALSE
  )

  cols <- names(batch_upload)
  cols_q <- vapply(cols, qid, character(1))

  update_cols <- setdiff(cols, "trade_id")

  update_set <- paste(
    vapply(
      update_cols,
      function(x) sprintf("%s = EXCLUDED.%s", qid(x), qid(x)),
      character(1)
    ),
    collapse = ", "
  )

  col_list <- paste(cols_q, collapse = ", ")

  upsert_sql <- sprintf(
    "
    INSERT INTO %s (%s)
    SELECT %s
    FROM %s
    ON CONFLICT (trade_id) DO UPDATE
    SET %s;
    ",
    target_tbl_q,
    col_list,
    col_list,
    temp_tbl_q,
    update_set
  )

  dbExecute(con, upsert_sql)
}

# -----------------------------------------------------------------------------
# 9) checks
# -----------------------------------------------------------------------------

n_total <- dbGetQuery(
  con,
  "SELECT COUNT(*) AS n_rows FROM public.twitter_backtest_results;"
)

n_open <- dbGetQuery(
  con,
  "SELECT COUNT(*) AS n_open
   FROM public.twitter_backtest_results
   WHERE completed IS FALSE OR completed IS NULL;"
)

n_dupes <- dbGetQuery(
  con,
  "
  SELECT COUNT(*) AS n_duplicate_trade_ids
  FROM (
    SELECT trade_id
    FROM public.twitter_backtest_results
    GROUP BY trade_id
    HAVING COUNT(*) > 1
  ) d;
  "
)

message("Done.")
print(n_total)
print(n_open)
print(n_dupes)
