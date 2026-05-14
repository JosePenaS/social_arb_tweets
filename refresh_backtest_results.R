#!/usr/bin/env Rscript

# =============================================================================
# Social Arbitrage Backtest - 10% take-profit / 5% stop strategy
# File expected by YAML: refresh_backtest_results.R
#
# This script:
#   1) Pulls signals from public.twitter_trade_signals
#   2) Backtests only new signals + incomplete/open rows
#   3) Uses target_pct = 0.10 and stop_pct = 0.05 by default
#   4) Preserves horizon from the signal table
#   5) Applies lockup scan using the first real trading day >= lockup date
#   6) Uploads/upserts into public.twitter_backtest_results_10_perc_strategy
#
# Required GitHub secrets/env vars:
#   SUPABASE_POOLER_HOST
#   SUPABASE_PORT        default 5432
#   SUPABASE_DB          default postgres
#   SUPABASE_USER
#   SUPABASE_PWD
#
# Optional env vars:
#   BACKTEST_AS_OF_DATE       optional manual override, YYYY-MM-DD
#   LOCAL_TZ                  default America/New_York; used to avoid GitHub UTC date drift
#   CACHE_DIR                 default cache_px_tp10_sl5
#   BACKTEST_TARGET_TABLE     default twitter_backtest_results_10_perc_strategy
#   BACKTEST_STRATEGY_LABEL   default tp10_sl5
#   TARGET_PCT                default 0.10
#   STOP_PCT                  default 0.05
#   DEBUG_YAHOO               default false
# =============================================================================

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

message("RUNNING FILE: refresh_backtest_results.R | SAFE YAHOO + SMOKE TEST + LOCAL AS_OF_DATE VERSION | 2026-05-13")

# -----------------------------------------------------------------------------
# 0) Controls
# -----------------------------------------------------------------------------

STRATEGY_LABEL <- Sys.getenv("BACKTEST_STRATEGY_LABEL", "tp10_sl5")
TARGET_TABLE_NAME <- Sys.getenv(
  "BACKTEST_TARGET_TABLE",
  "twitter_backtest_results_10_perc_strategy"
)

TARGET_PCT <- as.numeric(Sys.getenv("TARGET_PCT", "0.10"))
STOP_PCT   <- as.numeric(Sys.getenv("STOP_PCT", "0.05"))

CACHE_DIR <- Sys.getenv("CACHE_DIR", "cache_px_tp10_sl5")

LOCAL_TZ <- Sys.getenv("LOCAL_TZ", "America/New_York")

AS_OF_DATE_ENV <- Sys.getenv("BACKTEST_AS_OF_DATE", "")

AS_OF_DATE <- if (nzchar(AS_OF_DATE_ENV)) {
  as.Date(AS_OF_DATE_ENV)
} else {
  # Important for GitHub Actions:
  # Sys.Date() uses the runner/system date, which may already be tomorrow in UTC.
  # Using New York local date minus 1 avoids requesting future Yahoo daily data.
  as.Date(lubridate::with_tz(Sys.time(), LOCAL_TZ)) - lubridate::days(1)
}

DEBUG_YAHOO <- identical(tolower(Sys.getenv("DEBUG_YAHOO", "false")), "true")

message("Strategy label: ", STRATEGY_LABEL)
message("Target table: public.", TARGET_TABLE_NAME)
message("Target pct: ", TARGET_PCT)
message("Stop pct: ", STOP_PCT)
message("Local TZ: ", LOCAL_TZ)
message("As-of date: ", AS_OF_DATE)
message("Cache dir: ", CACHE_DIR)
message("Debug Yahoo: ", DEBUG_YAHOO)

# -----------------------------------------------------------------------------
# 1) Helpers
# -----------------------------------------------------------------------------

`%||%` <- function(x, y) if (is.null(x)) y else x

empty_px_tbl <- function() {
  tibble(
    date   = as.Date(character()),
    open   = numeric(),
    high   = numeric(),
    low    = numeric(),
    close  = numeric(),
    volume = numeric()
  )
}

as_logicalish <- function(x) {
  if (is.logical(x)) return(x)

  y <- tolower(trimws(as.character(x)))

  dplyr::case_when(
    is.na(y) ~ NA,
    y %in% c("true", "t", "1", "yes", "y", "si", "sí") ~ TRUE,
    y %in% c("false", "f", "0", "no", "n") ~ FALSE,
    TRUE ~ NA
  )
}

normalize_symbol_for_yahoo <- function(sym) {
  sym <- toupper(trimws(as.character(sym)))

  # Keep common Yahoo exchange suffixes like ATZ.TO, SHOP.TO, BHP.AX, VOD.L, etc.
  if (grepl("\\.(TO|V|L|AX|SA|MX|PA|AS|DE|F|HK|SS|SZ|T)$", sym)) {
    return(sym)
  }

  # Convert US share-class tickers like BRK.B / BF.B to BRK-B / BF-B.
  gsub("\\.", "-", sym)
}

date_to_unix <- function(x, end_of_day = FALSE) {
  x <- as.POSIXct(as.Date(x), tz = "UTC")
  if (end_of_day) x <- x + 86399
  floor(as.numeric(x))
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

  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = host,
    port     = port,
    dbname   = db,
    user     = user,
    password = pwd,
    sslmode  = "require"
  )
}

qid <- function(con, x) as.character(DBI::dbQuoteIdentifier(con, x))

pg_type_from_vector <- function(x) {
  if (inherits(x, "POSIXct") || inherits(x, "POSIXt")) return("timestamp with time zone")
  if (inherits(x, "Date")) return("date")
  if (is.logical(x)) return("boolean")
  if (is.integer(x)) return("integer")
  if (is.numeric(x)) return("double precision")
  "text"
}

add_missing_columns <- function(con, table_schema, table_name, df) {
  existing_cols <- DBI::dbGetQuery(
    con,
    "
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = $1
      AND table_name = $2;
    ",
    params = list(table_schema, table_name)
  )$column_name

  missing_cols <- setdiff(names(df), existing_cols)

  if (length(missing_cols) == 0) {
    message("No missing columns to add.")
    return(invisible(NULL))
  }

  message("Adding missing columns: ", paste(missing_cols, collapse = ", "))

  table_q <- as.character(DBI::dbQuoteIdentifier(
    con,
    DBI::Id(schema = table_schema, table = table_name)
  ))

  for (col in missing_cols) {
    pg_type <- pg_type_from_vector(df[[col]])

    sql <- sprintf(
      "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;",
      table_q,
      qid(con, col),
      pg_type
    )

    DBI::dbExecute(con, sql)
  }

  invisible(NULL)
}

# -----------------------------------------------------------------------------
# 2) Yahoo fetch + cache
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
      cat("Yahoo symbol:", sym, "\n")
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

      if (debug) {
        message("Yahoo response did not pass validation. First 800 chars:")
        message(substr(txt, 1, 800))
      }
    }

    Sys.sleep(min(2^k, 8))
  }

  FALSE
}

read_yahoo_chart_json <- function(path) {
  if (!file.exists(path)) return(empty_px_tbl())

  txt <- tryCatch(
    paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n"),
    error = function(e) ""
  )

  if (!nzchar(txt)) return(empty_px_tbl())

  js <- tryCatch(
    jsonlite::fromJSON(txt, simplifyVector = FALSE),
    error = function(e) NULL
  )

  if (is.null(js) || is.null(js$chart)) return(empty_px_tbl())
  if (!is.null(js$chart$error)) return(empty_px_tbl())

  res <- js$chart$result
  if (is.null(res) || length(res) == 0 || is.null(res[[1]])) return(empty_px_tbl())

  res1 <- res[[1]]
  timestamps <- res1$timestamp
  quote_obj  <- tryCatch(res1$indicators$quote[[1]], error = function(e) NULL)

  if (is.null(timestamps) || is.null(quote_obj)) return(empty_px_tbl())

  to_num <- function(x) {
    if (is.null(x)) return(numeric())

    if (is.list(x)) {
      return(vapply(
        x,
        function(v) {
          if (is.null(v)) NA_real_ else suppressWarnings(as.numeric(v))
        },
        numeric(1)
      ))
    }

    suppressWarnings(as.numeric(x))
  }

  ts_vec    <- to_num(timestamps)
  open_vec  <- to_num(quote_obj$open)
  high_vec  <- to_num(quote_obj$high)
  low_vec   <- to_num(quote_obj$low)
  close_vec <- to_num(quote_obj$close)
  vol_vec   <- to_num(quote_obj$volume)

  min_len <- min(
    length(ts_vec),
    length(open_vec),
    length(high_vec),
    length(low_vec),
    length(close_vec),
    length(vol_vec)
  )

  if (min_len == 0) return(empty_px_tbl())

  tibble(
    date = as.Date(
      as.POSIXct(ts_vec[seq_len(min_len)], origin = "1970-01-01", tz = "UTC")
    ),
    open   = as.numeric(open_vec[seq_len(min_len)]),
    high   = as.numeric(high_vec[seq_len(min_len)]),
    low    = as.numeric(low_vec[seq_len(min_len)]),
    close  = as.numeric(close_vec[seq_len(min_len)]),
    volume = as.numeric(vol_vec[seq_len(min_len)])
  ) %>%
    filter(!is.na(date)) %>%
    arrange(date)
}

get_px_via_syscurl_yahoo <- function(sym, from, to, debug = DEBUG_YAHOO) {
  tmp_json <- tempfile(fileext = ".json")
  on.exit({
    if (file.exists(tmp_json)) unlink(tmp_json)
  }, add = TRUE)

  ok <- fetch_yahoo_chart_syscurl(
    sym = sym,
    from = from,
    to = to,
    dest_json = tmp_json,
    debug = debug
  )

  if (!ok) {
    if (debug && file.exists(tmp_json)) {
      txt_dbg <- tryCatch(
        paste(readLines(tmp_json, warn = FALSE, encoding = "UTF-8"), collapse = "\n"),
        error = function(e) ""
      )

      message("Yahoo fetch returned ok=FALSE for ", sym, ". First 800 chars:")
      message(substr(txt_dbg, 1, 800))
    } else {
      message("Yahoo fetch returned ok=FALSE for ", sym, " and no response file was available.")
    }

    return(empty_px_tbl())
  }

  if (debug && file.exists(tmp_json)) {
    txt_dbg <- tryCatch(
      paste(readLines(tmp_json, warn = FALSE, encoding = "UTF-8"), collapse = "\n"),
      error = function(e) ""
    )

    message("Yahoo raw response first 800 chars for ", sym, ":")
    message(substr(txt_dbg, 1, 800))
  }

  px <- read_yahoo_chart_json(tmp_json)

  if (!is.data.frame(px) || nrow(px) == 0 || !"date" %in% names(px)) {
    return(empty_px_tbl())
  }

  px %>%
    mutate(date = as.Date(date)) %>%
    filter(date >= as.Date(from), date <= as.Date(to)) %>%
    arrange(date)
}

get_px_cached <- function(sym, from, to, cache_dir = CACHE_DIR, fetch_fn = get_px_via_syscurl_yahoo) {

  if (is.na(from) || is.na(to) || !nzchar(sym)) return(empty_px_tbl())

  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)

  fname <- file.path(
    cache_dir,
    paste0(gsub("[^A-Za-z0-9_.-]", "_", sym), ".rds")
  )

  cached <- if (file.exists(fname)) {
    tryCatch(readRDS(fname), error = function(e) empty_px_tbl())
  } else {
    empty_px_tbl()
  }

  if (!is.data.frame(cached) || nrow(cached) == 0 || !"date" %in% names(cached)) {
    cached <- empty_px_tbl()
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
        empty_px_tbl()
      }
    )

    if (is.data.frame(fresh) && nrow(fresh) > 0 && "date" %in% names(fresh)) {
      cached <- bind_rows(cached, fresh) %>%
        mutate(date = as.Date(date)) %>%
        filter(!is.na(date)) %>%
        distinct(date, .keep_all = TRUE) %>%
        arrange(date)

      saveRDS(cached, fname)
    } else {
      message(sprintf("No usable Yahoo price data for %s [%s to %s]", sym, need_from, need_to))
    }
  }

  if (nrow(cached) == 0 || !"date" %in% names(cached)) return(empty_px_tbl())

  cached %>%
    mutate(date = as.Date(date)) %>%
    filter(date >= need_from, date <= need_to) %>%
    arrange(date)
}

# -----------------------------------------------------------------------------
# Yahoo smoke test
# -----------------------------------------------------------------------------

message("Running Yahoo smoke test with AMZN...")

smoke_from <- as.Date("2024-04-22")
smoke_to   <- as.Date("2024-05-12")

message("Yahoo smoke test window: ", smoke_from, " to ", smoke_to)

smoke_px <- get_px_via_syscurl_yahoo(
  sym = "AMZN",
  from = smoke_from,
  to = smoke_to,
  debug = DEBUG_YAHOO
)

message("Yahoo smoke test AMZN rows: ", nrow(smoke_px))

if (nrow(smoke_px) == 0) {
  stop("Yahoo smoke test failed for AMZN. Stopping before processing all trades.")
}

# -----------------------------------------------------------------------------
# 3) Strategy logic
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
                           target_pct = TARGET_PCT,
                           stop_pct = STOP_PCT,
                           as_of_date = AS_OF_DATE) {

  if (!is.data.frame(px) || nrow(px) == 0 || !"date" %in% names(px)) return(tibble())
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

  directional_ret_5d <- if (is_short) -ret_5d_prior else ret_5d_prior
  directional_ret_20d <- if (is_short) -ret_20d_prior else ret_20d_prior
  directional_dist_sma10 <- if (is_short) -dist_sma10 else dist_sma10

  px_scan <- px_full %>%
    filter(date >= (as.Date(tweet_ts_et) - days(lookback)))

  raw_lockup_d <- entry_date + days(lockup)

  lockup_exit_row <- px_full %>%
    filter(date >= raw_lockup_d) %>%
    slice_head(n = 1)

  lockup_exit_d <- if (nrow(lockup_exit_row) > 0) {
    lockup_exit_row$date[[1]]
  } else {
    raw_lockup_d
  }

  tgt_px  <- if (is_short) entry_price * (1 - target_pct) else entry_price * (1 + target_pct)
  stop_px <- if (is_short) entry_price * (1 + stop_pct) else entry_price * (1 - stop_pct)

  scan_start <- if (entry_at == "close") entry_date + days(1) else entry_date

  daily <- px_scan %>%
    filter(date >= scan_start, date <= lockup_exit_d) %>%
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

    if (data_through_date < lockup_exit_d) {
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
        filter(date == lockup_exit_d) %>%
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
                                target_pct = TARGET_PCT,
                                stop_pct = STOP_PCT,
                                cache_dir = CACHE_DIR,
                                as_of_date = AS_OF_DATE) {

  px <- get_px_cached(
    sym = row$ticker[[1]],
    from = as.Date(row$tweet_ts_et[[1]]) - days(250),
    to = as_of_date,
    cache_dir = cache_dir
  )

  if (!is.data.frame(px) || nrow(px) == 0 || !"date" %in% names(px)) {
    message(sprintf("Skipping %s: no usable price data.", row$ticker[[1]]))
    return(tibble())
  }

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
    as_of_date = as_of_date
  )

  if (nrow(out) == 0) return(tibble())

  bind_cols(row, out %>% select(-ticker, -side, -tweet_ts_et))
}

add_spy_regime <- function(results_tbl,
                           cache_dir = CACHE_DIR,
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
                                     cache_dir = CACHE_DIR,
                                     add_market_regime = TRUE,
                                     target_pct = TARGET_PCT,
                                     stop_pct = STOP_PCT,
                                     as_of_date = AS_OF_DATE) {
  if (!is.data.frame(input_tbl) || nrow(input_tbl) == 0) return(tibble())

  n <- nrow(input_tbl)
  pb <- txtProgressBar(min = 0, max = n, style = 3)
  on.exit(close(pb), add = TRUE)

  results_list <- vector("list", n)

  for (i in seq_len(n)) {
    row_i <- input_tbl[i, , drop = FALSE]

    message(sprintf("[%d/%d] Processing: %s | %s", i, n, row_i$username[[1]], row_i$ticker[[1]]))

    results_list[[i]] <- tryCatch(
      simulate_trade_full(
        row = row_i,
        target_pct = target_pct,
        stop_pct = stop_pct,
        cache_dir = cache_dir,
        as_of_date = as_of_date
      ),
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
# 4) Pull source tables
# -----------------------------------------------------------------------------

con <- connect_supabase()
on.exit(DBI::dbDisconnect(con), add = TRUE)

signals_raw <- DBI::dbReadTable(con, DBI::Id(schema = "public", table = "twitter_trade_signals"))

target_tbl   <- DBI::Id(schema = "public", table = TARGET_TABLE_NAME)
target_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, target_tbl))

backtest_exists <- DBI::dbExistsTable(con, target_tbl)

backtest_raw <- if (backtest_exists) {
  DBI::dbReadTable(con, target_tbl)
} else {
  tibble()
}

# -----------------------------------------------------------------------------
# 5) Standardize inputs
# -----------------------------------------------------------------------------

if ("created_at" %in% names(signals_raw)) {
  signals_raw <- signals_raw %>%
    mutate(
      created_at = as.POSIXct(created_at, tz = "UTC"),
      tweet_ts_et = lubridate::with_tz(created_at, "America/New_York")
    )
} else if ("created_et" %in% names(signals_raw)) {
  signals_raw <- signals_raw %>%
    mutate(
      tweet_ts_et = lubridate::force_tz(as.POSIXct(created_et), "America/New_York")
    )
} else if ("tweet_ts_et" %in% names(signals_raw)) {
  signals_raw <- signals_raw %>%
    mutate(tweet_ts_et = as.POSIXct(tweet_ts_et, tz = "America/New_York"))
} else {
  stop("No timestamp column found. Expected created_at, created_et, or tweet_ts_et.")
}

signals_tbl <- signals_raw %>%
  mutate(
    ticker = stringr::str_to_upper(as.character(ticker)),
    side = if ("direction" %in% names(.)) {
      stringr::str_to_lower(as.character(direction))
    } else if ("side" %in% names(.)) {
      stringr::str_to_lower(as.character(side))
    } else {
      NA_character_
    },
    horizon = if ("horizon" %in% names(.)) {
      stringr::str_to_lower(stringr::str_trim(as.character(horizon)))
    } else {
      NA_character_
    },
    horizon = dplyr::case_when(
      horizon %in% c("short", "mid", "long") ~ horizon,
      is.na(horizon) | horizon == "" ~ "missing",
      TRUE ~ "other"
    ),
    conversation_id   = as.character(conversation_id),
    conviction        = as.numeric(conviction),
    buy_zone          = as_logicalish(buy_zone),
    sentiment_score   = as.numeric(sentiment_score),
    evidence_score    = as.numeric(evidence_score),
    specificity_score = as.numeric(specificity_score),
    has_quant_data    = as_logicalish(has_quant_data),
    signal_key = paste(
      username,
      conversation_id,
      ticker,
      format(tweet_ts_et, "%Y%m%d%H%M%S"),
      side,
      sep = "_"
    )
  ) %>%
  transmute(
    ticker,
    tweet_ts_et,
    side,
    horizon,
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
  ) %>%
  filter(    !is.na(tweet_ts_et),
    !is.na(ticker),
    ticker != "",
    side %in% c("long", "short")
  ) %>%
  distinct(signal_key, .keep_all = TRUE)

backtest_tbl <- if (nrow(backtest_raw) == 0) {
  tibble()
} else {
  backtest_raw %>%
    mutate(
      tweet_ts_et       = as.POSIXct(tweet_ts_et, tz = "America/New_York"),
      ticker            = str_to_upper(as.character(ticker)),
      side              = str_to_lower(as.character(side)),
      horizon           = if ("horizon" %in% names(.)) as.character(horizon) else NA_character_,
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
# 6) Choose what to rerun
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
      horizon,
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

message("Existing backtest rows: ", nrow(backtest_tbl))
message("Signals available: ", nrow(signals_tbl))
message("Incomplete rows to refresh: ", nrow(incomplete_rows))
message("New signals to backtest: ", nrow(new_signals))
message("Total rows to process: ", nrow(todo_tbl))

if (nrow(todo_tbl) == 0) {
  message("Nothing to process. Exiting.")
  quit(save = "no", status = 0)
}

# -----------------------------------------------------------------------------
# 7) Run backtest batch
# -----------------------------------------------------------------------------

batch_results <- run_backtest_incremental(
  input_tbl = todo_tbl,
  cache_dir = CACHE_DIR,
  add_market_regime = TRUE,
  target_pct = TARGET_PCT,
  stop_pct = STOP_PCT,
  as_of_date = AS_OF_DATE
)

if (nrow(batch_results) == 0) {
  message("Backtest batch returned 0 rows. Exiting.")
  quit(save = "no", status = 0)
}

# -----------------------------------------------------------------------------
# 8) Prepare upload batch
# -----------------------------------------------------------------------------

date_cols <- intersect(
  c(
    "entry_date",
    "px_start",
    "px_end",
    "first_exit_date",
    "final_exit_date",
    "data_through_date",
    "spy_regime_date"
  ),
  names(batch_results)
)

batch_upload <- batch_results %>%
  mutate(
    across(all_of(date_cols), as.Date),
    tweet_ts_et = as.POSIXct(tweet_ts_et, tz = "America/New_York"),
    conversation_id = as.character(conversation_id),
    strategy_label = STRATEGY_LABEL
  )

if ("date" %in% names(batch_upload) && !"spy_regime_date" %in% names(batch_upload)) {
  batch_upload <- batch_upload %>% rename(spy_regime_date = date)
}
if ("date" %in% names(batch_upload) && "spy_regime_date" %in% names(batch_upload)) {
  batch_upload <- batch_upload %>% select(-date)
}

batch_upload <- batch_upload %>%
  mutate(
    existing_trade_id = if_else(!is.na(trade_id) & nzchar(trade_id), trade_id, NA_character_),
    trade_id_base_new = if_else(
      is.na(existing_trade_id),
      paste(
        STRATEGY_LABEL,
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
  relocate(trade_id, strategy_label, .before = ticker) %>%
  distinct(trade_id, .keep_all = TRUE)

batch_upload %>%
  summarise(
    n_rows = n(),
    n_trade_ids = n_distinct(trade_id),
    duplicate_trade_ids = n() - n_distinct(trade_id)
  ) %>%
  print()

if (any(is.na(batch_upload$trade_id)) || any(!nzchar(batch_upload$trade_id))) {
  stop("Some rows have missing/empty trade_id.")
}

if (nrow(batch_upload) != dplyr::n_distinct(batch_upload$trade_id)) {
  stop("batch_upload has duplicate trade_id values.")
}

# -----------------------------------------------------------------------------
# 9) Create table or upsert
# -----------------------------------------------------------------------------

if (!DBI::dbExistsTable(con, target_tbl)) {

  message("Target table does not exist. Creating public.", TARGET_TABLE_NAME)

  DBI::dbWriteTable(
    con,
    target_tbl,
    batch_upload,
    overwrite = FALSE,
    row.names = FALSE
  )

} else {

  message("Target table exists. Checking for missing columns...")
  add_missing_columns(
    con = con,
    table_schema = "public",
    table_name = TARGET_TABLE_NAME,
    df = batch_upload
  )
}

existing_dupes <- DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT COUNT(*) AS n_duplicate_trade_ids
    FROM (
      SELECT trade_id
      FROM %s
      GROUP BY trade_id
      HAVING COUNT(*) > 1
    ) d;
    ",
    target_tbl_q
  )
)

print(existing_dupes)

if (existing_dupes$n_duplicate_trade_ids[[1]] > 0) {
  stop("Target table has duplicate trade_id values. Fix those before upserting.")
}

idx_trade_id <- "idx_tbr_tp10_sl5_trade_id"
idx_entry_date <- "idx_tbr_tp10_sl5_entry_date"
idx_ticker <- "idx_tbr_tp10_sl5_ticker"
idx_username <- "idx_tbr_tp10_sl5_username"

DBI::dbExecute(
  con,
  sprintf(
    "CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s(trade_id);",
    qid(con, idx_trade_id),
    target_tbl_q
  )
)

DBI::dbExecute(
  con,
  sprintf(
    "CREATE INDEX IF NOT EXISTS %s ON %s(entry_date);",
    qid(con, idx_entry_date),
    target_tbl_q
  )
)

DBI::dbExecute(
  con,
  sprintf(
    "CREATE INDEX IF NOT EXISTS %s ON %s(ticker);",
    qid(con, idx_ticker),
    target_tbl_q
  )
)

DBI::dbExecute(
  con,
  sprintf(
    "CREATE INDEX IF NOT EXISTS %s ON %s(username);",
    qid(con, idx_username),
    target_tbl_q
  )
)

temp_name  <- paste0("tmp_tbr_tp10_sl5_", format(Sys.time(), "%Y%m%d%H%M%S"))
temp_tbl   <- DBI::Id(table = temp_name)
temp_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, temp_tbl))

DBI::dbWriteTable(
  con,
  temp_tbl,
  batch_upload,
  temporary = TRUE,
  overwrite = TRUE,
  row.names = FALSE
)

cols <- names(batch_upload)
cols_q <- vapply(cols, function(x) qid(con, x), character(1))

update_cols <- setdiff(cols, "trade_id")

update_set <- paste(
  vapply(
    update_cols,
    function(x) sprintf("%s = EXCLUDED.%s", qid(con, x), qid(con, x)),
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

DBI::dbExecute(con, upsert_sql)

message("Upload/upsert complete.")

# -----------------------------------------------------------------------------
# 10) Checks
# -----------------------------------------------------------------------------

DBI::dbGetQuery(
  con,
  sprintf(
    "SELECT COUNT(*) AS n_rows
     FROM %s;",
    target_tbl_q
  )
) %>% print()

DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT COUNT(*) AS n_duplicate_trade_ids
    FROM (
      SELECT trade_id
      FROM %s
      GROUP BY trade_id
      HAVING COUNT(*) > 1
    ) d;
    ",
    target_tbl_q
  )
) %>% print()

DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT
      MIN(entry_date) AS first_entry_date,
      MAX(entry_date) AS last_entry_date,
      COUNT(*) AS n_rows
    FROM %s;
    ",
    target_tbl_q
  )
) %>% print()

DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT
      strategy_label,
      target_pct,
      stop_pct,
      COUNT(*) AS n_rows
    FROM %s
    GROUP BY strategy_label, target_pct, stop_pct
    ORDER BY n_rows DESC;
    ",
    target_tbl_q
  )
) %>% print()

DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT
      horizon,
      COUNT(*) AS n_rows
    FROM %s
    GROUP BY horizon
    ORDER BY n_rows DESC;
    ",
    target_tbl_q
  )
) %>% print()







    
