#!/usr/bin/env Rscript

###############################################################################
# refresh_exit_grid_results_raw.R
#
# GitHub Actions production script for the RAW exit-parameter grid.
#
# What it does:
#   1) Reads public.twitter_trade_signal_features
#   2) Builds the exit parameter grid:
#        target_pct   = 0.10
#        stop_pct     = 0.05
#        runner_share = 1/3, 1/2, 2/3
#        sma_len      = 10, 15, 20
#        lockup       = 10, 20, 30
#      Total = 27 combinations
#   3) Runs gap-aware backtests for selected signals × all grid combinations
#   4) Uploads row-level raw grid results to public.twitter_exit_grid_results_raw
#
# Normal scheduled run:
#   - Processes new recent signals
#   - Refreshes incomplete/open signals
#   - Replaces existing rows for those selected signals
#
# Manual full rebuild:
#   - Set EXIT_GRID_FORCE_REBUILD=true
#   - Processes all valid signals
#   - Deletes previous rows for this run label before upload
###############################################################################

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(stringr)
  library(lubridate)
  library(jsonlite)
  library(zoo)
  library(TTR)
  library(tidyr)
})

###############################################################################
# 0 · Environment helpers ------------------------------------------------------
###############################################################################

env_bool <- function(name, default = FALSE) {
  val <- Sys.getenv(name, unset = if (default) "true" else "false")
  tolower(trimws(val)) %in% c("true", "1", "yes", "y")
}

env_num <- function(name, default) {
  val <- Sys.getenv(name, unset = as.character(default))
  out <- suppressWarnings(as.numeric(val))
  if (is.na(out)) default else out
}

env_int_or_inf <- function(name, default = Inf) {
  val <- Sys.getenv(name, unset = as.character(default))
  val <- tolower(trimws(val))
  if (val %in% c("inf", "infinite", "all")) return(Inf)
  out <- suppressWarnings(as.numeric(val))
  if (is.na(out)) default else out
}

env_num_vec <- function(name, default) {
  val <- Sys.getenv(name, unset = default)
  out <- suppressWarnings(as.numeric(strsplit(val, ",")[[1]]))
  out <- out[!is.na(out)]
  if (length(out) == 0) {
    suppressWarnings(as.numeric(strsplit(default, ",")[[1]]))
  } else {
    out
  }
}

env_int_vec <- function(name, default) {
  as.integer(env_num_vec(name, default))
}

get_env_first <- function(...) {
  names <- c(...)
  for (nm in names) {
    val <- Sys.getenv(nm, unset = "")
    if (nzchar(val)) return(val)
  }
  ""
}

get_default_as_of_date <- function() {
  as.Date(lubridate::with_tz(Sys.time(), "America/New_York"))
}

###############################################################################
# 1 · Controls ----------------------------------------------------------------
###############################################################################

EXIT_GRID_ENABLED <- env_bool("EXIT_GRID_ENABLED", TRUE)

if (!EXIT_GRID_ENABLED) {
  message("EXIT_GRID_ENABLED is false. Exiting.")
  quit(save = "no", status = 0)
}

FEATURES_TABLE <- Sys.getenv(
  "FEATURES_TABLE",
  "twitter_trade_signal_features"
)

EXIT_GRID_TARGET_TABLE <- Sys.getenv(
  "EXIT_GRID_TARGET_TABLE",
  "twitter_exit_grid_results_raw"
)

EXIT_GRID_RUN_LABEL <- Sys.getenv(
  "EXIT_GRID_RUN_LABEL",
  "exit_grid_tp10_sl5_raw_allsignals_27combos_v1"
)

EXIT_GRID_FORCE_REBUILD <- env_bool("EXIT_GRID_FORCE_REBUILD", FALSE)

EXIT_GRID_NEW_SIGNAL_LOOKBACK_DAYS <- env_num(
  "EXIT_GRID_NEW_SIGNAL_LOOKBACK_DAYS",
  31
)

EXIT_GRID_MAX_ROWS_LOCAL <- env_int_or_inf(
  "EXIT_GRID_MAX_ROWS_LOCAL",
  Inf
)

EXIT_GRID_UPLOAD_CHUNK_SIZE <- as.integer(env_num(
  "EXIT_GRID_UPLOAD_CHUNK_SIZE",
  5000
))

LOOKBACK_DAYS <- env_num("LOOKBACK_DAYS", 30)

TARGET_PCTS <- env_num_vec("EXIT_GRID_TARGET_PCTS", "0.10")
STOP_PCTS <- env_num_vec("EXIT_GRID_STOP_PCTS", "0.05")
RUNNER_SHARES <- env_num_vec(
  "EXIT_GRID_RUNNER_SHARES",
  "0.3333333333333333,0.50,0.6666666666666666"
)
SMA_LENS <- env_int_vec("EXIT_GRID_SMA_LENS", "10,15,20")
LOCKUPS <- env_int_vec("EXIT_GRID_LOCKUPS", "10,20,30")

CACHE_DIR <- Sys.getenv("CACHE_DIR", "cache_px_tp10_sl5")
DEBUG_YAHOO <- env_bool("DEBUG_YAHOO", FALSE)

AS_OF_DATE <- Sys.getenv("AS_OF_DATE", unset = "")
AS_OF_DATE <- if (nzchar(AS_OF_DATE)) {
  as.Date(AS_OF_DATE)
} else {
  get_default_as_of_date()
}

if (is.na(AS_OF_DATE)) {
  stop("AS_OF_DATE could not be parsed. Use format YYYY-MM-DD.")
}

NEW_SIGNAL_CUTOFF <- AS_OF_DATE - lubridate::days(EXIT_GRID_NEW_SIGNAL_LOOKBACK_DAYS)

cat("RUNNING FILE: refresh_exit_grid_results_raw.R\n")
cat("Feature source table: public.", FEATURES_TABLE, "\n", sep = "")
cat("Target raw grid table: public.", EXIT_GRID_TARGET_TABLE, "\n", sep = "")
cat("Run label:", EXIT_GRID_RUN_LABEL, "\n")
cat("As-of date:", as.character(AS_OF_DATE), "\n")
cat("New signal cutoff:", as.character(NEW_SIGNAL_CUTOFF), "\n")
cat("New signal lookback days:", EXIT_GRID_NEW_SIGNAL_LOOKBACK_DAYS, "\n")
cat("Force rebuild:", EXIT_GRID_FORCE_REBUILD, "\n")
cat("Max rows local:", as.character(EXIT_GRID_MAX_ROWS_LOCAL), "\n")
cat("Cache dir:", CACHE_DIR, "\n")
cat("Debug Yahoo:", DEBUG_YAHOO, "\n")
cat("Grid target pct(s):", paste(TARGET_PCTS, collapse = ", "), "\n")
cat("Grid stop pct(s):", paste(STOP_PCTS, collapse = ", "), "\n")
cat("Grid runner share(s):", paste(RUNNER_SHARES, collapse = ", "), "\n")
cat("Grid SMA len(s):", paste(SMA_LENS, collapse = ", "), "\n")
cat("Grid lockup(s):", paste(LOCKUPS, collapse = ", "), "\n")

###############################################################################
# 2 · Supabase connection ------------------------------------------------------
###############################################################################

connect_supabase <- function() {
  host <- get_env_first("SUPABASE_POOLER_HOST", "SUPABASE_HOST")
  port <- Sys.getenv("SUPABASE_PORT", "5432")
  dbname <- Sys.getenv("SUPABASE_DB", "postgres")
  user <- Sys.getenv("SUPABASE_USER", "")
  password <- get_env_first("SUPABASE_PWD", "SUPABASE_PASSWORD")

  if (!nzchar(host) || !nzchar(user) || !nzchar(password)) {
    stop(
      paste(
        "Missing Supabase credentials.",
        "Set SUPABASE_POOLER_HOST or SUPABASE_HOST,",
        "SUPABASE_USER, and SUPABASE_PWD/SUPABASE_PASSWORD."
      )
    )
  }

  DBI::dbConnect(
    RPostgres::Postgres(),
    host = host,
    port = as.integer(port),
    dbname = dbname,
    user = user,
    password = password,
    sslmode = "require"
  )
}

con <- connect_supabase()

on.exit({
  try(DBI::dbDisconnect(con), silent = TRUE)
}, add = TRUE)

cat("Supabase connected.\n")

qid <- function(x) as.character(DBI::dbQuoteIdentifier(con, x))

###############################################################################
# 3 · General helpers ----------------------------------------------------------
###############################################################################

`%||%` <- function(x, y) if (is.null(x)) y else x

as_logicalish <- function(x) {
  if (is.logical(x)) return(x)

  y <- tolower(trimws(as.character(x)))

  dplyr::case_when(
    is.na(y) ~ NA,
    y %in% c("true", "t", "1", "yes", "y") ~ TRUE,
    y %in% c("false", "f", "0", "no", "n") ~ FALSE,
    TRUE ~ NA
  )
}

normalize_symbol_for_yahoo <- function(sym) {
  sym <- toupper(trimws(as.character(sym)))

  dplyr::case_when(
    sym == "BRK.B" ~ "BRK-B",
    sym == "BF.B" ~ "BF-B",
    TRUE ~ sym
  )
}

safe_min_date <- function(x) {
  x <- as.Date(x)
  if (length(x) == 0 || all(is.na(x))) return(as.Date(NA))
  min(x, na.rm = TRUE)
}

safe_max_date <- function(x) {
  x <- as.Date(x)
  if (length(x) == 0 || all(is.na(x))) return(as.Date(NA))
  max(x, na.rm = TRUE)
}

safe_yahoo_to_date <- function(to_date) {
  to_date <- as.Date(to_date)

  # Never ask Yahoo daily endpoint for today/future.
  safe_to <- min(to_date, Sys.Date() - lubridate::days(1), na.rm = TRUE)

  # Roll back weekends.
  while (lubridate::wday(safe_to, week_start = 1) %in% c(6, 7)) {
    safe_to <- safe_to - lubridate::days(1)
  }

  as.Date(safe_to)
}

date_to_unix <- function(x, end_of_day = FALSE, cap_to_now = TRUE) {
  out <- as.POSIXct(as.Date(x), tz = "UTC")

  if (end_of_day) {
    out <- out + 86399
  }

  if (isTRUE(cap_to_now)) {
    now_utc <- as.POSIXct(Sys.time(), tz = "UTC") - 60

    if (!is.na(out) && out > now_utc) {
      out <- now_utc
    }
  }

  as.integer(floor(as.numeric(out)))
}

make_ts_key <- function(x) {
  x <- suppressWarnings(as.POSIXct(x, tz = "America/New_York"))
  out <- format(lubridate::with_tz(x, "America/New_York"), "%Y%m%d%H%M%S")
  out[is.na(x)] <- "NA"
  out
}

build_signal_key <- function(signal_id, username, conversation_id, ticker, tweet_ts_et) {
  signal_id_clean <- trimws(as.character(signal_id))
  signal_id_clean[is.na(signal_id_clean)] <- ""

  fallback_key <- paste(
    "fallback",
    as.character(username),
    as.character(conversation_id),
    toupper(as.character(ticker)),
    make_ts_key(tweet_ts_et),
    sep = "__"
  )

  ifelse(
    nzchar(signal_id_clean),
    paste0("signal_id::", signal_id_clean),
    fallback_key
  )
}

empty_px_tbl <- function() {
  tibble(
    date = as.Date(character()),
    open = numeric(),
    high = numeric(),
    low = numeric(),
    close = numeric(),
    volume = numeric()
  )
}

###############################################################################
# 4 · Yahoo fetch + cache ------------------------------------------------------
###############################################################################

fetch_yahoo_chart_range_text <- function(sym, range = "1y", retries = 4, debug = DEBUG_YAHOO) {
  if (is.na(sym) || !nzchar(sym)) return(NULL)

  yahoo_sym <- normalize_symbol_for_yahoo(sym)
  if (is.na(yahoo_sym) || !nzchar(yahoo_sym)) return(NULL)

  base_url <- paste0(
    "https://query2.finance.yahoo.com/v8/finance/chart/",
    URLencode(yahoo_sym, reserved = TRUE)
  )

  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) stop("curl not found on system PATH")

  args <- c(
    "--http1.1",
    "--ipv4",
    "-L",
    "--compressed",
    "--silent",
    "--show-error",
    "--user-agent", "Mozilla/5.0",
    "-G",
    base_url,
    "--data-urlencode", paste0("range=", range),
    "--data-urlencode", "interval=1d",
    "--data-urlencode", "events=history",
    "--data-urlencode", "includePrePost=false",
    "--data-urlencode", "includeAdjustedClose=true"
  )

  debug_url <- paste0(
    base_url,
    "?range=", range,
    "&interval=1d&events=history&includePrePost=false&includeAdjustedClose=true"
  )

  for (k in seq_len(retries)) {
    err_file <- tempfile(fileext = ".txt")

    out <- tryCatch(
      suppressWarnings(system2(curl_bin, args = args, stdout = TRUE, stderr = err_file)),
      error = function(e) e
    )

    status <- if (inherits(out, "error")) 1L else attr(out, "status")
    if (is.null(status)) status <- 0L

    txt <- if (inherits(out, "error")) "" else paste(out, collapse = "")

    err_txt <- if (file.exists(err_file)) {
      paste(readLines(err_file, warn = FALSE), collapse = "\n")
    } else {
      ""
    }

    if (file.exists(err_file)) unlink(err_file)

    if (debug) {
      cat("Range fallback attempt:", k, "\n")
      cat("Ticker:", sym, "\n")
      cat("Yahoo symbol:", yahoo_sym, "\n")
      cat("URL:", debug_url, "\n")
      cat("Status:", status, "\n")
      cat("Response chars:", nchar(txt), "\n")
      cat("Starts with:", substr(txt, 1, 200), "\n")
      if (nzchar(err_txt)) cat("Curl stderr:", err_txt, "\n")
    }

    if (
      identical(as.integer(status), 0L) &&
      nzchar(txt) &&
      grepl('"chart"', txt, fixed = TRUE) &&
      !grepl("Too Many Requests", txt, fixed = TRUE)
    ) {
      return(txt)
    }

    Sys.sleep(min(2^k, 10))
  }

  NULL
}

fetch_yahoo_chart_syscurl_text <- function(sym, from_date, to_date, retries = 6, debug = DEBUG_YAHOO) {
  if (is.na(from_date) || is.na(to_date) || !nzchar(sym)) return(NULL)

  yahoo_sym <- normalize_symbol_for_yahoo(sym)
  if (is.na(yahoo_sym) || !nzchar(yahoo_sym)) return(NULL)

  safe_to_date <- safe_yahoo_to_date(to_date)

  if (is.na(safe_to_date) || safe_to_date < as.Date(from_date)) {
    if (debug) {
      cat("Invalid Yahoo safe_to_date.\n")
      cat("from_date:", as.character(from_date), "\n")
      cat("to_date:", as.character(to_date), "\n")
      cat("safe_to_date:", as.character(safe_to_date), "\n")
    }
    return(NULL)
  }

  period1 <- date_to_unix(from_date, end_of_day = FALSE, cap_to_now = FALSE)
  period2 <- date_to_unix(safe_to_date, end_of_day = TRUE, cap_to_now = FALSE)

  if (is.na(period1) || is.na(period2) || period2 <= period1) {
    if (debug) {
      cat("Invalid Yahoo period range.\n")
      cat("period1:", period1, "\n")
      cat("period2:", period2, "\n")
    }
    return(NULL)
  }

  base_url <- paste0(
    "https://query2.finance.yahoo.com/v8/finance/chart/",
    URLencode(yahoo_sym, reserved = TRUE)
  )

  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) stop("curl not found on system PATH")

  args <- c(
    "--http1.1",
    "--ipv4",
    "-L",
    "--compressed",
    "--silent",
    "--show-error",
    "--user-agent", "Mozilla/5.0",
    "-G",
    base_url,
    "--data-urlencode", paste0("period1=", period1),
    "--data-urlencode", paste0("period2=", period2),
    "--data-urlencode", "interval=1d",
    "--data-urlencode", "includeAdjustedClose=true"
  )

  debug_url <- paste0(
    base_url,
    "?period1=", period1,
    "&period2=", period2,
    "&interval=1d&includeAdjustedClose=true"
  )

  for (k in seq_len(retries)) {
    err_file <- tempfile(fileext = ".txt")

    out <- tryCatch(
      suppressWarnings(system2(curl_bin, args = args, stdout = TRUE, stderr = err_file)),
      error = function(e) e
    )

    status <- if (inherits(out, "error")) 1L else attr(out, "status")
    if (is.null(status)) status <- 0L

    txt <- if (inherits(out, "error")) "" else paste(out, collapse = "")

    err_txt <- if (file.exists(err_file)) {
      paste(readLines(err_file, warn = FALSE), collapse = "\n")
    } else {
      ""
    }

    if (file.exists(err_file)) unlink(err_file)

    if (debug) {
      cat("Attempt:", k, "\n")
      cat("Ticker:", sym, "\n")
      cat("Yahoo symbol:", yahoo_sym, "\n")
      cat("URL:", debug_url, "\n")
      cat("Status:", status, "\n")
      cat("Response chars:", nchar(txt), "\n")
      cat("Starts with:", substr(txt, 1, 200), "\n")
      if (nzchar(err_txt)) cat("Curl stderr:", err_txt, "\n")
    }

    if (
      identical(as.integer(status), 0L) &&
      nzchar(txt) &&
      grepl('"chart"', txt, fixed = TRUE) &&
      !grepl("Too Many Requests", txt, fixed = TRUE)
    ) {
      return(txt)
    }

    Sys.sleep(min(2^k, 8))
  }

  NULL
}

read_yahoo_chart_text <- function(txt) {
  if (is.null(txt) || !nzchar(txt)) return(empty_px_tbl())

  js <- tryCatch(
    jsonlite::fromJSON(txt, simplifyVector = FALSE),
    error = function(e) {
      message("Could not parse Yahoo JSON: ", e$message)
      NULL
    }
  )

  if (is.null(js) || is.null(js$chart) || !is.null(js$chart$error)) {
    return(empty_px_tbl())
  }

  res <- js$chart$result
  if (is.null(res) || length(res) == 0 || is.null(res[[1]])) return(empty_px_tbl())

  res1 <- res[[1]]
  timestamps <- res1$timestamp
  quote_obj <- res1$indicators$quote[[1]]

  if (is.null(timestamps) || is.null(quote_obj)) return(empty_px_tbl())

  ts_vec <- unlist(timestamps)
  open_vec <- unlist(quote_obj$open)
  high_vec <- unlist(quote_obj$high)
  low_vec <- unlist(quote_obj$low)
  close_vec <- unlist(quote_obj$close)
  vol_vec <- unlist(quote_obj$volume)

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
    date = as.Date(as.POSIXct(ts_vec[seq_len(min_len)], origin = "1970-01-01", tz = "UTC")),
    open = suppressWarnings(as.numeric(open_vec[seq_len(min_len)])),
    high = suppressWarnings(as.numeric(high_vec[seq_len(min_len)])),
    low = suppressWarnings(as.numeric(low_vec[seq_len(min_len)])),
    close = suppressWarnings(as.numeric(close_vec[seq_len(min_len)])),
    volume = suppressWarnings(as.numeric(vol_vec[seq_len(min_len)]))
  ) %>%
    filter(!is.na(date), !is.na(close)) %>%
    arrange(date)
}

get_px_via_syscurl_yahoo <- function(sym, from, to, debug = DEBUG_YAHOO) {
  from <- as.Date(from)
  to <- as.Date(to)

  if (is.na(from) || is.na(to) || from > to || !nzchar(sym)) {
    return(empty_px_tbl())
  }

  txt <- fetch_yahoo_chart_syscurl_text(
    sym = sym,
    from_date = from,
    to_date = to,
    debug = debug
  )

  px <- read_yahoo_chart_text(txt)

  if (nrow(px) == 0) {
    message("Period-based Yahoo fetch failed for ", sym, ". Trying Yahoo range fallback...")

    txt2 <- fetch_yahoo_chart_range_text(
      sym = sym,
      range = "1y",
      debug = debug
    )

    px <- read_yahoo_chart_text(txt2)
  }

  if (nrow(px) == 0) {
    message("Yahoo range fallback also failed for ", sym)
    return(empty_px_tbl())
  }

  px %>%
    filter(!is.na(date), date >= from, date <= to) %>%
    arrange(date)
}

get_px_cached <- function(sym, from, to, cache_dir = CACHE_DIR, fetch_fn = get_px_via_syscurl_yahoo) {
  if (is.na(from) || is.na(to) || !nzchar(sym)) return(empty_px_tbl())

  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)

  safe_sym <- gsub("[^A-Za-z0-9_.-]", "_", normalize_symbol_for_yahoo(sym))
  fname <- file.path(cache_dir, paste0(safe_sym, ".rds"))

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
  need_to <- as.Date(to)

  fetch_ranges <- list()

  if (nrow(cached) == 0) {
    fetch_ranges <- list(c(need_from, need_to))
  } else {
    have_min <- min(cached$date, na.rm = TRUE)
    have_max <- max(cached$date, na.rm = TRUE)

    if (need_from < have_min) {
      fetch_ranges <- append(fetch_ranges, list(c(need_from, have_min - days(1))))
    }

    if (need_to > have_max) {
      fetch_ranges <- append(fetch_ranges, list(c(have_max + days(1), need_to)))
    }
  }

  if (length(fetch_ranges) > 0) {
    for (rng in fetch_ranges) {
      fr <- as.Date(rng[1])
      tr <- as.Date(rng[2])

      if (!is.na(fr) && !is.na(tr) && fr <= tr) {
        fresh <- tryCatch(
          fetch_fn(sym = sym, from = fr, to = tr),
          error = function(e) {
            message(sprintf("Fetch failed for %s [%s to %s]: %s", sym, fr, tr, e$message))
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
        }
      }
    }
  }

  if (nrow(cached) == 0 || !"date" %in% names(cached)) {
    return(empty_px_tbl())
  }

  cached %>%
    mutate(date = as.Date(date)) %>%
    filter(!is.na(date), date >= need_from, date <= need_to) %>%
    arrange(date)
}

###############################################################################
# 5 · Smoke test ---------------------------------------------------------------
###############################################################################

cat("Running price smoke test with AMZN...\n")

smoke_from <- AS_OF_DATE - days(20)
smoke_to <- safe_yahoo_to_date(AS_OF_DATE)

cat("Price smoke test window:", as.character(smoke_from), "to", as.character(smoke_to), "\n")

smoke_px <- get_px_cached(
  sym = "AMZN",
  from = smoke_from,
  to = smoke_to,
  cache_dir = CACHE_DIR
)

cat("Yahoo smoke test AMZN rows:", nrow(smoke_px), "\n")

if (nrow(smoke_px) == 0) {
  warning("Yahoo smoke test failed: no AMZN rows returned. Continuing anyway.")
}

###############################################################################
# 6 · Read feature-table signals -----------------------------------------------
###############################################################################

features_tbl_id <- DBI::Id(schema = "public", table = FEATURES_TABLE)

if (!DBI::dbExistsTable(con, features_tbl_id)) {
  stop("Features table does not exist: public.", FEATURES_TABLE)
}

signals_from_features <- DBI::dbReadTable(con, features_tbl_id) %>%
  as_tibble()

cat("Feature table rows:", nrow(signals_from_features), "\n")

required_feature_cols <- c(
  "created_at", "ticker", "direction", "entry_date", "entry_at",
  "feature_date", "sma10_prior", "prev_close", "prev_dist_sma10",
  "ret_5d_prior", "ret_20d_prior", "volatility_20d"
)

missing_required <- setdiff(required_feature_cols, names(signals_from_features))
if (length(missing_required) > 0) {
  stop("Feature table is missing required columns: ", paste(missing_required, collapse = ", "))
}

signals <- signals_from_features %>%
  mutate(
    row_id = if ("row_id" %in% names(.)) as.integer(row_id) else dplyr::row_number(),
    signal_id = if ("signal_id" %in% names(.)) as.character(signal_id) else NA_character_,

    created_at_utc = if ("created_at_utc" %in% names(.)) {
      as.POSIXct(created_at_utc, tz = "UTC")
    } else {
      as.POSIXct(created_at, tz = "UTC")
    },

    tweet_ts_et = if ("tweet_ts_et" %in% names(.)) {
      lubridate::with_tz(as.POSIXct(tweet_ts_et, tz = "UTC"), "America/New_York")
    } else {
      lubridate::with_tz(created_at_utc, "America/New_York")
    },

    ticker = str_to_upper(as.character(ticker)),

    side = if ("direction" %in% names(.)) {
      str_to_lower(as.character(direction))
    } else if ("side" %in% names(.)) {
      str_to_lower(as.character(side))
    } else {
      NA_character_
    },

    horizon = if ("horizon" %in% names(.)) {
      str_to_lower(str_trim(as.character(horizon)))
    } else {
      NA_character_
    },

    horizon = case_when(
      horizon %in% c("short", "mid", "long") ~ horizon,
      is.na(horizon) | horizon == "" ~ "missing",
      TRUE ~ "other"
    ),

    conversation_id = if ("conversation_id" %in% names(.)) as.character(conversation_id) else NA_character_,
    conviction = if ("conviction" %in% names(.)) suppressWarnings(as.numeric(conviction)) else NA_real_,
    buy_zone = if ("buy_zone" %in% names(.)) as_logicalish(buy_zone) else NA,
    sentiment_score = if ("sentiment_score" %in% names(.)) suppressWarnings(as.numeric(sentiment_score)) else NA_real_,
    evidence_score = if ("evidence_score" %in% names(.)) suppressWarnings(as.numeric(evidence_score)) else NA_real_,
    specificity_score = if ("specificity_score" %in% names(.)) suppressWarnings(as.numeric(specificity_score)) else NA_real_,
    has_quant_data = if ("has_quant_data" %in% names(.)) as_logicalish(has_quant_data) else NA,

    signal_date = if ("signal_date" %in% names(.)) as.Date(signal_date) else as.Date(tweet_ts_et),
    entry_date = as.Date(entry_date),
    entry_at = str_to_lower(as.character(entry_at)),
    feature_date = as.Date(feature_date),
    feature_lag_days = if ("feature_lag_days" %in% names(.)) as.integer(feature_lag_days) else NA_integer_,

    sma10_prior = suppressWarnings(as.numeric(sma10_prior)),
    prev_close = suppressWarnings(as.numeric(prev_close)),
    prev_dist_sma10 = suppressWarnings(as.numeric(prev_dist_sma10)),
    ret_5d_prior = suppressWarnings(as.numeric(ret_5d_prior)),
    ret_20d_prior = suppressWarnings(as.numeric(ret_20d_prior)),
    volatility_20d = suppressWarnings(as.numeric(volatility_20d)),

    signal_key = build_signal_key(
      signal_id = signal_id,
      username = if ("username" %in% names(.)) username else NA_character_,
      conversation_id = conversation_id,
      ticker = ticker,
      tweet_ts_et = tweet_ts_et
    )
  ) %>%
  filter(
    !is.na(tweet_ts_et),
    !is.na(ticker),
    ticker != "",
    side %in% c("long", "short"),
    !is.na(entry_date),
    entry_at %in% c("open", "close")
  )

cat("Prepared valid feature-table signals:", nrow(signals), "\n")

if (nrow(signals) > 0) {
  print(signals %>% summarise(
    rows = n(),
    unique_signal_ids = n_distinct(signal_id, na.rm = TRUE),
    unique_signal_keys = n_distinct(signal_key, na.rm = TRUE),
    min_signal_date = safe_min_date(signal_date),
    max_signal_date = safe_max_date(signal_date),
    rows_with_entry_date = sum(!is.na(entry_date)),
    rows_with_volatility_20d = sum(!is.na(volatility_20d)),
    rows_with_sma10_prior = sum(!is.na(sma10_prior))
  ))
}

###############################################################################
# 7 · Parameter grid -----------------------------------------------------------
###############################################################################

param_grid <- tidyr::crossing(
  target_pct = TARGET_PCTS,
  stop_pct = STOP_PCTS,
  runner_share = RUNNER_SHARES,
  sma_len = SMA_LENS,
  lockup = LOCKUPS
) %>%
  mutate(combo_id = row_number()) %>%
  select(combo_id, everything())

print(param_grid)
cat("Grid combinations:", nrow(param_grid), "\n")

###############################################################################
# 8 · Select signals to process ------------------------------------------------
###############################################################################

target_tbl <- DBI::Id(schema = "public", table = EXIT_GRID_TARGET_TABLE)
target_exists <- DBI::dbExistsTable(con, target_tbl)

get_existing_grid_status <- function() {
  if (!target_exists) {
    return(tibble(
      signal_key = character(),
      signal_id = character(),
      combo_count = integer(),
      has_open = logical()
    ))
  }

  fields <- DBI::dbListFields(con, target_tbl)

  if (!all(c("grid_run_label", "combo_id") %in% fields)) {
    return(tibble(
      signal_key = character(),
      signal_id = character(),
      combo_count = integer(),
      has_open = logical()
    ))
  }

  key_col <- if ("signal_key" %in% fields) {
    "signal_key"
  } else if ("signal_id" %in% fields) {
    "signal_id"
  } else {
    NA_character_
  }

  if (is.na(key_col)) {
    return(tibble(
      signal_key = character(),
      signal_id = character(),
      combo_count = integer(),
      has_open = logical()
    ))
  }

  completed_expr <- if ("completed" %in% fields) {
    "COALESCE(completed, false) = false"
  } else {
    "false"
  }

  trade_status_expr <- if ("trade_status" %in% fields) {
    "COALESCE(trade_status, '') <> 'completed'"
  } else {
    "false"
  }

  sql <- paste0(
    "SELECT ",
    DBI::dbQuoteIdentifier(con, key_col),
    " AS row_key, ",
    "COUNT(DISTINCT combo_id) AS combo_count, ",
    "BOOL_OR(", completed_expr, " OR ", trade_status_expr, ") AS has_open ",
    "FROM ",
    DBI::dbQuoteIdentifier(con, target_tbl),
    " WHERE grid_run_label = $1 ",
    "GROUP BY ",
    DBI::dbQuoteIdentifier(con, key_col)
  )

  out <- DBI::dbGetQuery(con, sql, params = list(EXIT_GRID_RUN_LABEL)) %>%
    as_tibble() %>%
    mutate(
      row_key = as.character(row_key),
      combo_count = as.integer(combo_count),
      has_open = as.logical(has_open)
    )

  if (identical(key_col, "signal_key")) {
    out %>%
      transmute(
        signal_key = row_key,
        signal_id = NA_character_,
        combo_count,
        has_open
      )
  } else {
    out %>%
      transmute(
        signal_key = NA_character_,
        signal_id = row_key,
        combo_count,
        has_open
      )
  }
}

existing_grid_status <- get_existing_grid_status()

cat("Existing raw grid grouped rows:", nrow(existing_grid_status), "\n")

if (EXIT_GRID_FORCE_REBUILD || nrow(existing_grid_status) == 0) {
  signals_run <- signals

  if (EXIT_GRID_FORCE_REBUILD) {
    cat("EXIT_GRID_FORCE_REBUILD is TRUE. Full grid rebuild selected.\n")
  } else {
    cat("Raw grid table is empty or has no usable grid status. Full initial grid selected.\n")
  }

} else {
  if (any(!is.na(existing_grid_status$signal_key))) {
    full_existing <- existing_grid_status %>%
      filter(combo_count >= nrow(param_grid), !is.na(signal_key), nzchar(signal_key)) %>%
      select(signal_key)

    incomplete_existing <- existing_grid_status %>%
      filter(combo_count < nrow(param_grid), !is.na(signal_key), nzchar(signal_key)) %>%
      select(signal_key)

    open_existing <- existing_grid_status %>%
      filter(has_open %in% TRUE, !is.na(signal_key), nzchar(signal_key)) %>%
      select(signal_key)

    new_recent_signals <- signals %>%
      filter(
        signal_date >= NEW_SIGNAL_CUTOFF,
        signal_date <= AS_OF_DATE
      ) %>%
      anti_join(full_existing, by = "signal_key")

    incomplete_signals <- signals %>%
      semi_join(incomplete_existing, by = "signal_key")

    open_signals <- signals %>%
      semi_join(open_existing, by = "signal_key")

  } else {
    full_existing <- existing_grid_status %>%
      filter(combo_count >= nrow(param_grid), !is.na(signal_id), nzchar(signal_id)) %>%
      select(signal_id)

    incomplete_existing <- existing_grid_status %>%
      filter(combo_count < nrow(param_grid), !is.na(signal_id), nzchar(signal_id)) %>%
      select(signal_id)

    open_existing <- existing_grid_status %>%
      filter(has_open %in% TRUE, !is.na(signal_id), nzchar(signal_id)) %>%
      select(signal_id)

    new_recent_signals <- signals %>%
      filter(
        signal_date >= NEW_SIGNAL_CUTOFF,
        signal_date <= AS_OF_DATE
      ) %>%
      anti_join(full_existing, by = "signal_id")

    incomplete_signals <- signals %>%
      semi_join(incomplete_existing, by = "signal_id")

    open_signals <- signals %>%
      semi_join(open_existing, by = "signal_id")
  }

  signals_run <- bind_rows(
    new_recent_signals,
    incomplete_signals,
    open_signals
  ) %>%
    distinct(signal_key, .keep_all = TRUE)

  cat("New recent grid signals:", nrow(new_recent_signals), "\n")
  cat("Incomplete existing grid signals:", nrow(incomplete_signals), "\n")
  cat("Open existing grid signals:", nrow(open_signals), "\n")
}

if (is.finite(EXIT_GRID_MAX_ROWS_LOCAL)) {
  signals_run <- signals_run %>% slice_head(n = EXIT_GRID_MAX_ROWS_LOCAL)
}

cat("Total grid signals to process:", nrow(signals_run), "\n")
cat("Total expected simulations this run:", nrow(signals_run) * nrow(param_grid), "\n")

if (nrow(signals_run) == 0) {
  cat("No raw exit-grid rows to process. Exiting.\n")
  quit(save = "no", status = 0)
}

###############################################################################
# 9 · Gap-aware strategy logic with runner-share grid --------------------------
###############################################################################

simulate_trade <- function(row,
                           px,
                           lookback = LOOKBACK_DAYS,
                           lockup = 30,
                           sma_len = 10,
                           target_pct = 0.10,
                           stop_pct = 0.05,
                           runner_share = 1/3,
                           as_of_date = AS_OF_DATE) {

  runner_share <- suppressWarnings(as.numeric(runner_share))
  if (is.na(runner_share)) runner_share <- 1/3
  runner_share <- max(min(runner_share, 1), 0)

  first_exit_share <- 1 - runner_share

  ticker <- row$ticker[[1]]
  tweet_ts_et <- row$tweet_ts_et[[1]]
  side <- tolower(row$side[[1]] %||% "long")

  entry_date <- as.Date(row$entry_date[[1]])
  entry_at <- as.character(row$entry_at[[1]])

  if (!is.data.frame(px) || nrow(px) == 0) return(tibble())
  if (is.na(ticker) || ticker == "" || is.na(tweet_ts_et)) return(tibble())
  if (is.na(entry_date) || is.na(entry_at) || entry_at == "") return(tibble())

  is_short <- identical(side, "short")

  if (is.null(as_of_date) || is.na(as_of_date)) {
    as_of_date <- max(px$date, na.rm = TRUE)
  }

  as_of_date <- as.Date(as_of_date)

  px_full <- px %>%
    filter(date <= as_of_date) %>%
    arrange(date) %>%
    mutate(
      date = as.Date(date),
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
  if (max(px_full$date, na.rm = TRUE) < entry_date) return(tibble())

  data_through_date <- max(px_full$date, na.rm = TRUE)

  entry_row <- px_full %>%
    filter(date == entry_date) %>%
    slice_head(n = 1)

  if (nrow(entry_row) == 0) return(tibble())

  rows_before_entry <- sum(px_full$date <= entry_date)
  non_na_closes_before_entry <- sum(!is.na(px_full$close[px_full$date <= entry_date]))

  entry_price <- if (entry_at == "open") {
    entry_row$open[[1]]
  } else {
    entry_row$close[[1]]
  }

  if (is.na(entry_price) || entry_price <= 0) return(tibble())

  # Use the prior day's selected SMA value for this grid combo.
  # This avoids lookahead and lets SMA10/SMA15/SMA20 be tested fairly.
  entry_idx <- which(px_full$date == entry_date)[1]
  entry_sma <- if (!is.na(entry_idx) && entry_idx > 1) {
    px_full$sma_val[[entry_idx - 1]]
  } else {
    NA_real_
  }

  entry_below_sma10 <- if (!is.na(entry_sma)) {
    entry_price < entry_sma
  } else {
    NA
  }

  dist_sma10 <- suppressWarnings(as.numeric(row$prev_dist_sma10[[1]]))
  ret_5d_prior <- suppressWarnings(as.numeric(row$ret_5d_prior[[1]]))
  ret_20d_prior <- suppressWarnings(as.numeric(row$ret_20d_prior[[1]]))
  volatility_20d <- suppressWarnings(as.numeric(row$volatility_20d[[1]]))
  prev_close <- suppressWarnings(as.numeric(row$prev_close[[1]]))

  gap_from_prev_close <- if (
    entry_at == "open" &&
      !is.na(prev_close) &&
      prev_close > 0 &&
      !is.na(entry_price)
  ) {
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

  tgt_px <- if (is_short) {
    entry_price * (1 - target_pct)
  } else {
    entry_price * (1 + target_pct)
  }

  stop_px <- if (is_short) {
    entry_price * (1 + stop_pct)
  } else {
    entry_price * (1 - stop_pct)
  }

  scan_start <- if (entry_at == "close") {
    entry_date + days(1)
  } else {
    entry_date
  }

  daily <- px_scan %>%
    filter(date >= scan_start, date <= lockup_exit_d) %>%
    mutate(
      gap_stop_hit = if (is_short) {
        open >= stop_px
      } else {
        open <= stop_px
      },

      gap_take_hit = if (is_short) {
        open <= tgt_px
      } else {
        open >= tgt_px
      },

      intraday_stop_hit = if (is_short) {
        high >= stop_px
      } else {
        low <= stop_px
      },

      intraday_take_hit = if (is_short) {
        low <= tgt_px
      } else {
        high >= tgt_px
      },

      event_type = case_when(
        gap_stop_hit        ~ "gap_stop",
        gap_take_hit        ~ "gap_target",
        intraday_stop_hit   ~ "intraday_stop",
        intraday_take_hit   ~ "intraday_target",
        TRUE                ~ NA_character_
      ),

      event_price = case_when(
        event_type == "gap_stop"        ~ open,
        event_type == "gap_target"      ~ open,
        event_type == "intraday_stop"   ~ stop_px,
        event_type == "intraday_target" ~ tgt_px,
        TRUE                            ~ NA_real_
      ),

      stop_hit = event_type %in% c("gap_stop", "intraday_stop"),
      take_hit = event_type %in% c("gap_target", "intraday_target")
    )

  first_event <- daily %>%
    filter(!is.na(event_type)) %>%
    slice_head(n = 1)

  part_exit <- FALSE
  stop_exit <- FALSE
  completed <- TRUE
  trade_status <- "completed"
  completion_reason <- NA_character_
  exit_reason <- NA_character_

  first_exit_type <- NA_character_
  final_exit_type <- NA_character_

  mtm_row <- px_full %>% slice_tail(n = 1)
  mtm_date <- mtm_row$date[[1]]
  mtm_price <- mtm_row$close[[1]]

  if (nrow(first_event) == 0) {

    if (data_through_date < lockup_exit_d) {
      part_exit <- FALSE
      stop_exit <- FALSE
      completed <- FALSE
      trade_status <- "open_no_exit_yet"
      completion_reason <- "within_lockup_no_exit_yet"
      exit_reason <- "open_no_exit_yet"

      first_exit_d <- as.Date(NA)
      first_exit_px <- NA_real_
      final_exit_d <- mtm_date
      final_exit_px <- mtm_price

      first_exit_type <- NA_character_
      final_exit_type <- "mark_to_market"

    } else {
      part_exit <- FALSE
      stop_exit <- FALSE
      completed <- TRUE
      trade_status <- "completed"
      completion_reason <- "time_exit_lockup"
      exit_reason <- "time_exit_lockup"

      day_row <- px_full %>%
        filter(date == lockup_exit_d) %>%
        slice_head(n = 1)

      if (nrow(day_row) == 0) {
        day_row <- px_full %>% slice_tail(n = 1)
      }

      first_exit_d <- day_row$date[[1]]
      first_exit_px <- day_row$close[[1]]
      final_exit_d <- first_exit_d
      final_exit_px <- first_exit_px

      first_exit_type <- "time_exit_lockup"
      final_exit_type <- "time_exit_lockup"
    }

  } else if (isTRUE(first_event$stop_hit[[1]])) {

    part_exit <- FALSE
    stop_exit <- TRUE
    completed <- TRUE
    trade_status <- "completed"
    completion_reason <- "stop_first"
    exit_reason <- first_event$event_type[[1]]

    first_exit_d <- first_event$date[[1]]
    first_exit_px <- first_event$event_price[[1]]
    final_exit_d <- first_exit_d
    final_exit_px <- first_exit_px

    first_exit_type <- first_event$event_type[[1]]
    final_exit_type <- first_event$event_type[[1]]

  } else {

    part_exit <- TRUE
    stop_exit <- FALSE

    first_exit_d <- first_event$date[[1]]
    first_exit_px <- first_event$event_price[[1]]
    first_exit_type <- first_event$event_type[[1]]

    post_target_px <- px_full %>%
      filter(date > first_exit_d)

    if (nrow(post_target_px) == 0) {
      completed <- FALSE
      trade_status <- "open_runner"
      completion_reason <- "runner_no_post_target_data"
      exit_reason <- "open_runner"

      final_exit_d <- mtm_date
      final_exit_px <- mtm_price
      final_exit_type <- "mark_to_market"

    } else if (!is_short) {

      if (!isTRUE(entry_below_sma10)) {

        trail_exit <- post_target_px %>%
          filter(close < sma_val) %>%
          slice_head(n = 1)

        if (nrow(trail_exit) > 0) {
          completed <- TRUE
          trade_status <- "completed"
          completion_reason <- "runner_trail_exit"
          exit_reason <- "runner_trail_exit"

          final_exit_d <- trail_exit$date[[1]]
          final_exit_px <- trail_exit$close[[1]]
          final_exit_type <- "runner_trail_exit"

        } else {
          completed <- FALSE
          trade_status <- "open_runner"
          completion_reason <- "runner_no_trail_exit_yet"
          exit_reason <- "open_runner"

          final_exit_d <- mtm_date
          final_exit_px <- mtm_price
          final_exit_type <- "mark_to_market"
        }

      } else {

        armed_rows <- post_target_px %>%
          mutate(above_sma = close > sma_val)

        first_reclaim <- armed_rows %>%
          filter(above_sma) %>%
          slice_head(n = 1)

        pre_reclaim_stop <- armed_rows %>%
          {
            if (nrow(first_reclaim) > 0) {
              filter(., date <= first_reclaim$date[[1]])
            } else {
              .
            }
          } %>%
          mutate(
            pre_reclaim_gap_stop_hit = open <= stop_px,
            pre_reclaim_intraday_stop_hit = low <= stop_px,

            pre_reclaim_stop_type = case_when(
              pre_reclaim_gap_stop_hit      ~ "pre_reclaim_gap_stop",
              pre_reclaim_intraday_stop_hit ~ "pre_reclaim_intraday_stop",
              TRUE                          ~ NA_character_
            ),

            pre_reclaim_stop_price = case_when(
              pre_reclaim_gap_stop_hit      ~ open,
              pre_reclaim_intraday_stop_hit ~ stop_px,
              TRUE                          ~ NA_real_
            )
          ) %>%
          filter(!is.na(pre_reclaim_stop_type)) %>%
          slice_head(n = 1)

        if (nrow(pre_reclaim_stop) > 0) {
          completed <- TRUE
          trade_status <- "completed"
          completion_reason <- "pre_reclaim_stop"
          exit_reason <- pre_reclaim_stop$pre_reclaim_stop_type[[1]]

          final_exit_d <- pre_reclaim_stop$date[[1]]
          final_exit_px <- pre_reclaim_stop$pre_reclaim_stop_price[[1]]
          final_exit_type <- pre_reclaim_stop$pre_reclaim_stop_type[[1]]

        } else if (nrow(first_reclaim) == 0) {
          completed <- FALSE
          trade_status <- "open_runner"
          completion_reason <- "runner_waiting_for_sma_reclaim"
          exit_reason <- "open_runner"

          final_exit_d <- mtm_date
          final_exit_px <- mtm_price
          final_exit_type <- "mark_to_market"

        } else {

          trail_exit <- armed_rows %>%
            filter(date > first_reclaim$date[[1]], close < sma_val) %>%
            slice_head(n = 1)

          if (nrow(trail_exit) > 0) {
            completed <- TRUE
            trade_status <- "completed"
            completion_reason <- "runner_trail_exit"
            exit_reason <- "runner_trail_exit"

            final_exit_d <- trail_exit$date[[1]]
            final_exit_px <- trail_exit$close[[1]]
            final_exit_type <- "runner_trail_exit"

          } else {
            completed <- FALSE
            trade_status <- "open_runner"
            completion_reason <- "runner_no_trail_exit_yet"
            exit_reason <- "open_runner"

            final_exit_d <- mtm_date
            final_exit_px <- mtm_price
            final_exit_type <- "mark_to_market"
          }
        }
      }

    } else {

      if (isTRUE(entry_below_sma10)) {

        trail_exit <- post_target_px %>%
          filter(close > sma_val) %>%
          slice_head(n = 1)

        if (nrow(trail_exit) > 0) {
          completed <- TRUE
          trade_status <- "completed"
          completion_reason <- "runner_trail_exit"
          exit_reason <- "runner_trail_exit"

          final_exit_d <- trail_exit$date[[1]]
          final_exit_px <- trail_exit$close[[1]]
          final_exit_type <- "runner_trail_exit"

        } else {
          completed <- FALSE
          trade_status <- "open_runner"
          completion_reason <- "runner_no_trail_exit_yet"
          exit_reason <- "open_runner"

          final_exit_d <- mtm_date
          final_exit_px <- mtm_price
          final_exit_type <- "mark_to_market"
        }

      } else {

        armed_rows <- post_target_px %>%
          mutate(below_sma = close < sma_val)

        first_reclaim <- armed_rows %>%
          filter(below_sma) %>%
          slice_head(n = 1)

        pre_reclaim_stop <- armed_rows %>%
          {
            if (nrow(first_reclaim) > 0) {
              filter(., date <= first_reclaim$date[[1]])
            } else {
              .
            }
          } %>%
          mutate(
            pre_reclaim_gap_stop_hit = open >= stop_px,
            pre_reclaim_intraday_stop_hit = high >= stop_px,

            pre_reclaim_stop_type = case_when(
              pre_reclaim_gap_stop_hit      ~ "pre_reclaim_gap_stop",
              pre_reclaim_intraday_stop_hit ~ "pre_reclaim_intraday_stop",
              TRUE                          ~ NA_character_
            ),

            pre_reclaim_stop_price = case_when(
              pre_reclaim_gap_stop_hit      ~ open,
              pre_reclaim_intraday_stop_hit ~ stop_px,
              TRUE                          ~ NA_real_
            )
          ) %>%
          filter(!is.na(pre_reclaim_stop_type)) %>%
          slice_head(n = 1)

        if (nrow(pre_reclaim_stop) > 0) {
          completed <- TRUE
          trade_status <- "completed"
          completion_reason <- "pre_reclaim_stop"
          exit_reason <- pre_reclaim_stop$pre_reclaim_stop_type[[1]]

          final_exit_d <- pre_reclaim_stop$date[[1]]
          final_exit_px <- pre_reclaim_stop$pre_reclaim_stop_price[[1]]
          final_exit_type <- pre_reclaim_stop$pre_reclaim_stop_type[[1]]

        } else if (nrow(first_reclaim) == 0) {
          completed <- FALSE
          trade_status <- "open_runner"
          completion_reason <- "runner_waiting_for_sma_reclaim"
          exit_reason <- "open_runner"

          final_exit_d <- mtm_date
          final_exit_px <- mtm_price
          final_exit_type <- "mark_to_market"

        } else {

          trail_exit <- armed_rows %>%
            filter(date > first_reclaim$date[[1]], close > sma_val) %>%
            slice_head(n = 1)

          if (nrow(trail_exit) > 0) {
            completed <- TRUE
            trade_status <- "completed"
            completion_reason <- "runner_trail_exit"
            exit_reason <- "runner_trail_exit"

            final_exit_d <- trail_exit$date[[1]]
            final_exit_px <- trail_exit$close[[1]]
            final_exit_type <- "runner_trail_exit"

          } else {
            completed <- FALSE
            trade_status <- "open_runner"
            completion_reason <- "runner_no_trail_exit_yet"
            exit_reason <- "open_runner"

            final_exit_d <- mtm_date
            final_exit_px <- mtm_price
            final_exit_type <- "mark_to_market"
          }
        }
      }
    }
  }

  first_leg_return <- if (is_short) {
    (entry_price - first_exit_px) / entry_price
  } else {
    (first_exit_px - entry_price) / entry_price
  }

  runner_leg_return <- if (is_short) {
    (entry_price - final_exit_px) / entry_price
  } else {
    (final_exit_px - entry_price) / entry_price
  }

  final_return <- if (part_exit) {
    first_exit_share * first_leg_return + runner_share * runner_leg_return
  } else {
    runner_leg_return
  }

  tibble(
    entry_price = entry_price,
    entry_sma = entry_sma,
    has_entry_sma = !is.na(entry_sma),
    entry_below_sma10 = entry_below_sma10,

    dist_sma10 = dist_sma10,
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
    first_exit_type = first_exit_type,

    final_exit_date = final_exit_d,
    final_exit_price = final_exit_px,
    final_exit_type = final_exit_type,

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
    runner_share = runner_share,
    first_exit_share = first_exit_share,
    sma_len = as.integer(sma_len),
    lockup = as.integer(lockup),
    total_return = final_return
  )
}

###############################################################################
# 10 · Run one signal across all parameter combinations -------------------------
###############################################################################

run_one_signal_grid <- function(row_i,
                                param_grid,
                                cache_dir = CACHE_DIR,
                                as_of_date = AS_OF_DATE) {

  px_from <- min(
    as.Date(row_i$feature_date[[1]]),
    as.Date(row_i$entry_date[[1]]),
    as.Date(row_i$tweet_ts_et[[1]]),
    na.rm = TRUE
  ) - days(250)

  px <- get_px_cached(
    sym = row_i$ticker[[1]],
    from = px_from,
    to = as_of_date,
    cache_dir = cache_dir
  )

  if (!is.data.frame(px) || nrow(px) == 0) return(tibble())

  purrr::pmap_dfr(
    param_grid,
    function(combo_id, target_pct, stop_pct, runner_share, sma_len, lockup) {
      out <- tryCatch(
        simulate_trade(
          row = row_i,
          px = px,
          lookback = LOOKBACK_DAYS,
          lockup = lockup,
          sma_len = sma_len,
          target_pct = target_pct,
          stop_pct = stop_pct,
          runner_share = runner_share,
          as_of_date = as_of_date
        ),
        error = function(e) {
          message("Simulation error for ", row_i$ticker[[1]], ": ", e$message)
          tibble()
        }
      )

      if (nrow(out) == 0) return(tibble())

      bind_cols(row_i, out) %>%
        mutate(
          combo_id = combo_id,
          target_pct = target_pct,
          stop_pct = stop_pct,
          runner_share = runner_share,
          sma_len = as.integer(sma_len),
          lockup = as.integer(lockup)
        )
    }
  )
}

###############################################################################
# 11 · Run grid ----------------------------------------------------------------
###############################################################################

grid_results <- purrr::map_dfr(
  seq_len(nrow(signals_run)),
  function(i) {
    row_i <- signals_run[i, , drop = FALSE]

    username_i <- if ("username" %in% names(row_i)) {
      as.character(row_i$username[[1]])
    } else {
      NA_character_
    }

    message(sprintf(
      "[%d/%d] Raw grid processing: %s | %s | %s",
      i,
      nrow(signals_run),
      username_i,
      row_i$ticker[[1]],
      as.character(row_i$entry_date[[1]])
    ))

    run_one_signal_grid(
      row_i = row_i,
      param_grid = param_grid,
      cache_dir = CACHE_DIR,
      as_of_date = AS_OF_DATE
    )
  }
)

cat("Raw grid result rows produced:", nrow(grid_results), "\n")

if (nrow(grid_results) == 0) {
  cat("No grid rows produced. Exiting.\n")
  quit(save = "no", status = 0)
}

###############################################################################
# 12 · Upload helpers ----------------------------------------------------------
###############################################################################

clean_names_simple <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9_]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  x <- ifelse(x == "", "col", x)
  make.unique(x, sep = "_")
}

clean_for_supabase <- function(df) {
  df <- as_tibble(df)

  names(df) <- clean_names_simple(names(df))

  factor_cols <- names(df)[purrr::map_lgl(df, is.factor)]
  if (length(factor_cols) > 0) {
    df[factor_cols] <- lapply(df[factor_cols], as.character)
  }

  # Convert Date to ISO text. Existing DATE columns in Postgres should cast them.
  date_cols <- names(df)[purrr::map_lgl(df, inherits, what = "Date")]
  if (length(date_cols) > 0) {
    df[date_cols] <- lapply(df[date_cols], as.character)
  }

  # Convert POSIX to UTC ISO text. Existing TIMESTAMPTZ/TEXT columns should accept them.
  posix_cols <- names(df)[purrr::map_lgl(df, ~ inherits(.x, "POSIXt"))]
  if (length(posix_cols) > 0) {
    df[posix_cols] <- lapply(df[posix_cols], function(x) {
      as.character(lubridate::with_tz(as.POSIXct(x), "UTC"))
    })
  }

  difftime_cols <- names(df)[purrr::map_lgl(df, ~ inherits(.x, "difftime"))]
  if (length(difftime_cols) > 0) {
    df[difftime_cols] <- lapply(df[difftime_cols], as.numeric)
  }

  numeric_cols <- names(df)[purrr::map_lgl(df, is.numeric)]
  if (length(numeric_cols) > 0) {
    df[numeric_cols] <- lapply(df[numeric_cols], function(x) {
      x[!is.finite(x)] <- NA_real_
      x
    })
  }

  list_cols <- names(df)[purrr::map_lgl(df, is.list)]
  if (length(list_cols) > 0) {
    df[list_cols] <- lapply(df[list_cols], function(x) {
      purrr::map_chr(x, function(z) {
        if (is.null(z) || length(z) == 0) {
          NA_character_
        } else {
          jsonlite::toJSON(z, auto_unbox = TRUE, null = "null")
        }
      })
    })
  }

  df
}

sql_type_for_vector <- function(x) {
  if (is.logical(x)) return("BOOLEAN")
  if (is.integer(x)) return("INTEGER")
  if (is.numeric(x)) return("DOUBLE PRECISION")
  "TEXT"
}

ensure_missing_columns <- function(con, table_id, data) {
  if (!DBI::dbExistsTable(con, table_id)) {
    message("Creating table: ", as.character(DBI::dbQuoteIdentifier(con, table_id)))
    DBI::dbWriteTable(con, table_id, data[0, ], row.names = FALSE)
    return(invisible(TRUE))
  }

  existing_cols <- DBI::dbListFields(con, table_id)
  missing_cols <- setdiff(names(data), existing_cols)

  if (length(missing_cols) == 0) {
    message("No missing columns to add.")
    return(invisible(TRUE))
  }

  for (col in missing_cols) {
    sql_type <- sql_type_for_vector(data[[col]])

    sql <- sprintf(
      "ALTER TABLE %s ADD COLUMN %s %s;",
      as.character(DBI::dbQuoteIdentifier(con, table_id)),
      qid(col),
      sql_type
    )

    message("Adding missing column: ", col, " ", sql_type)
    DBI::dbExecute(con, sql)
  }

  invisible(TRUE)
}

delete_existing_signal_rows <- function(keys_df) {
  fields <- if (DBI::dbExistsTable(con, target_tbl)) {
    DBI::dbListFields(con, target_tbl)
  } else {
    character()
  }

  target_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, target_tbl))

  # Prefer signal_key. Fallback to signal_id for old raw table uploads.
  if ("signal_key" %in% fields && "signal_key" %in% names(keys_df)) {
    keys <- unique(na.omit(as.character(keys_df$signal_key)))
    key_col <- "signal_key"
  } else {
    keys <- unique(na.omit(as.character(keys_df$signal_id)))
    key_col <- "signal_id"
  }

  keys <- keys[nzchar(keys)]

  if (length(keys) == 0) return(invisible(0))

  total_deleted <- 0
  chunks <- split(keys, ceiling(seq_along(keys) / 500))

  for (ids in chunks) {
    ids_sql <- paste(DBI::dbQuoteString(con, ids), collapse = ",")

    sql <- paste0(
      "DELETE FROM ", target_tbl_q,
      " WHERE grid_run_label = ", DBI::dbQuoteString(con, EXIT_GRID_RUN_LABEL),
      " AND ", DBI::dbQuoteIdentifier(con, key_col), " IN (", ids_sql, ")"
    )

    deleted <- DBI::dbExecute(con, sql)
    total_deleted <- total_deleted + deleted
  }

  invisible(total_deleted)
}

###############################################################################
# 13 · Prepare upload and delete old rows --------------------------------------
###############################################################################

grid_results_upload <- grid_results %>%
  mutate(
    grid_run_label = EXIT_GRID_RUN_LABEL,
    uploaded_at_utc = as.character(lubridate::with_tz(Sys.time(), "UTC")),
    .before = 1
  )

grid_results_upload <- clean_for_supabase(grid_results_upload)

target_tbl <- DBI::Id(schema = "public", table = EXIT_GRID_TARGET_TABLE)

ensure_missing_columns(con, target_tbl, grid_results_upload)

if (EXIT_GRID_FORCE_REBUILD) {
  delete_sql <- paste0(
    "DELETE FROM ",
    DBI::dbQuoteIdentifier(con, target_tbl),
    " WHERE grid_run_label = $1"
  )

  deleted <- DBI::dbExecute(con, delete_sql, params = list(EXIT_GRID_RUN_LABEL))
  message("Force rebuild deleted existing rows: ", deleted)
} else {
  deleted <- delete_existing_signal_rows(signals_run)
  message("Deleted existing rows for selected signals: ", deleted)
}

###############################################################################
# 14 · Upload chunks -----------------------------------------------------------
###############################################################################

idx <- seq_len(nrow(grid_results_upload))
chunks <- split(idx, ceiling(idx / EXIT_GRID_UPLOAD_CHUNK_SIZE))

message(
  "Uploading ",
  nrow(grid_results_upload),
  " rows to public.",
  EXIT_GRID_TARGET_TABLE,
  " in ",
  length(chunks),
  " chunks."
)

for (j in seq_along(chunks)) {
  chunk_df <- grid_results_upload[chunks[[j]], , drop = FALSE]

  DBI::dbWriteTable(
    con,
    target_tbl,
    chunk_df,
    append = TRUE,
    row.names = FALSE
  )

  message("Uploaded chunk ", j, "/", length(chunks), " rows: ", nrow(chunk_df))
}

###############################################################################
# 15 · Indexes and verification ------------------------------------------------
###############################################################################

create_index_safe <- function(table_name, index_name, cols) {
  sql <- paste0(
    "CREATE INDEX IF NOT EXISTS ",
    DBI::dbQuoteIdentifier(con, index_name),
    " ON ",
    DBI::dbQuoteIdentifier(con, "public"), ".",
    DBI::dbQuoteIdentifier(con, table_name),
    " (",
    paste(DBI::dbQuoteIdentifier(con, cols), collapse = ", "),
    ")"
  )

  tryCatch(
    DBI::dbExecute(con, sql),
    error = function(e) message("Index skipped: ", e$message)
  )
}

create_index_safe(
  EXIT_GRID_TARGET_TABLE,
  "idx_exit_grid_raw_run_combo",
  c("grid_run_label", "combo_id")
)

create_index_safe(
  EXIT_GRID_TARGET_TABLE,
  "idx_exit_grid_raw_run_entry",
  c("grid_run_label", "entry_date")
)

create_index_safe(
  EXIT_GRID_TARGET_TABLE,
  "idx_exit_grid_raw_run_signal",
  c("grid_run_label", "signal_id")
)

create_index_safe(
  EXIT_GRID_TARGET_TABLE,
  "idx_exit_grid_raw_run_ticker",
  c("grid_run_label", "ticker")
)

try({
  if ("signal_key" %in% DBI::dbListFields(con, target_tbl)) {
    create_index_safe(
      EXIT_GRID_TARGET_TABLE,
      "idx_exit_grid_raw_run_signal_key",
      c("grid_run_label", "signal_key")
    )
  }
}, silent = TRUE)

verify_sql <- paste0(
  "SELECT ",
  "COUNT(*) AS n_rows, ",
  "COUNT(DISTINCT signal_id) AS unique_signals, ",
  "COUNT(DISTINCT combo_id) AS unique_combos, ",
  "MIN(entry_date) AS first_entry, ",
  "MAX(entry_date) AS last_entry ",
  "FROM ",
  DBI::dbQuoteIdentifier(con, target_tbl),
  " WHERE grid_run_label = $1"
)

verify <- DBI::dbGetQuery(con, verify_sql, params = list(EXIT_GRID_RUN_LABEL))

print(verify)

label_sql <- paste0(
  "SELECT grid_run_label, COUNT(*) AS n_rows ",
  "FROM ",
  DBI::dbQuoteIdentifier(con, target_tbl),
  " GROUP BY grid_run_label ",
  "ORDER BY n_rows DESC"
)

cat("Run labels currently in table:\n")
print(DBI::dbGetQuery(con, label_sql))

message("Raw exit grid GitHub refresh finished.")
