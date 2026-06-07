#!/usr/bin/env Rscript

###############################################################################
# refresh_backtest_results_gap_aware.R
#
# Production refresh for the feature-table based GAP-AWARE backtest.
#
# Pipeline intended:
#   twitter_ingest.R
#   generate_tweet_signals.R
#   refresh_trade_signal_features.R
#   refresh_backtest_results_gap_aware.R
#
# Main changes vs the older backtest:
#   1) Reads from public.twitter_trade_signal_features, not raw signals.
#   2) Uses precomputed entry_date, entry_at, sma10_prior, prev_dist_sma10,
#      ret_*_prior, volatility_20d, etc.
#   3) Exit logic is gap-aware:
#        - gap stop at open
#        - gap target at open
#        - intraday stop
#        - intraday target
#      in that priority order on the first event day.
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
  library(glue)
})

###############################################################################
# 0 · Controls ----------------------------------------------------------------
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
  if (tolower(val) %in% c("inf", "infinite", "all")) return(Inf)
  out <- suppressWarnings(as.numeric(val))
  if (is.na(out)) default else out
}

get_default_as_of_date <- function() {
  as.Date(lubridate::with_tz(Sys.time(), "America/New_York"))
}

STRATEGY_LABEL <- Sys.getenv("STRATEGY_LABEL", "tp10_sl5")

# This file uses the feature table as the source.
FEATURES_TABLE <- Sys.getenv("FEATURES_TABLE", "twitter_trade_signal_features")

TARGET_TABLE_NAME <- Sys.getenv(
  "GAPAWARE_BACKTEST_TABLE",
  "twitter_backtest_results_10_perc_strategy_gapaware"
)

TARGET_PCT <- env_num("TARGET_PCT", 0.10)
STOP_PCT <- env_num("STOP_PCT", 0.05)

LOOKBACK_DAYS <- env_num("LOOKBACK_DAYS", 30)
LOCKUP_DAYS <- env_num("LOCKUP_DAYS", 30)
SMA_LEN <- env_num("SMA_LEN", 10)

CACHE_DIR <- Sys.getenv("CACHE_DIR", "cache_px_tp10_sl5")

DEBUG_YAHOO <- env_bool("DEBUG_YAHOO", FALSE)
FORCE_REBUILD <- env_bool("FORCE_REBUILD", FALSE)

# Only new signals from this many days back are processed.
# Existing open/uncompleted trades are refreshed regardless of age.
NEW_SIGNAL_LOOKBACK_DAYS <- env_num("NEW_SIGNAL_LOOKBACK_DAYS", 31)
MAX_ROWS_LOCAL <- env_int_or_inf("MAX_ROWS_LOCAL", Inf)

AS_OF_DATE <- Sys.getenv("AS_OF_DATE", unset = "")
AS_OF_DATE <- if (nzchar(AS_OF_DATE)) {
  as.Date(AS_OF_DATE)
} else {
  get_default_as_of_date()
}

if (is.na(AS_OF_DATE)) {
  stop("AS_OF_DATE could not be parsed. Use format YYYY-MM-DD.")
}

NEW_SIGNAL_CUTOFF <- AS_OF_DATE - lubridate::days(NEW_SIGNAL_LOOKBACK_DAYS)

cat("RUNNING FILE: refresh_backtest_results_gap_aware.R | FEATURE-TABLE GAP-AWARE VERSION\n")
cat("Strategy label:", STRATEGY_LABEL, "\n")
cat("Feature source table: public.", FEATURES_TABLE, "\n", sep = "")
cat("Target table: public.", TARGET_TABLE_NAME, "\n", sep = "")
cat("Target pct:", TARGET_PCT, "\n")
cat("Stop pct:", STOP_PCT, "\n")
cat("Local TZ: America/New_York\n")
cat("As-of date:", as.character(AS_OF_DATE), "\n")
cat("New signal cutoff:", as.character(NEW_SIGNAL_CUTOFF), "\n")
cat("New signal lookback days:", NEW_SIGNAL_LOOKBACK_DAYS, "\n")
cat("Cache dir:", CACHE_DIR, "\n")
cat("Debug Yahoo:", DEBUG_YAHOO, "\n")
cat("Force rebuild:", FORCE_REBUILD, "\n")

###############################################################################
# 1 · Supabase connection ------------------------------------------------------
###############################################################################

get_env_first <- function(...) {
  names <- c(...)
  for (nm in names) {
    val <- Sys.getenv(nm, unset = "")
    if (nzchar(val)) return(val)
  }
  ""
}

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
        "SUPABASE_USER, and SUPABASE_PWD."
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
# 2 · Helpers -----------------------------------------------------------------
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

sql_type_for_vector <- function(x) {
  if (inherits(x, "POSIXct") || inherits(x, "POSIXt")) return("TIMESTAMPTZ")
  if (inherits(x, "Date")) return("DATE")
  if (is.logical(x)) return("BOOLEAN")
  if (is.integer(x)) return("INTEGER")
  if (is.numeric(x)) return("DOUBLE PRECISION")
  "TEXT"
}

ensure_missing_columns <- function(con, target_tbl, data) {
  target_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, target_tbl))
  existing_cols <- DBI::dbListFields(con, target_tbl)
  missing_cols <- setdiff(names(data), existing_cols)

  if (length(missing_cols) == 0) {
    message("No missing columns to add to ", TARGET_TABLE_NAME, ".")
    return(invisible(TRUE))
  }

  for (col in missing_cols) {
    sql_type <- sql_type_for_vector(data[[col]])
    sql <- sprintf(
      "ALTER TABLE %s ADD COLUMN %s %s;",
      target_tbl_q,
      qid(col),
      sql_type
    )
    message("Adding missing column: ", col, " ", sql_type)
    DBI::dbExecute(con, sql)
  }

  invisible(TRUE)
}

safe_index_name <- function(...) {
  x <- paste(..., sep = "_")
  x <- gsub("[^A-Za-z0-9_]", "_", x)
  substr(x, 1, 60)
}

###############################################################################
# 3 · Yahoo fetch + cache -------------------------------------------------------
###############################################################################

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
    filter(
      !is.na(date),
      date >= from,
      date <= to
    ) %>%
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
    filter(
      !is.na(date),
      date >= need_from,
      date <= need_to
    ) %>%
    arrange(date)
}

###############################################################################
# 4 · Smoke test ---------------------------------------------------------------
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
# 5 · Read and prepare feature-table signals ----------------------------------
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

    horizon = if ("horizon" %in% names(.)) str_to_lower(str_trim(as.character(horizon))) else NA_character_,
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
  cat("Latest signal date:", as.character(max(signals$signal_date, na.rm = TRUE)), "\n")
  print(signals %>% summarise(
    rows = n(),
    unique_signal_ids = n_distinct(signal_id, na.rm = TRUE),
    min_signal_date = safe_min_date(signal_date),
    max_signal_date = safe_max_date(signal_date),
    rows_with_entry_date = sum(!is.na(entry_date)),
    rows_with_volatility_20d = sum(!is.na(volatility_20d)),
    rows_with_sma10_prior = sum(!is.na(sma10_prior))
  ))
}

###############################################################################
# 6 · Select rows to process ---------------------------------------------------
###############################################################################

target_tbl <- DBI::Id(schema = "public", table = TARGET_TABLE_NAME)
target_exists <- DBI::dbExistsTable(con, target_tbl)

get_existing_backtest <- function() {
  if (!target_exists) {
    return(
      tibble(
        signal_id = character(),
        username = character(),
        conversation_id = character(),
        ticker = character(),
        tweet_ts_et = as.POSIXct(character()),
        trade_id = character(),
        completed = logical(),
        trade_status = character(),
        signal_key = character()
      )
    )
  }

  target_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, target_tbl))
  existing_fields <- DBI::dbListFields(con, target_tbl)

  field_expr <- function(col, default_sql, alias = col) {
    if (col %in% existing_fields) {
      sprintf("%s AS %s", qid(col), qid(alias))
    } else {
      sprintf("%s AS %s", default_sql, qid(alias))
    }
  }

  where_strategy <- if ("strategy_label" %in% existing_fields) {
    sprintf(
      "WHERE strategy_label = %s",
      as.character(DBI::dbQuoteString(con, STRATEGY_LABEL))
    )
  } else {
    ""
  }

  sql <- sprintf(
    "
    SELECT
      %s,
      %s,
      %s,
      %s,
      %s,
      %s,
      %s,
      %s
    FROM %s
    %s;
    ",
    field_expr("signal_id", "NULL::text"),
    field_expr("username", "NULL::text"),
    field_expr("conversation_id", "NULL::text"),
    field_expr("ticker", "NULL::text"),
    field_expr("tweet_ts_et", "NULL::timestamptz"),
    field_expr("trade_id", "NULL::text"),
    field_expr("completed", "TRUE"),
    field_expr("trade_status", "'completed'::text"),
    target_tbl_q,
    where_strategy
  )

  DBI::dbGetQuery(con, sql) %>%
    as_tibble() %>%
    mutate(
      signal_id = as.character(signal_id),
      username = as.character(username),
      conversation_id = as.character(conversation_id),
      ticker = str_to_upper(as.character(ticker)),
      tweet_ts_et = as.POSIXct(tweet_ts_et, tz = "America/New_York"),
      trade_id = as.character(trade_id),
      completed = as.logical(completed),
      trade_status = tolower(as.character(trade_status)),
      signal_key = build_signal_key(
        signal_id = signal_id,
        username = username,
        conversation_id = conversation_id,
        ticker = ticker,
        tweet_ts_et = tweet_ts_et
      )
    )
}

existing_backtest <- get_existing_backtest()
cat("Existing backtest rows:", nrow(existing_backtest), "\n")

existing_keys <- existing_backtest %>%
  filter(!is.na(signal_key), nzchar(signal_key)) %>%
  distinct(signal_key)

open_keys <- existing_backtest %>%
  filter(
    is.na(completed) |
      completed != TRUE |
      is.na(trade_status) |
      trade_status != "completed"
  ) %>%
  filter(!is.na(signal_key), nzchar(signal_key)) %>%
  distinct(signal_key)

if (FORCE_REBUILD) {
  signals_run <- signals
  cat("FORCE_REBUILD is TRUE. All valid feature-table signals will be processed.\n")
} else {
  new_recent_signals <- signals %>%
    filter(
      signal_date >= NEW_SIGNAL_CUTOFF,
      signal_date <= AS_OF_DATE
    ) %>%
    anti_join(existing_keys, by = "signal_key")

  open_signals <- signals %>%
    semi_join(open_keys, by = "signal_key")

  signals_run <- bind_rows(
    new_recent_signals,
    open_signals
  ) %>%
    distinct(signal_key, .keep_all = TRUE)

  cat("New recent feature-table signals to process:", nrow(new_recent_signals), "\n")
  cat("Open existing signals to refresh:", nrow(open_signals), "\n")
}

if (is.finite(MAX_ROWS_LOCAL)) {
  signals_run <- signals_run %>% slice_head(n = MAX_ROWS_LOCAL)
}

cat("Total rows to process:", nrow(signals_run), "\n")

if (nrow(signals_run) == 0) {
  cat("No rows to process. Exiting.\n")
  quit(save = "no", status = 0)
}

###############################################################################
# 7 · Gap-aware strategy logic -------------------------------------------------
###############################################################################

simulate_trade <- function(row,
                           px,
                           lookback = LOOKBACK_DAYS,
                           lockup = LOCKUP_DAYS,
                           sma_len = SMA_LEN,
                           target_pct = TARGET_PCT,
                           stop_pct = STOP_PCT,
                           as_of_date = AS_OF_DATE) {

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
      open = as.numeric(open),
      high = as.numeric(high),
      low = as.numeric(low),
      close = zoo::na.locf(close, na.rm = FALSE),
      open = zoo::na.locf(open, na.rm = FALSE),
      high = zoo::na.locf(high, na.rm = FALSE),
      low = zoo::na.locf(low, na.rm = FALSE),
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

  entry_sma <- suppressWarnings(as.numeric(row$sma10_prior[[1]]))

  entry_below_sma10 <- if (!is.na(entry_sma)) {
    entry_price < entry_sma
  } else {
    NA
  }

  sma10_prior <- suppressWarnings(as.numeric(row$sma10_prior[[1]]))
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
        gap_stop_hit ~ "gap_stop",
        gap_take_hit ~ "gap_target",
        intraday_stop_hit ~ "intraday_stop",
        intraday_take_hit ~ "intraday_target",
        TRUE ~ NA_character_
      ),

      event_price = case_when(
        event_type == "gap_stop" ~ open,
        event_type == "gap_target" ~ open,
        event_type == "intraday_stop" ~ stop_px,
        event_type == "intraday_target" ~ tgt_px,
        TRUE ~ NA_real_
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

      if (nrow(day_row) == 0) day_row <- dplyr::last(px_full)

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
              pre_reclaim_gap_stop_hit ~ "pre_reclaim_gap_stop",
              pre_reclaim_intraday_stop_hit ~ "pre_reclaim_intraday_stop",
              TRUE ~ NA_character_
            ),

            pre_reclaim_stop_price = case_when(
              pre_reclaim_gap_stop_hit ~ open,
              pre_reclaim_intraday_stop_hit ~ stop_px,
              TRUE ~ NA_real_
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
              pre_reclaim_gap_stop_hit ~ "pre_reclaim_gap_stop",
              pre_reclaim_intraday_stop_hit ~ "pre_reclaim_intraday_stop",
              TRUE ~ NA_character_
            ),

            pre_reclaim_stop_price = case_when(
              pre_reclaim_gap_stop_hit ~ open,
              pre_reclaim_intraday_stop_hit ~ stop_px,
              TRUE ~ NA_real_
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
    total_return = final_return
  )
}

simulate_trade_full <- function(row,
                                lookback = LOOKBACK_DAYS,
                                lockup = LOCKUP_DAYS,
                                sma_len = SMA_LEN,
                                target_pct = TARGET_PCT,
                                stop_pct = STOP_PCT,
                                cache_dir = CACHE_DIR,
                                as_of_date = AS_OF_DATE) {

  px_anchor <- suppressWarnings(min(
    as.Date(row$feature_date[[1]]),
    as.Date(row$entry_date[[1]]),
    as.Date(row$tweet_ts_et[[1]]),
    na.rm = TRUE
  ))

  if (is.infinite(px_anchor) || is.na(px_anchor)) {
    px_anchor <- as.Date(row$entry_date[[1]])
  }

  px_from <- as.Date(px_anchor) - days(250)
  px_to <- as.Date(as_of_date)

  px <- get_px_cached(
    sym = row$ticker[[1]],
    from = px_from,
    to = px_to,
    cache_dir = cache_dir
  )

  out <- simulate_trade(
    row = row,
    px = px,
    lookback = lookback,
    lockup = lockup,
    sma_len = sma_len,
    target_pct = target_pct,
    stop_pct = stop_pct,
    as_of_date = as_of_date
  )

  if (nrow(out) == 0) return(tibble())

  bind_cols(row, out)
}

###############################################################################
# 8 · SPY market regime --------------------------------------------------------
###############################################################################

add_spy_regime <- function(results_tbl,
                           cache_dir = CACHE_DIR,
                           spy_symbol = "SPY",
                           spy_from = as.Date("2009-01-01"),
                           use_previous_day = TRUE) {
  if (!is.data.frame(results_tbl) || nrow(results_tbl) == 0) return(results_tbl)

  results_tbl <- results_tbl %>%
    mutate(
      tweet_ts_et = as.POSIXct(tweet_ts_et, tz = "America/New_York"),
      entry_date = as.Date(entry_date),
      px_start = as.Date(px_start),
      px_end = as.Date(px_end),
      first_exit_date = as.Date(first_exit_date),
      final_exit_date = as.Date(final_exit_date),
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

  spy_to <- safe_max_date(results_tbl$data_through_date)

  if (is.na(spy_to)) return(results_tbl)

  spy_px <- get_px_cached(
    sym = spy_symbol,
    from = spy_from,
    to = spy_to,
    cache_dir = cache_dir,
    fetch_fn = get_px_via_syscurl_yahoo
  )

  if (nrow(spy_px) == 0 || !"date" %in% names(spy_px)) {
    message("No SPY price data available; returning results without regime columns.")
    return(results_tbl)
  }

  spy_regime <- spy_px %>%
    arrange(date) %>%
    mutate(
      spy_regime_date = as.Date(date),
      spy_close = as.numeric(close),
      spy_sma50 = TTR::SMA(spy_close, n = 50),
      spy_sma200 = TTR::SMA(spy_close, n = 200),
      market_regime_50 = case_when(
        is.na(spy_sma50) ~ NA_character_,
        spy_close >= spy_sma50 ~ "above_sma50",
        spy_close < spy_sma50 ~ "below_sma50",
        TRUE ~ NA_character_
      ),
      market_regime_200 = case_when(
        is.na(spy_sma200) ~ NA_character_,
        spy_close >= spy_sma200 ~ "above_sma200",
        spy_close < spy_sma200 ~ "below_sma200",
        TRUE ~ NA_character_
      )
    ) %>%
    select(
      spy_regime_date,
      spy_close,
      spy_sma50,
      spy_sma200,
      market_regime_50,
      market_regime_200
    )

  join_tbl <- results_tbl %>%
    mutate(
      spy_join_date = if (isTRUE(use_previous_day)) entry_date - days(1) else entry_date
    )

  joined <- join_tbl %>%
    left_join(spy_regime, by = c("spy_join_date" = "spy_regime_date")) %>%
    rename(spy_regime_date = spy_join_date)

  joined
}

###############################################################################
# 9 · Run backtest -------------------------------------------------------------
###############################################################################

cat("Running gap-aware backtest rows...\n")

results <- purrr::map_dfr(seq_len(nrow(signals_run)), function(i) {
  row <- signals_run[i, ]

  cat(
    sprintf(
      "[%s/%s] %s %s %s entry=%s %s\n",
      i,
      nrow(signals_run),
      row$ticker[[1]],
      row$side[[1]],
      as.character(row$signal_date[[1]]),
      as.character(row$entry_date[[1]]),
      row$entry_at[[1]]
    )
  )

  tryCatch(
    simulate_trade_full(row = row),
    error = function(e) {
      message("Backtest failed for row ", i, " ticker ", row$ticker[[1]], ": ", e$message)
      tibble()
    }
  )
})

cat("Backtest result rows:", nrow(results), "\n")

if (nrow(results) == 0) {
  warning("No backtest rows produced. Exiting without upload.")
  quit(save = "no", status = 0)
}

results <- add_spy_regime(results)

###############################################################################
# 10 · Prepare upload ----------------------------------------------------------
###############################################################################

upload_started_at <- Sys.time()

results_upload <- results %>%
  mutate(
    strategy_label = STRATEGY_LABEL,
    loaded_at = upload_started_at,
    target_pct = TARGET_PCT,
    stop_pct = STOP_PCT
  )

# Rename a generic date column if any slipped in from joins.
if ("date" %in% names(results_upload)) {
  results_upload <- results_upload %>%
    mutate(extra_date = as.Date(date)) %>%
    select(-date)
}

date_cols <- intersect(
  c(
    "signal_date",
    "entry_date",
    "feature_date",
    "px_start",
    "px_end",
    "first_exit_date",
    "final_exit_date",
    "data_through_date",
    "spy_regime_date"
  ),
  names(results_upload)
)

posix_cols <- intersect(
  c(
    "created_at",
    "created_at_utc",
    "tweet_ts_et",
    "updated_at",
    "loaded_at"
  ),
  names(results_upload)
)

results_upload <- results_upload %>%
  mutate(
    across(all_of(date_cols), as.Date),
    across(all_of(posix_cols), ~ as.POSIXct(.x, tz = "UTC"))
  )

char_cols <- intersect(
  c(
    "signal_id",
    "ticker",
    "ticker_raw",
    "side",
    "direction",
    "horizon",
    "rationale",
    "username",
    "conversation_id",
    "evidence_types",
    "evidence_snippet",
    "entry_at",
    "first_exit_type",
    "final_exit_type",
    "exit_reason",
    "trade_status",
    "completion_reason",
    "market_regime_50",
    "market_regime_200",
    "strategy_label",
    "signal_key"
  ),
  names(results_upload)
)

results_upload <- results_upload %>%
  mutate(across(all_of(char_cols), as.character))

# Stable strategy-specific trade_id.
results_upload <- results_upload %>%
  mutate(
    trade_id_base = ifelse(
      !is.na(signal_id) & nzchar(as.character(signal_id)),
      paste(strategy_label, "signal_id", signal_id, sep = "__"),
      paste(
        strategy_label,
        username,
        conversation_id,
        ticker,
        make_ts_key(tweet_ts_et),
        entry_at,
        sep = "__"
      )
    )
  ) %>%
  group_by(trade_id_base) %>%
  mutate(
    trade_id = if (dplyr::n() == 1L) {
      trade_id_base
    } else {
      paste0(trade_id_base, "__", dplyr::row_number())
    }
  ) %>%
  ungroup() %>%
  select(-trade_id_base) %>%
  relocate(trade_id, strategy_label, .before = ticker) %>%
  distinct(trade_id, .keep_all = TRUE)

cat("Upload rows:", nrow(results_upload), "\n")
print(results_upload %>% summarise(
  n_rows = n(),
  n_distinct_trade_ids = n_distinct(trade_id),
  first_entry_date = safe_min_date(entry_date),
  last_entry_date = safe_max_date(entry_date),
  n_completed = sum(completed == TRUE, na.rm = TRUE),
  n_open = sum(is.na(completed) | completed != TRUE, na.rm = TRUE),
  n_gap_stop = sum(exit_reason == "gap_stop", na.rm = TRUE),
  n_gap_target = sum(exit_reason == "gap_target", na.rm = TRUE)
))

###############################################################################
# 11 · Upload / upsert ---------------------------------------------------------
###############################################################################

target_tbl <- DBI::Id(schema = "public", table = TARGET_TABLE_NAME)
target_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, target_tbl))

if (!DBI::dbExistsTable(con, target_tbl)) {
  message("Creating new table: public.", TARGET_TABLE_NAME)

  DBI::dbWriteTable(
    con,
    target_tbl,
    results_upload,
    overwrite = FALSE,
    row.names = FALSE
  )

} else {
  message("Table already exists. Upserting into: public.", TARGET_TABLE_NAME)

  ensure_missing_columns(con, target_tbl, results_upload)
}

idx_base <- safe_index_name(TARGET_TABLE_NAME)

DBI::dbExecute(
  con,
  sprintf(
    "CREATE UNIQUE INDEX IF NOT EXISTS %s ON public.%s(trade_id);",
    qid(paste0("idx_", idx_base, "_trade_id")),
    TARGET_TABLE_NAME
  )
)

DBI::dbExecute(
  con,
  sprintf(
    "CREATE INDEX IF NOT EXISTS %s ON public.%s(entry_date);",
    qid(paste0("idx_", idx_base, "_entry_date")),
    TARGET_TABLE_NAME
  )
)

DBI::dbExecute(
  con,
  sprintf(
    "CREATE INDEX IF NOT EXISTS %s ON public.%s(ticker);",
    qid(paste0("idx_", idx_base, "_ticker")),
    TARGET_TABLE_NAME
  )
)

DBI::dbExecute(
  con,
  sprintf(
    "CREATE INDEX IF NOT EXISTS %s ON public.%s(username);",
    qid(paste0("idx_", idx_base, "_username")),
    TARGET_TABLE_NAME
  )
)

# Re-check fields after possible table creation / ALTER.
existing_cols_after <- DBI::dbListFields(con, target_tbl)
upload_cols <- intersect(names(results_upload), existing_cols_after)
results_upload <- results_upload %>% select(all_of(upload_cols))

# Use temporary table for upsert.
temp_name <- paste0("tmp_", TARGET_TABLE_NAME, "_", format(Sys.time(), "%Y%m%d%H%M%S"))
temp_tbl <- DBI::Id(table = temp_name)
temp_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, temp_tbl))

DBI::dbWriteTable(
  con,
  temp_tbl,
  results_upload,
  temporary = TRUE,
  overwrite = TRUE,
  row.names = FALSE
)

cols <- names(results_upload)
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

DBI::dbExecute(con, upsert_sql)

###############################################################################
# 12 · Quick checks ------------------------------------------------------------
###############################################################################

cat("Upload/upsert complete. Quick checks:\n")

print(DBI::dbGetQuery(
  con,
  sprintf(
    "SELECT COUNT(*) AS n_rows FROM public.%s;",
    TARGET_TABLE_NAME
  )
))

print(DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT COUNT(*) AS n_duplicate_trade_ids
    FROM (
      SELECT trade_id
      FROM public.%s
      GROUP BY trade_id
      HAVING COUNT(*) > 1
    ) d;
    ",
    TARGET_TABLE_NAME
  )
))

print(DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT
      MIN(entry_date) AS first_entry_date,
      MAX(entry_date) AS last_entry_date,
      COUNT(*) AS n_rows,
      SUM(CASE WHEN completed IS TRUE THEN 1 ELSE 0 END) AS n_completed,
      SUM(CASE WHEN completed IS NOT TRUE THEN 1 ELSE 0 END) AS n_open
    FROM public.%s;
    ",
    TARGET_TABLE_NAME
  )
))

print(DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT
      strategy_label,
      target_pct,
      stop_pct,
      COUNT(*) AS n_rows
    FROM public.%s
    GROUP BY strategy_label, target_pct, stop_pct
    ORDER BY n_rows DESC;
    ",
    TARGET_TABLE_NAME
  )
))

print(DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT
      trade_status,
      completed,
      COUNT(*) AS n_rows
    FROM public.%s
    GROUP BY trade_status, completed
    ORDER BY n_rows DESC;
    ",
    TARGET_TABLE_NAME
  )
))

cat("Done.\n")
