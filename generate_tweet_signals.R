###############################################################################
# refresh_backtest_results.R
# Refresh Twitter signal backtest results and upload/upsert to Supabase
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

SIGNALS_TABLE <- Sys.getenv("SIGNALS_TABLE", "twitter_trade_signals")

TARGET_TABLE_NAME <- Sys.getenv(
  "BACKTEST_TABLE",
  "twitter_backtest_results_10_perc_strategy"
)

TARGET_PCT <- env_num("TARGET_PCT", 0.10)
STOP_PCT <- env_num("STOP_PCT", 0.05)

LOOKBACK_DAYS <- env_num("LOOKBACK_DAYS", 30)
LOCKUP_DAYS <- env_num("LOCKUP_DAYS", 30)
SMA_LEN <- env_num("SMA_LEN", 10)

CACHE_DIR <- Sys.getenv("CACHE_DIR", "cache_px_tp10_sl5")

DEBUG_YAHOO <- env_bool("DEBUG_YAHOO", FALSE)

# Normally false. If true, reruns all valid signals.
FORCE_REBUILD <- env_bool("FORCE_REBUILD", FALSE)

# Only new signals from this many days back are processed.
# Open/uncompleted trades are refreshed regardless of age.
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

cat("RUNNING FILE: refresh_backtest_results.R | PRODUCTION REFRESH VERSION\n")
cat("Strategy label:", STRATEGY_LABEL, "\n")
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
  
  # Do not convert all dots to hyphens.
  # Exchange suffixes like BRBY.L should stay as BRBY.L.
  dplyr::case_when(
    sym == "BRK.B" ~ "BRK-B",
    sym == "BF.B" ~ "BF-B",
    TRUE ~ sym
  )
}

date_to_unix <- function(x, end_of_day = FALSE) {
  x <- as.POSIXct(as.Date(x), tz = "UTC")
  if (end_of_day) x <- x + 86399
  as.integer(x)
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

###############################################################################
# 3 · Yahoo fetch + cache -------------------------------------------------------
###############################################################################

fetch_yahoo_chart_syscurl <- function(sym, from, to, dest_json, retries = 4, debug = DEBUG_YAHOO) {
  if (is.na(from) || is.na(to) || !nzchar(sym)) return(FALSE)
  
  yahoo_sym <- normalize_symbol_for_yahoo(sym)
  
  period1 <- date_to_unix(from, end_of_day = FALSE)
  period2 <- date_to_unix(to, end_of_day = TRUE)
  
  url <- paste0(
    "https://query2.finance.yahoo.com/v8/finance/chart/", yahoo_sym,
    "?period1=", period1,
    "&period2=", period2,
    "&interval=1d&includeAdjustedClose=true"
  )
  
  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) stop("curl not found on system")
  
  args <- c(
    "--http1.1",
    "--ipv4",
    "-sS",
    "-L",
    "--compressed",
    "--user-agent", "Mozilla/5.0",
    "--header", "Accept: application/json",
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
      cat("Yahoo symbol:", yahoo_sym, "\n")
      cat("URL:", url, "\n")
      cat("period1:", period1, "=>", as.character(as.POSIXct(period1, origin = "1970-01-01", tz = "UTC")), "\n")
      cat("period2:", period2, "=>", as.character(as.POSIXct(period2, origin = "1970-01-01", tz = "UTC")), "\n")
      cat("period2 > period1:", period2 > period1, "\n")
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
      
      if (debug) {
        cat("Yahoo response first 300 chars:\n")
        cat(substr(txt, 1, 300), "\n")
      }
      
      if (
        grepl('"chart"', txt, fixed = TRUE) &&
        !grepl("Too Many Requests", txt, fixed = TRUE)
      ) {
        return(TRUE)
      }
      
      if (grepl("Too Many Requests", txt, fixed = TRUE)) {
        Sys.sleep(min(2^k, 10))
        next
      }
    }
    
    Sys.sleep(min(2^k, 10))
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
  
  if (is.null(js) || is.null(js$chart) || !is.null(js$chart$error)) {
    return(tibble())
  }
  
  res <- js$chart$result
  if (is.null(res) || length(res) == 0) return(tibble())
  
  res1 <- res[[1]]
  timestamps <- res1$timestamp
  quote_obj <- res1$indicators$quote[[1]]
  
  if (is.null(timestamps) || is.null(quote_obj)) return(tibble())
  
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
  
  if (min_len == 0) return(tibble())
  
  tibble(
    date = as.Date(as.POSIXct(ts_vec[seq_len(min_len)], origin = "1970-01-01", tz = "UTC")),
    open = suppressWarnings(as.numeric(open_vec[seq_len(min_len)])),
    high = suppressWarnings(as.numeric(high_vec[seq_len(min_len)])),
    low = suppressWarnings(as.numeric(low_vec[seq_len(min_len)])),
    close = suppressWarnings(as.numeric(close_vec[seq_len(min_len)])),
    volume = suppressWarnings(as.numeric(vol_vec[seq_len(min_len)]))
  ) %>%
    filter(!is.na(date)) %>%
    arrange(date)
}

get_px_via_syscurl_yahoo <- function(sym, from, to, debug = DEBUG_YAHOO) {
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

get_px_cached <- function(sym, from, to, cache_dir = CACHE_DIR, fetch_fn = get_px_via_syscurl_yahoo) {
  if (is.na(from) || is.na(to) || !nzchar(sym)) return(tibble())
  
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  
  safe_sym <- gsub("[^A-Za-z0-9_.-]", "_", normalize_symbol_for_yahoo(sym))
  fname <- file.path(cache_dir, paste0(safe_sym, ".rds"))
  
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
    }
  }
  
  if (nrow(cached) == 0) return(tibble())
  
  cached %>%
    filter(date >= need_from, date <= need_to) %>%
    arrange(date)
}

###############################################################################
# 4 · Smoke test ---------------------------------------------------------------
###############################################################################

cat("Running price smoke test with AMZN...\n")

smoke_from <- AS_OF_DATE - days(20)
smoke_to <- AS_OF_DATE

cat("Price smoke test window:", as.character(smoke_from), "to", as.character(smoke_to), "\n")

smoke_px <- get_px_cached(
  sym = "AMZN",
  from = smoke_from,
  to = smoke_to,
  cache_dir = CACHE_DIR
)

cat("Yahoo smoke test AMZN rows:", nrow(smoke_px), "\n")

if (nrow(smoke_px) == 0) {
  stop("Yahoo smoke test failed: no AMZN rows returned.")
}

###############################################################################
# 5 · Read and prepare signals -------------------------------------------------
###############################################################################

signals_tbl_id <- DBI::Id(schema = "public", table = SIGNALS_TABLE)

if (!DBI::dbExistsTable(con, signals_tbl_id)) {
  stop("Signals table does not exist: public.", SIGNALS_TABLE)
}

signals_from_tweets <- DBI::dbReadTable(con, signals_tbl_id) %>%
  as_tibble()

cat("Signals table rows:", nrow(signals_from_tweets), "\n")

if (!"created_at" %in% names(signals_from_tweets)) {
  stop("Expected `created_at` column in signals table.")
}

if (!"direction" %in% names(signals_from_tweets)) {
  stop("Expected `direction` column in signals table.")
}

signals_src <- signals_from_tweets %>%
  mutate(
    row_id = row_number(),
    created_at = as.POSIXct(created_at, tz = "UTC"),
    tweet_ts_et = lubridate::with_tz(created_at, "America/New_York"),
    signal_date = as.Date(tweet_ts_et)
  )

signals <- signals_src %>%
  mutate(
    signal_id = if ("signal_id" %in% names(.)) as.character(signal_id) else NA_character_,
    ticker = str_to_upper(as.character(ticker)),
    side = str_to_lower(as.character(direction)),
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
    conversation_id = as.character(conversation_id),
    conviction = suppressWarnings(as.numeric(conviction)),
    buy_zone = as_logicalish(buy_zone),
    sentiment_score = suppressWarnings(as.numeric(sentiment_score)),
    evidence_score = if ("evidence_score" %in% names(.)) suppressWarnings(as.numeric(evidence_score)) else NA_real_,
    specificity_score = if ("specificity_score" %in% names(.)) suppressWarnings(as.numeric(specificity_score)) else NA_real_,
    has_quant_data = if ("has_quant_data" %in% names(.)) as_logicalish(has_quant_data) else NA
  ) %>%
  transmute(
    row_id,
    signal_id,
    ticker,
    tweet_ts_et,
    signal_date,
    side,
    horizon,
    conviction,
    buy_zone,
    sentiment_score,
    rationale = if ("rationale" %in% names(.)) as.character(rationale) else NA_character_,
    username = as.character(username),
    conversation_id,
    evidence_score,
    specificity_score,
    has_quant_data,
    evidence_types = if ("evidence_types" %in% names(.)) as.character(evidence_types) else NA_character_,
    evidence_snippet = if ("evidence_snippet" %in% names(.)) as.character(evidence_snippet) else NA_character_
  ) %>%
  filter(
    !is.na(tweet_ts_et),
    !is.na(ticker),
    ticker != "",
    side %in% c("long", "short")
  ) %>%
  mutate(
    signal_key = build_signal_key(
      signal_id = signal_id,
      username = username,
      conversation_id = conversation_id,
      ticker = ticker,
      tweet_ts_et = tweet_ts_et
    )
  )

cat("Prepared valid signals:", nrow(signals), "\n")
cat("Latest signal date:", as.character(max(signals$signal_date, na.rm = TRUE)), "\n")

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
  
  cat("FORCE_REBUILD is TRUE. All valid signals will be processed.\n")
  
} else {
  
  # New signals only if they happened within the last month/lookback window.
  new_recent_signals <- signals %>%
    filter(
      signal_date >= NEW_SIGNAL_CUTOFF,
      signal_date <= AS_OF_DATE
    ) %>%
    anti_join(existing_keys, by = "signal_key")
  
  # Existing open/uncompleted trades are refreshed regardless of age.
  open_signals <- signals %>%
    semi_join(open_keys, by = "signal_key")
  
  signals_run <- bind_rows(
    new_recent_signals,
    open_signals
  ) %>%
    distinct(signal_key, .keep_all = TRUE)
  
  cat("New recent signals to process:", nrow(new_recent_signals), "\n")
  cat("Open existing signals to refresh:", nrow(open_signals), "\n")
}

if (is.finite(MAX_ROWS_LOCAL)) {
  signals_run <- signals_run %>%
    slice_head(n = MAX_ROWS_LOCAL)
}

cat("Total rows to process:", nrow(signals_run), "\n")

if (nrow(signals_run) == 0) {
  cat("No rows to process. Exiting.\n")
  quit(save = "no", status = 0)
}

###############################################################################
# 7 · Strategy logic -----------------------------------------------------------
###############################################################################

pick_entry_from_px <- function(px, tweet_ts_et) {
  tweet_ts_et <- lubridate::with_tz(
    as.POSIXct(tweet_ts_et, tz = "America/New_York"),
    "America/New_York"
  )
  
  tweet_date <- as.Date(tweet_ts_et)
  tweet_time <- format(tweet_ts_et, "%H:%M:%S")
  
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
                           lookback = LOOKBACK_DAYS,
                           lockup = LOCKUP_DAYS,
                           sma_len = SMA_LEN,
                           target_pct = TARGET_PCT,
                           stop_pct = STOP_PCT,
                           as_of_date = AS_OF_DATE) {
  
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
  if (max(px_full$date, na.rm = TRUE) < as.Date(tweet_ts_et)) return(tibble())
  
  data_through_date <- max(px_full$date, na.rm = TRUE)
  
  entry_pick <- pick_entry_from_px(px_full, tweet_ts_et)
  entry_date <- entry_pick$entry_date
  entry_at <- entry_pick$entry_at
  
  if (is.na(entry_date)) return(tibble())
  
  entry_row <- px_full %>%
    filter(date == entry_date) %>%
    slice_head(n = 1)
  
  if (nrow(entry_row) == 0) return(tibble())
  
  entry_idx <- match(entry_date, px_full$date)
  
  rows_before_entry <- sum(px_full$date <= entry_date)
  non_na_closes_before_entry <- sum(!is.na(px_full$close[px_full$date <= entry_date]))
  
  entry_price <- if (entry_at == "open") entry_row$open[[1]] else entry_row$close[[1]]
  entry_sma <- entry_row$sma_val[[1]]
  
  if (is.na(entry_sma)) {
    last_n <- px_full %>%
      filter(date <= entry_date) %>%
      slice_tail(n = sma_len)
    
    if (nrow(last_n) == sma_len && all(!is.na(last_n$close))) {
      entry_sma <- mean(last_n$close)
    }
  }
  
  entry_below_sma10 <- if (!is.na(entry_sma)) {
    entry_price < entry_sma
  } else {
    NA
  }
  
  # ------------------------------------------------------------------
  # Features known before/at entry
  # ------------------------------------------------------------------
  
  sma10_prior <- if (!is.na(entry_idx) && entry_idx > 10) {
    mean(px_full$close[(entry_idx - 10):(entry_idx - 1)], na.rm = TRUE)
  } else {
    NA_real_
  }
  
  dist_sma10 <- if (!is.na(sma10_prior) && sma10_prior > 0) {
    entry_price / sma10_prior - 1
  } else {
    NA_real_
  }
  
  ret_5d_prior <- if (!is.na(entry_idx) && entry_idx > 6) {
    px_full$close[[entry_idx - 1]] / px_full$close[[entry_idx - 6]] - 1
  } else {
    NA_real_
  }
  
  ret_20d_prior <- if (!is.na(entry_idx) && entry_idx > 21) {
    px_full$close[[entry_idx - 1]] / px_full$close[[entry_idx - 21]] - 1
  } else {
    NA_real_
  }
  
  daily_ret <- px_full$close / dplyr::lag(px_full$close) - 1
  
  enough_20d_history <- FALSE
  volatility_20d <- NA_real_
  
  if (!is.na(entry_idx) && entry_idx > 21) {
    prior_20_returns <- daily_ret[(entry_idx - 20):(entry_idx - 1)]
    enough_20d_history <- length(prior_20_returns) == 20 && sum(!is.na(prior_20_returns)) >= 19
    
    if (enough_20d_history) {
      volatility_20d <- sd(prior_20_returns, na.rm = TRUE)
    }
  }
  
  volatility_20d_ann <- if (!is.na(volatility_20d)) {
    volatility_20d * sqrt(252)
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
  
  # ------------------------------------------------------------------
  # Exit logic
  # ------------------------------------------------------------------
  
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
  
  tgt_px <- if (is_short) entry_price * (1 - target_pct) else entry_price * (1 + target_pct)
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
      
      if (nrow(day_row) == 0) day_row <- px_full %>% slice_tail(n = 1)
      
      first_exit_d <- day_row$date[[1]]
      first_exit_px <- day_row$close[[1]]
      final_exit_d <- first_exit_d
      final_exit_px <- first_exit_px
    }
    
  } else if (isTRUE(first_event$stop_hit[[1]])) {
    
    part_exit <- FALSE
    stop_exit <- TRUE
    completed <- TRUE
    trade_status <- "completed"
    completion_reason <- "stop_first"
    exit_reason <- "stop_first"
    
    first_exit_d <- first_event$date[[1]]
    first_exit_px <- stop_px
    final_exit_d <- first_exit_d
    final_exit_px <- first_exit_px
    
  } else {
    
    part_exit <- TRUE
    stop_exit <- FALSE
    
    first_exit_d <- first_event$date[[1]]
    first_exit_px <- tgt_px
    
    post_target_px <- px_full %>% filter(date > first_exit_d)
    
    if (nrow(post_target_px) == 0) {
      completed <- FALSE
      trade_status <- "open_runner"
      completion_reason <- "runner_no_post_target_data"
      exit_reason <- "open_runner"
      final_exit_d <- mtm_date
      final_exit_px <- mtm_price
      
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
        } else {
          completed <- FALSE
          trade_status <- "open_runner"
          completion_reason <- "runner_no_trail_exit_yet"
          exit_reason <- "open_runner"
          final_exit_d <- mtm_date
          final_exit_px <- mtm_price
        }
        
      } else {
        armed_rows <- post_target_px %>%
          mutate(above_sma = close > sma_val)
        
        first_reclaim <- armed_rows %>%
          filter(above_sma) %>%
          slice_head(n = 1)
        
        pre_reclaim_stop <- armed_rows %>%
          { if (nrow(first_reclaim) > 0) filter(., date <= first_reclaim$date[[1]]) else . } %>%
          filter(low <= stop_px) %>%
          slice_head(n = 1)
        
        if (nrow(pre_reclaim_stop) > 0) {
          completed <- TRUE
          trade_status <- "completed"
          completion_reason <- "pre_reclaim_stop"
          exit_reason <- "pre_reclaim_stop"
          final_exit_d <- pre_reclaim_stop$date[[1]]
          final_exit_px <- stop_px
          
        } else if (nrow(first_reclaim) == 0) {
          completed <- FALSE
          trade_status <- "open_runner"
          completion_reason <- "runner_waiting_for_sma_reclaim"
          exit_reason <- "open_runner"
          final_exit_d <- mtm_date
          final_exit_px <- mtm_price
          
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
          } else {
            completed <- FALSE
            trade_status <- "open_runner"
            completion_reason <- "runner_no_trail_exit_yet"
            exit_reason <- "open_runner"
            final_exit_d <- mtm_date
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
          completed <- TRUE
          trade_status <- "completed"
          completion_reason <- "runner_trail_exit"
          exit_reason <- "runner_trail_exit"
          final_exit_d <- trail_exit$date[[1]]
          final_exit_px <- trail_exit$close[[1]]
        } else {
          completed <- FALSE
          trade_status <- "open_runner"
          completion_reason <- "runner_no_trail_exit_yet"
          exit_reason <- "open_runner"
          final_exit_d <- mtm_date
          final_exit_px <- mtm_price
        }
        
      } else {
        armed_rows <- post_target_px %>%
          mutate(below_sma = close < sma_val)
        
        first_reclaim <- armed_rows %>%
          filter(below_sma) %>%
          slice_head(n = 1)
        
        pre_reclaim_stop <- armed_rows %>%
          { if (nrow(first_reclaim) > 0) filter(., date <= first_reclaim$date[[1]]) else . } %>%
          filter(high >= stop_px) %>%
          slice_head(n = 1)
        
        if (nrow(pre_reclaim_stop) > 0) {
          completed <- TRUE
          trade_status <- "completed"
          completion_reason <- "pre_reclaim_stop"
          exit_reason <- "pre_reclaim_stop"
          final_exit_d <- pre_reclaim_stop$date[[1]]
          final_exit_px <- stop_px
          
        } else if (nrow(first_reclaim) == 0) {
          completed <- FALSE
          trade_status <- "open_runner"
          completion_reason <- "runner_waiting_for_sma_reclaim"
          exit_reason <- "open_runner"
          final_exit_d <- mtm_date
          final_exit_px <- mtm_price
          
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
          } else {
            completed <- FALSE
            trade_status <- "open_runner"
            completion_reason <- "runner_no_trail_exit_yet"
            exit_reason <- "open_runner"
            final_exit_d <- mtm_date
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
    
    sma10_prior = sma10_prior,
    dist_sma10 = dist_sma10,
    ret_5d_prior = ret_5d_prior,
    ret_20d_prior = ret_20d_prior,
    volatility_20d = volatility_20d,
    volatility_20d_ann = volatility_20d_ann,
    enough_20d_history = enough_20d_history,
    gap_from_prev_close = gap_from_prev_close,
    directional_ret_5d = directional_ret_5d,
    directional_ret_20d = directional_ret_20d,
    directional_dist_sma10 = directional_dist_sma10,
    
    rows_before_entry = rows_before_entry,
    non_na_closes_before_entry = non_na_closes_before_entry,
    px_start = safe_min_date(px_full$date),
    px_end = safe_max_date(px_full$date),
    
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
                                lookback = LOOKBACK_DAYS,
                                lockup = LOCKUP_DAYS,
                                sma_len = SMA_LEN,
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
  
  bind_cols(
    row %>% select(-signal_key),
    out %>% select(-ticker, -side, -tweet_ts_et)
  ) %>%
    mutate(win = total_return > 0)
}

###############################################################################
# 8 · Run backtest --------------------------------------------------------------
###############################################################################

cat("Running simulations...\n")

results <- purrr::map_dfr(
  seq_len(nrow(signals_run)),
  function(i) {
    row_i <- signals_run[i, , drop = FALSE]
    
    message(sprintf(
      "[%d/%d] Processing: %s | %s",
      i,
      nrow(signals_run),
      row_i$username[[1]],
      row_i$ticker[[1]]
    ))
    
    tryCatch(
      simulate_trade_full(
        row = row_i,
        cache_dir = CACHE_DIR,
        as_of_date = AS_OF_DATE
      ),
      error = function(e) {
        message(sprintf("Error on %s: %s", row_i$ticker[[1]], e$message))
        tibble()
      }
    )
  }
)

cat("Simulation rows produced:", nrow(results), "\n")

if (nrow(results) == 0) {
  cat("No simulation rows produced. Exiting.\n")
  quit(save = "no", status = 0)
}

###############################################################################
# 9 · Add SPY market regime -----------------------------------------------------
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
      "spy_close",
      "spy_sma50",
      "spy_sma200",
      "market_regime_50",
      "market_regime_200"
    )))
  
  spy_to <- max(results_tbl$data_through_date, na.rm = TRUE)
  
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
      spy_sma50 = TTR::SMA(close, n = 50),
      spy_sma200 = TTR::SMA(close, n = 200),
      market_regime_50 = case_when(
        is.na(spy_sma50) ~ NA_character_,
        close > spy_sma50 ~ "bullish",
        close < spy_sma50 ~ "bearish",
        TRUE ~ "neutral"
      ),
      market_regime_200 = case_when(
        is.na(spy_sma200) ~ NA_character_,
        close > spy_sma200 ~ "bullish",
        close < spy_sma200 ~ "bearish",
        TRUE ~ "neutral"
      )
    ) %>%
    transmute(
      date,
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
        spy_regime %>% arrange(date),
        by = join_by(closest(entry_date > date))
      )
  } else {
    results_tbl %>%
      arrange(entry_date) %>%
      left_join(
        spy_regime %>% arrange(date),
        by = join_by(closest(entry_date >= date))
      )
  }
}

results <- add_spy_regime(
  results_tbl = results,
  cache_dir = CACHE_DIR,
  spy_symbol = "SPY",
  spy_from = as.Date("2009-01-01"),
  use_previous_day = TRUE
)

cat("Rows after SPY regime:", nrow(results), "\n")

cat("Volatility coverage in this run:\n")
print(
  results %>%
    summarise(
      n = n(),
      n_with_volatility = sum(!is.na(volatility_20d)),
      n_missing_volatility = sum(is.na(volatility_20d)),
      pct_with_volatility = mean(!is.na(volatility_20d))
    )
)

###############################################################################
# 10 · Prepare upload -----------------------------------------------------------
###############################################################################

upload_started_at <- Sys.time()

results_upload <- results

if ("date" %in% names(results_upload)) {
  results_upload <- results_upload %>%
    mutate(spy_regime_date = as.Date(date)) %>%
    select(-date)
}

date_cols <- intersect(
  c(
    "signal_date",
    "entry_date",
    "px_start",
    "px_end",
    "first_exit_date",
    "final_exit_date",
    "data_through_date",
    "spy_regime_date"
  ),
  names(results_upload)
)

results_upload <- results_upload %>%
  mutate(across(all_of(date_cols), as.Date))

if ("tweet_ts_et" %in% names(results_upload)) {
  results_upload <- results_upload %>%
    mutate(tweet_ts_et = as.POSIXct(tweet_ts_et, tz = "America/New_York"))
}

char_cols <- intersect(
  c(
    "signal_id",
    "ticker",
    "side",
    "horizon",
    "rationale",
    "username",
    "conversation_id",
    "evidence_types",
    "evidence_snippet",
    "entry_at",
    "exit_reason",
    "trade_status",
    "completion_reason",
    "market_regime_50",
    "market_regime_200"
  ),
  names(results_upload)
)

results_upload <- results_upload %>%
  mutate(
    across(all_of(char_cols), as.character),
    strategy_label = STRATEGY_LABEL
  )

results_upload <- results_upload %>%
  mutate(
    trade_id_base = paste(
      strategy_label,
      username,
      conversation_id,
      ticker,
      format(lubridate::with_tz(tweet_ts_et, "America/New_York"), "%Y%m%d%H%M%S"),
      entry_at,
      sep = "_"
    )
  ) %>%
  group_by(trade_id_base) %>%
  mutate(
    trade_id = if (dplyr::n() == 1L) {
      trade_id_base
    } else {
      paste0(trade_id_base, "_", dplyr::row_number())
    }
  ) %>%
  ungroup() %>%
  select(-trade_id_base) %>%
  mutate(loaded_at = upload_started_at) %>%
  relocate(trade_id, strategy_label, .before = ticker) %>%
  distinct(trade_id, .keep_all = TRUE)

cat("Upload rows:", nrow(results_upload), "\n")

print(
  results_upload %>%
    summarise(
      n_rows = n(),
      n_distinct_trade_ids = n_distinct(trade_id),
      first_entry_date = min(entry_date, na.rm = TRUE),
      last_entry_date = max(entry_date, na.rm = TRUE),
      n_with_volatility = sum(!is.na(volatility_20d)),
      n_missing_volatility = sum(is.na(volatility_20d))
    )
)

###############################################################################
# 11 · Upload / upsert ----------------------------------------------------------
###############################################################################

target_tbl <- DBI::Id(schema = "public", table = TARGET_TABLE_NAME)
target_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, target_tbl))

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
  
  if (length(missing_cols) == 0) return(invisible(TRUE))
  
  for (col in missing_cols) {
    sql_type <- sql_type_for_vector(data[[col]])
    sql <- sprintf(
      "ALTER TABLE %s ADD COLUMN %s %s;",
      target_tbl_q,
      qid(col),
      sql_type
    )
    
    message("Adding missing column to target table: ", col, " ", sql_type)
    DBI::dbExecute(con, sql)
  }
  
  invisible(TRUE)
}

create_target_indexes <- function(con, table_name) {
  safe_idx_base <- gsub("[^A-Za-z0-9_]", "_", table_name)
  
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_trade_id ON public.%s(trade_id);",
      safe_idx_base,
      table_name
    )
  )
  
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS idx_%s_entry_date ON public.%s(entry_date);",
      safe_idx_base,
      table_name
    )
  )
  
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS idx_%s_ticker ON public.%s(ticker);",
      safe_idx_base,
      table_name
    )
  )
  
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS idx_%s_username ON public.%s(username);",
      safe_idx_base,
      table_name
    )
  )
  
  if ("signal_id" %in% names(results_upload)) {
    DBI::dbExecute(
      con,
      sprintf(
        "CREATE INDEX IF NOT EXISTS idx_%s_signal_id ON public.%s(signal_id);",
        safe_idx_base,
        table_name
      )
    )
  }
}

if (!DBI::dbExistsTable(con, target_tbl)) {
  
  message("Creating new table: public.", TARGET_TABLE_NAME)
  
  DBI::dbWriteTable(
    con,
    target_tbl,
    results_upload,
    overwrite = FALSE,
    row.names = FALSE
  )
  
  create_target_indexes(con, TARGET_TABLE_NAME)
  
} else {
  
  message("Table already exists. Upserting into: public.", TARGET_TABLE_NAME)
  
  ensure_missing_columns(con, target_tbl, results_upload)
  create_target_indexes(con, TARGET_TABLE_NAME)
  
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
  
  target_cols <- DBI::dbListFields(con, target_tbl)
  cols <- intersect(names(results_upload), target_cols)
  
  update_cols <- setdiff(cols, "trade_id")
  
  update_set <- paste(
    vapply(
      update_cols,
      function(x) sprintf("%s = EXCLUDED.%s", qid(x), qid(x)),
      character(1)
    ),
    collapse = ", "
  )
  
  col_list <- paste(vapply(cols, qid, character(1)), collapse = ", ")
  
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
}

###############################################################################
# 12 · Final checks -------------------------------------------------------------
###############################################################################

cat("Final table checks:\n")

print(
  DBI::dbGetQuery(
    con,
    sprintf(
      "SELECT COUNT(*) AS n_rows FROM public.%s;",
      TARGET_TABLE_NAME
    )
  )
)

print(
  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        COUNT(*) AS n_rows,
        COUNT(volatility_20d) AS n_with_volatility,
        COUNT(*) - COUNT(volatility_20d) AS n_missing_volatility,
        MIN(entry_date) AS first_entry_date,
        MAX(entry_date) AS last_entry_date,
        MAX(loaded_at) AS latest_loaded_at
      FROM public.%s
      WHERE strategy_label = %s;
      ",
      TARGET_TABLE_NAME,
      as.character(DBI::dbQuoteString(con, STRATEGY_LABEL))
    )
  )
)

print(
  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        trade_status,
        completed,
        COUNT(*) AS n_rows
      FROM public.%s
      WHERE strategy_label = %s
      GROUP BY trade_status, completed
      ORDER BY n_rows DESC;
      ",
      TARGET_TABLE_NAME,
      as.character(DBI::dbQuoteString(con, STRATEGY_LABEL))
    )
  )
)

print(
  DBI::dbGetQuery(
    con,
    sprintf(
      "
      SELECT
        strategy_label,
        target_pct,
        stop_pct,
        COUNT(*) AS n_rows,
        COUNT(volatility_20d) AS n_with_volatility
      FROM public.%s
      GROUP BY strategy_label, target_pct, stop_pct
      ORDER BY n_rows DESC;
      ",
      TARGET_TABLE_NAME
    )
  )
)

cat("refresh_backtest_results.R completed successfully.\n")

dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_trade_signals;")

message("✅ Done. Upserted ", nrow(signals_db), " signals at ", Sys.time())

