#!/usr/bin/env Rscript

###############################################################################
# refresh_trade_signal_features.R
#
# Reads public.twitter_trade_signals, calculates market/ticker features, and
# upserts them into public.twitter_trade_signal_features.
#
# Intended pipeline:
#   twitter_ingest.R
#   generate_tweet_signals.R
#   refresh_trade_signal_features.R
#   refresh_backtest_results.R
###############################################################################

cat("RUNNING FILE: refresh_trade_signal_features.R | SIGNAL FEATURE REFRESH\n")

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(stringr)
  library(lubridate)
  library(zoo)
  library(jsonlite)
  library(tidyr)
  library(glue)
})

`%||%` <- function(x, y) if (is.null(x)) y else x

###############################################################################
# 0) Controls -----------------------------------------------------------------
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

get_env_first <- function(...) {
  names <- c(...)
  for (nm in names) {
    val <- Sys.getenv(nm, unset = "")
    if (nzchar(val)) return(val)
  }
  ""
}

SIGNALS_TABLE <- Sys.getenv("SIGNALS_TABLE", "twitter_trade_signals")
FEATURES_TABLE <- Sys.getenv("FEATURES_TABLE", "twitter_trade_signal_features")

FEATURE_LOOKBACK_DAYS <- env_num(
  "FEATURE_LOOKBACK_DAYS",
  env_num("NEW_SIGNAL_LOOKBACK_DAYS", 30)
)

MAX_FEATURE_ROWS <- env_int_or_inf("MAX_FEATURE_ROWS", Inf)
DEBUG_YAHOO <- env_bool("DEBUG_YAHOO", FALSE)
MAX_FEATURE_LAG_DAYS <- env_num("MAX_FEATURE_LAG_DAYS", 7)

AS_OF_DATE <- Sys.getenv("AS_OF_DATE", unset = "")
AS_OF_DATE <- if (nzchar(AS_OF_DATE)) {
  as.Date(AS_OF_DATE)
} else {
  as.Date(lubridate::with_tz(Sys.time(), "America/New_York"))
}

if (is.na(AS_OF_DATE)) {
  stop("AS_OF_DATE could not be parsed. Use format YYYY-MM-DD.")
}

cat("Signals table:", SIGNALS_TABLE, "\n")
cat("Features table:", FEATURES_TABLE, "\n")
cat("Feature lookback days:", FEATURE_LOOKBACK_DAYS, "\n")
cat("Max feature rows:", as.character(MAX_FEATURE_ROWS), "\n")
cat("Max feature lag days:", MAX_FEATURE_LAG_DAYS, "\n")
cat("Debug Yahoo:", DEBUG_YAHOO, "\n")
cat("As-of date:", as.character(AS_OF_DATE), "\n")

###############################################################################
# 1) Supabase connection -------------------------------------------------------
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

print(DBI::dbGetQuery(con, "select 'connected' as status, now();"))

qid <- function(x) as.character(DBI::dbQuoteIdentifier(con, x))

signals_tbl <- DBI::Id(schema = "public", table = SIGNALS_TABLE)
features_tbl <- DBI::Id(schema = "public", table = FEATURES_TABLE)

signals_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, signals_tbl))
features_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, features_tbl))

if (!DBI::dbExistsTable(con, signals_tbl)) {
  stop("Signals table does not exist: public.", SIGNALS_TABLE)
}

###############################################################################
# 2) Target table helpers ------------------------------------------------------
###############################################################################

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
    message("No missing columns to add to ", FEATURES_TABLE, ".")
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
    message("Adding missing column to ", FEATURES_TABLE, ": ", col, " ", sql_type)
    DBI::dbExecute(con, sql)
  }

  invisible(TRUE)
}

create_features_table_if_needed <- function(con) {
  DBI::dbExecute(
    con,
    glue::glue_sql(
      "
      CREATE TABLE IF NOT EXISTS {`features_tbl_q`} (
        signal_id text PRIMARY KEY,
        username text,
        conversation_id text,
        created_at timestamptz,
        text text,
        ticker text,
        conviction double precision,
        direction text,
        horizon text,
        stop_loss_pct integer,
        buy_zone boolean,
        sentiment_score double precision,
        rationale text,
        specificity_score double precision,
        has_quant_data boolean,
        evidence_score double precision,
        evidence_types text,
        evidence_snippet text,
        row_id integer,
        ticker_raw text,
        signal_date date,
        created_at_utc timestamptz,
        tweet_ts_et timestamptz,
        entry_date date,
        entry_at text,
        feature_date date,
        feature_lag_days integer,
        prev_close double precision,
        prev_open double precision,
        prev_high double precision,
        prev_low double precision,
        prev_volume double precision,
        ret_1d_prior double precision,
        ret_5d_prior double precision,
        ret_10d_prior double precision,
        ret_15d_prior double precision,
        ret_20d_prior double precision,
        volatility_5d double precision,
        volatility_10d double precision,
        volatility_15d double precision,
        volatility_20d double precision,
        volatility_5d_ann double precision,
        volatility_10d_ann double precision,
        volatility_15d_ann double precision,
        volatility_20d_ann double precision,
        sma10_prior double precision,
        prev_dist_sma10 double precision,
        prev_range_pct double precision,
        prev_intraday_return double precision,
        avg_volume_5d double precision,
        avg_volume_10d double precision,
        avg_volume_20d double precision,
        volume_ratio_5d double precision,
        volume_ratio_10d double precision,
        volume_ratio_20d double precision,
        enough_5d_history boolean,
        enough_10d_history boolean,
        enough_15d_history boolean,
        enough_20d_history boolean,
        updated_at timestamptz DEFAULT now()
      );
      ",
      .con = con
    )
  )

  safe_idx_base <- gsub("[^A-Za-z0-9_]", "_", FEATURES_TABLE)

  DBI::dbExecute(con, sprintf(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_signal_id ON public.%s(signal_id);",
    safe_idx_base, FEATURES_TABLE
  ))
  DBI::dbExecute(con, sprintf(
    "CREATE INDEX IF NOT EXISTS idx_%s_created_at ON public.%s(created_at);",
    safe_idx_base, FEATURES_TABLE
  ))
  DBI::dbExecute(con, sprintf(
    "CREATE INDEX IF NOT EXISTS idx_%s_ticker ON public.%s(ticker);",
    safe_idx_base, FEATURES_TABLE
  ))
  DBI::dbExecute(con, sprintf(
    "CREATE INDEX IF NOT EXISTS idx_%s_feature_date ON public.%s(feature_date);",
    safe_idx_base, FEATURES_TABLE
  ))

  invisible(TRUE)
}

create_features_table_if_needed(con)

###############################################################################
# 3) Ticker + Yahoo helpers ----------------------------------------------------
###############################################################################

is_bad_extracted_ticker <- function(x) {
  x <- toupper(trimws(as.character(x)))

  is.na(x) |
    x == "" |
    x %in% c("N/A", "NA", "NONE", "NULL", "TICKER", "URL", "TRUE") |
    grepl("_", x) |
    grepl("\\s", x) |
    grepl("[^A-Z0-9.=:&-]", x) |
    grepl("^[0-9]+(\\.[0-9]+)?$", x) |
    grepl("^[A-Z]+[0-9]+[A-Z0-9]*$", x) |
    x %in% c(
      "GOTR", "GOOGLE_TRENDS", "GOOGLE TRENDS",
      "OPENAI", "ANTHROPIC", "PERPLEXITY", "SPACEX", "WAYMO",
      "STRIPE", "DATABRICKS", "DEEPL", "ELEVENLABS", "CANVA",
      "SUBSTACK", "POLYMARKET", "KALSHI", "FIGURE", "FORBES",
      "BLOOMBERG", "CNBC", "WSJ", "FED", "OPEC", "NYSE",
      "NBA", "WNBA", "MAG7", "S&P 500", "DXY", "US10Y", "US3Y",
      "CITADEL", "CLICKHOUSE", "CHICK-FIL-A", "BORING COMPANY",
      "SUPABASE", "WORDPRESS", "FRAMER", "LOVABLE", "JASPER",
      "GROK", "XAI", "UNITREE", "ANDURIL", "ARDUINO", "BEEHIIV"
    )
}

normalize_symbol_for_yahoo <- function(sym) {
  raw_sym <- toupper(trimws(as.character(sym)))
  sym <- gsub("^\\$", "", raw_sym)
  sym <- toupper(trimws(sym))
  sym <- gsub("\\s+", "", sym)

  ticker_overrides <- c(
    "ADIDAS" = "ADS.DE", "ADIDAS.DE" = "ADS.DE", "ADS.GR" = "ADS.DE",
    "ASOS" = "ASC.L", "ARAMCO" = "2222.SR", "A2M" = "A2M.AX",
    "005930.K" = "005930.KS", "700.HK" = "0700.HK", "325.HK" = "0325.HK",
    "2317.TW" = "2317.TW", "2454.TW" = "2454.TW",
    "BAS" = "BAS.DE", "BMW" = "BMW.DE", "BATS" = "BATS.L",
    "BOO" = "BOO.L", "BOOH" = "BOO.L", "BOSS.GY" = "BOSS.DE",
    "BOSS:GR" = "BOSS.DE", "BOSS.GR" = "BOSS.DE", "BDL.NS" = "BDL.NS",
    "BEL.NS" = "BEL.NS", "BC.MI" = "BC.MI", "BANB.SW" = "BANB.SW",
    "BEN.FP" = "BEN.PA", "BNB" = "BNB-USD", "BONK" = "BONK-USD",
    "BRETT" = "BRETT-USD", "APECOIN" = "APE-USD", "BIFI" = "BIFI-USD",
    "BTRST" = "BTRST-USD", "BOSON" = "BOSON-USD",
    "CATG.FP" = "CATG.PA", "CDR.PW" = "CDR.WA", "COREWEAVE" = "CRWV",
    "CIRCLE" = "CRCL", "CELO" = "CELO-USD", "CELR" = "CELR-USD",
    "CHZ" = "CHZ-USD", "CORE" = "CORE-USD", "COQ" = "COQ-USD",
    "CANTO" = "CANTO-USD", "DFS.LN" = "DFS.L", "EL.FR" = "EL.PA",
    "EMBRAC_B" = "EMBRAC-B.ST", "FLOKI" = "FLOKI-USD", "FTM" = "FTM-USD",
    "GRT" = "GRT-USD", "GSPC" = "^GSPC", "IXIC" = "^IXIC",
    "IRX" = "^IRX", "ITX" = "ITX.MC", "KER.FP" = "KER.PA",
    "MC.FP" = "MC.PA", "MINA" = "MINA-USD", "N225" = "^N225",
    "NIKE" = "NKE", "ONDO" = "ONDO-USD", "PRADA" = "1913.HK",
    "PUIG" = "PUIG.MC", "PUM.GY" = "PUM.DE", "PUM.GR" = "PUM.DE",
    "RMS.FP" = "RMS.PA", "SAMSUNG" = "005930.KS", "SAP.GR" = "SAP.DE",
    "SFER.MI" = "SFER.MI", "SOL" = "SOL-USD", "SPX" = "^GSPC",
    "SQ" = "XYZ", "TCS" = "TCS.NS", "TATASTEEL.NS" = "TATASTEEL.NS",
    "TKWY" = "TKWY.AS", "TNX" = "^TNX", "UBI.FP" = "UBI.PA",
    "UHR.SW" = "UHR.SW", "VIX" = "^VIX", "VOW" = "VOW.DE",
    "XAUUSD" = "GC=F", "XAGUSD" = "SI=F", "ZAL" = "ZAL.DE"
  )

  if (!is.na(raw_sym) && raw_sym %in% names(ticker_overrides)) return(unname(ticker_overrides[[raw_sym]]))
  if (!is.na(sym) && sym %in% names(ticker_overrides)) return(unname(ticker_overrides[[sym]]))

  if (is_bad_extracted_ticker(raw_sym)) return(NA_character_)

  if (is.na(sym) || sym == "" || grepl("_", sym) || grepl("^[0-9]+\\.[0-9]+$", sym) || grepl("^[0-9]+[A-Z]+$", sym)) {
    return(NA_character_)
  }

  if (grepl("^\\^[A-Z0-9]+$", sym) || grepl("^[A-Z0-9]+=[A-Z]$", sym)) return(sym)
  if (grepl("^[A-Z0-9]+-USD$", sym)) return(sym)
  if (grepl("^[0-9]{4}\\.JP$", sym)) return(sub("\\.JP$", ".T", sym))
  if (grepl("^[0-9]{6}\\.K$", sym)) return(sub("\\.K$", ".KS", sym))
  if (grepl("^[A-Z0-9]+\\.FP$", sym)) return(sub("\\.FP$", ".PA", sym))
  if (grepl("^[A-Z0-9]+\\.(GY|GR)$", sym)) return(sub("\\.(GY|GR)$", ".DE", sym))
  if (grepl("^[A-Z0-9]+\\.LN$", sym)) return(sub("\\.LN$", ".L", sym))

  if (grepl("^[0-9]{1,4}\\.HK$", sym)) {
    num <- sub("\\.HK$", "", sym)
    return(paste0(sprintf("%04d", as.integer(num)), ".HK"))
  }

  if (grepl("^[0-9]{6}\\.CH$", sym)) {
    num <- sub("\\.CH$", "", sym)
    if (grepl("^(000|001|002|003|300|301)", num)) return(paste0(num, ".SZ"))
    if (grepl("^(600|601|603|605|688)", num)) return(paste0(num, ".SS"))
    return(NA_character_)
  }

  if (grepl("^[0-9]{6}$", sym)) {
    if (grepl("^(000|001|002|003|300|301)", sym)) return(paste0(sym, ".SZ"))
    if (grepl("^(600|601|603|605|688)", sym)) return(paste0(sym, ".SS"))
    return(NA_character_)
  }

  if (grepl("\\.(TO|V|L|AX|SA|MX|PA|AS|DE|F|HK|SS|SZ|T|ST|KS|KQ|TW|MC|SR|MI|SW|NS|CO|OL|HE|VI|BR|WA|TA|CN|NE|BK)$", sym)) return(sym)
  if (grepl("^[A-Z]{1,5}(\\.[A-Z])?$", sym)) return(gsub("\\.", "-", sym))

  NA_character_
}

empty_px_tbl <- function() {
  tibble(date = as.Date(character()), open = numeric(), high = numeric(), low = numeric(), close = numeric(), volume = numeric())
}

us_market_holidays <- as.Date(c(
  "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26", "2025-06-19",
  "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
  "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25", "2026-06-19",
  "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25"
))

date_to_unix <- function(x, end_of_day = FALSE) {
  x <- as.POSIXct(as.Date(x), tz = "UTC")
  if (end_of_day) x <- x + 86399
  as.integer(x)
}

fetch_yahoo_chart_syscurl_text <- function(sym, from_date, to_date, retries = 6, debug = DEBUG_YAHOO) {
  if (is.na(from_date) || is.na(to_date) || !nzchar(sym)) return(NULL)

  yahoo_sym <- normalize_symbol_for_yahoo(sym)
  if (is.na(yahoo_sym) || !nzchar(yahoo_sym)) return(NULL)

  period1 <- date_to_unix(from_date, end_of_day = FALSE)
  period2 <- date_to_unix(to_date, end_of_day = TRUE)

url <- paste0(
  "https://query2.finance.yahoo.com/v8/finance/chart/",
  URLencode(yahoo_sym, reserved = TRUE),
  "?period1=", period1,
  "&period2=", period2,
  "&interval=1d",
  "&includeAdjustedClose=true"
)

  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) stop("curl not found on system PATH")

  args <- c(
    "--http1.1", "--ipv4", "-L", "--compressed", "--silent", "--show-error",
    "--user-agent", "Mozilla/5.0", url
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
    err_txt <- if (file.exists(err_file)) paste(readLines(err_file, warn = FALSE), collapse = "\n") else ""
    if (file.exists(err_file)) unlink(err_file)

    if (debug) {
      cat("Attempt:", k, "\n")
      cat("Ticker:", sym, "\n")
      cat("Yahoo symbol:", yahoo_sym, "\n")
      cat("URL:", url, "\n")
      cat("Status:", status, "\n")
      cat("Response chars:", nchar(txt), "\n")
      cat("Starts with:", substr(txt, 1, 120), "\n")
      if (nzchar(err_txt)) cat("Curl stderr:", err_txt, "\n")
    }

    if (identical(as.integer(status), 0L) && nzchar(txt) && grepl('"chart"', txt, fixed = TRUE) && !grepl("Too Many Requests", txt, fixed = TRUE)) {
      return(txt)
    }

    Sys.sleep(min(2^k, 8))
  }

  NULL
}

fetch_yahoo_chart_range_text <- function(sym, range = "10y", retries = 4, debug = DEBUG_YAHOO) {
  if (!nzchar(sym)) return(NULL)

  yahoo_sym <- normalize_symbol_for_yahoo(sym)

  if (is.na(yahoo_sym) || !nzchar(yahoo_sym)) {
    return(NULL)
  }

url <- paste0(
  "https://query2.finance.yahoo.com/v8/finance/chart/",
  URLencode(yahoo_sym, reserved = TRUE),
  "?range=", range,
  "&interval=1d",
  "&events=history",
  "&includePrePost=false",
  "&includeAdjustedClose=true"
)

  curl_bin <- Sys.which("curl")

  if (!nzchar(curl_bin)) {
    stop("curl not found on system PATH")
  }

  args <- c(
    "--http1.1",
    "--ipv4",
    "-L",
    "--compressed",
    "--silent",
    "--show-error",
    "--user-agent", "Mozilla/5.0",
    url
  )

  for (k in seq_len(retries)) {
    err_file <- tempfile(fileext = ".txt")

    out <- tryCatch(
      suppressWarnings(
        system2(
          curl_bin,
          args = args,
          stdout = TRUE,
          stderr = err_file
        )
      ),
      error = function(e) e
    )

    status <- if (inherits(out, "error")) 1L else attr(out, "status")
    if (is.null(status)) status <- 0L

    txt <- if (inherits(out, "error")) {
      ""
    } else {
      paste(out, collapse = "")
    }

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
      cat("URL:", url, "\n")
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
                    
                    

read_yahoo_chart_text <- function(txt) {
  if (is.null(txt) || !nzchar(txt)) return(empty_px_tbl())

  js <- tryCatch(jsonlite::fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)
  if (is.null(js) || is.null(js$chart) || !is.null(js$chart$error)) return(empty_px_tbl())

  res <- js$chart$result
  if (is.null(res) || length(res) == 0) return(empty_px_tbl())

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

  min_len <- min(length(ts_vec), length(open_vec), length(high_vec), length(low_vec), length(close_vec), length(vol_vec))
  if (min_len == 0) return(empty_px_tbl())

  tibble(
    date = as.Date(as.POSIXct(ts_vec[seq_len(min_len)], origin = "1970-01-01", tz = "UTC")),
    open = as.numeric(open_vec[seq_len(min_len)]),
    high = as.numeric(high_vec[seq_len(min_len)]),
    low = as.numeric(low_vec[seq_len(min_len)]),
    close = as.numeric(close_vec[seq_len(min_len)]),
    volume = as.numeric(vol_vec[seq_len(min_len)])
  ) %>%
    filter(!is.na(date), !is.na(close)) %>%
    arrange(date)
}

fetch_px_yahoo_chart <- function(sym, from_date, to_date, debug = DEBUG_YAHOO) {
  from_date <- as.Date(from_date)
  to_date <- as.Date(to_date)

  if (is.na(from_date) || is.na(to_date) || from_date >= to_date) {
    message("Invalid date range for ", sym, ": ", from_date, " to ", to_date)
    return(empty_px_tbl())
  }

  # First try the normal period1/period2 Yahoo request.
  txt <- fetch_yahoo_chart_syscurl_text(
    sym = sym,
    from_date = from_date,
    to_date = to_date,
    debug = debug
  )

  px <- read_yahoo_chart_text(txt)

  # If period-based fetch fails, try range=10y fallback.
  # This avoids Yahoo's repeated endDate = -1 bug.
  if (nrow(px) == 0) {
    message("Period-based Yahoo fetch failed for ", sym, ". Trying range=10y fallback...")

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
      date >= from_date,
      date <= to_date
    ) %>%
    arrange(date)
}

next_market_day_fallback <- function(date, market_holidays = as.Date(character())) {
  next_date <- as.Date(date) + lubridate::days(1)
  while (lubridate::wday(next_date, week_start = 1) %in% c(6, 7) || next_date %in% market_holidays) {
    next_date <- next_date + lubridate::days(1)
  }
  as.Date(next_date)
}

pick_entry_from_px <- function(px, tweet_ts_et, market_holidays = as.Date(character())) {
  tweet_ts_et <- lubridate::with_tz(as.POSIXct(tweet_ts_et, tz = "America/New_York"), "America/New_York")
  tweet_date <- as.Date(tweet_ts_et)
  tweet_time <- format(tweet_ts_et, "%H:%M:%S")

  available_dates <- sort(unique(as.Date(px$date)))
  available_dates <- available_dates[!is.na(available_dates)]

  get_next_entry_date <- function(date) {
    candidate_dates <- available_dates[available_dates > date]
    if (length(candidate_dates) > 0) return(as.Date(min(candidate_dates)))
    next_market_day_fallback(date, market_holidays = market_holidays)
  }

  same_day_exists <- tweet_date %in% available_dates
  if (!same_day_exists) return(list(entry_date = get_next_entry_date(tweet_date), entry_at = "open"))
  if (tweet_time < "09:00:00") return(list(entry_date = tweet_date, entry_at = "open"))
  if (tweet_time < "15:30:00") return(list(entry_date = tweet_date, entry_at = "close"))
  list(entry_date = get_next_entry_date(tweet_date), entry_at = "open")
}

###############################################################################
# 4) Feature calculation -------------------------------------------------------
###############################################################################

calc_signal_features_one <- function(ticker, tweet_ts_et, px, sma_len = 10, market_holidays = as.Date(character()), max_feature_lag_days = MAX_FEATURE_LAG_DAYS) {
  empty_out <- function() {
    tibble(
      entry_date = as.Date(NA), entry_at = NA_character_, feature_date = as.Date(NA), feature_lag_days = NA_integer_,
      prev_close = NA_real_, prev_open = NA_real_, prev_high = NA_real_, prev_low = NA_real_, prev_volume = NA_real_,
      ret_1d_prior = NA_real_, ret_5d_prior = NA_real_, ret_10d_prior = NA_real_, ret_15d_prior = NA_real_, ret_20d_prior = NA_real_,
      volatility_5d = NA_real_, volatility_10d = NA_real_, volatility_15d = NA_real_, volatility_20d = NA_real_,
      volatility_5d_ann = NA_real_, volatility_10d_ann = NA_real_, volatility_15d_ann = NA_real_, volatility_20d_ann = NA_real_,
      sma10_prior = NA_real_, prev_dist_sma10 = NA_real_, prev_range_pct = NA_real_, prev_intraday_return = NA_real_,
      avg_volume_5d = NA_real_, avg_volume_10d = NA_real_, avg_volume_20d = NA_real_,
      volume_ratio_5d = NA_real_, volume_ratio_10d = NA_real_, volume_ratio_20d = NA_real_,
      enough_5d_history = FALSE, enough_10d_history = FALSE, enough_15d_history = FALSE, enough_20d_history = FALSE
    )
  }

  if (!is.data.frame(px) || nrow(px) == 0 || !"date" %in% names(px)) return(empty_out())

  px_full <- px %>%
    arrange(date) %>%
    mutate(
      date = as.Date(date), close = as.numeric(close), open = as.numeric(open), high = as.numeric(high), low = as.numeric(low), volume = as.numeric(volume),
      close = zoo::na.locf(close, na.rm = FALSE), open = zoo::na.locf(open, na.rm = FALSE), high = zoo::na.locf(high, na.rm = FALSE), low = zoo::na.locf(low, na.rm = FALSE)
    ) %>%
    filter(!is.na(date), !is.na(close), !is.na(open), !is.na(high), !is.na(low))

  if (nrow(px_full) < 21) return(empty_out())

  tweet_ts_et <- lubridate::with_tz(as.POSIXct(tweet_ts_et, tz = "America/New_York"), "America/New_York")
  tweet_date <- as.Date(tweet_ts_et)
  tweet_time <- format(tweet_ts_et, "%H:%M:%S")

  entry_pick <- pick_entry_from_px(px_full, tweet_ts_et, market_holidays = market_holidays)
  entry_date <- entry_pick$entry_date
  entry_at <- entry_pick$entry_at
  if (is.na(entry_date)) entry_at <- NA_character_

  has_tweet_date_px <- tweet_date %in% px_full$date
  feature_date <- if (has_tweet_date_px && tweet_time >= "16:00:00") {
    tweet_date
  } else {
    suppressWarnings(max(px_full$date[px_full$date < tweet_date], na.rm = TRUE))
  }

  if (is.infinite(feature_date)) return(empty_out())

  feature_date <- as.Date(feature_date)
  feature_lag_days <- as.integer(tweet_date - feature_date)
  if (is.na(feature_lag_days) || feature_lag_days > max_feature_lag_days) return(empty_out())

  feature_idx <- match(feature_date, px_full$date)
  if (is.na(feature_idx) || feature_idx <= 1) return(empty_out())

  daily_ret <- px_full$close / dplyr::lag(px_full$close) - 1

  calc_prior_ret <- function(n_days) {
    end_idx <- feature_idx
    start_idx <- feature_idx - n_days
    if (!is.na(feature_idx) && start_idx >= 1 && end_idx >= 1 && end_idx <= nrow(px_full) && !is.na(px_full$close[[start_idx]]) && !is.na(px_full$close[[end_idx]]) && px_full$close[[start_idx]] > 0) {
      px_full$close[[end_idx]] / px_full$close[[start_idx]] - 1
    } else {
      NA_real_
    }
  }

  calc_prior_vol <- function(n_days) {
    end_idx <- feature_idx
    start_idx <- feature_idx - n_days + 1
    if (!is.na(feature_idx) && start_idx >= 2 && end_idx >= 2 && end_idx <= length(daily_ret)) {
      vals <- daily_ret[start_idx:end_idx]
      vals <- vals[!is.na(vals)]
      if (length(vals) >= 2) sd(vals) else NA_real_
    } else {
      NA_real_
    }
  }

  calc_prior_avg_volume <- function(n_days) {
    end_idx <- feature_idx
    start_idx <- feature_idx - n_days + 1
    if (!is.na(feature_idx) && start_idx >= 1 && end_idx >= 1 && end_idx <= nrow(px_full) && "volume" %in% names(px_full)) {
      vals <- px_full$volume[start_idx:end_idx]
      vals <- vals[!is.na(vals)]
      if (length(vals) > 0) mean(vals) else NA_real_
    } else {
      NA_real_
    }
  }

  feature_row <- px_full[feature_idx, , drop = FALSE]
  prev_close <- feature_row$close[[1]]
  prev_open <- feature_row$open[[1]]
  prev_high <- feature_row$high[[1]]
  prev_low <- feature_row$low[[1]]
  prev_volume <- feature_row$volume[[1]]

  ret_1d_prior <- calc_prior_ret(1)
  ret_5d_prior <- calc_prior_ret(5)
  ret_10d_prior <- calc_prior_ret(10)
  ret_15d_prior <- calc_prior_ret(15)
  ret_20d_prior <- calc_prior_ret(20)

  volatility_5d <- calc_prior_vol(5)
  volatility_10d <- calc_prior_vol(10)
  volatility_15d <- calc_prior_vol(15)
  volatility_20d <- calc_prior_vol(20)

  sma10_prior <- if (!is.na(feature_idx) && feature_idx >= sma_len) {
    mean(px_full$close[(feature_idx - sma_len + 1):feature_idx], na.rm = TRUE)
  } else {
    NA_real_
  }

  prev_dist_sma10 <- if (!is.na(prev_close) && !is.na(sma10_prior) && sma10_prior > 0) prev_close / sma10_prior - 1 else NA_real_
  prev_range_pct <- if (!is.na(prev_high) && !is.na(prev_low) && !is.na(prev_close) && prev_close > 0) (prev_high - prev_low) / prev_close else NA_real_
  prev_intraday_return <- if (!is.na(prev_open) && !is.na(prev_close) && prev_open > 0) prev_close / prev_open - 1 else NA_real_

  avg_volume_5d <- calc_prior_avg_volume(5)
  avg_volume_10d <- calc_prior_avg_volume(10)
  avg_volume_20d <- calc_prior_avg_volume(20)

  volume_ratio_5d <- if (!is.na(prev_volume) && !is.na(avg_volume_5d) && avg_volume_5d > 0) prev_volume / avg_volume_5d else NA_real_
  volume_ratio_10d <- if (!is.na(prev_volume) && !is.na(avg_volume_10d) && avg_volume_10d > 0) prev_volume / avg_volume_10d else NA_real_
  volume_ratio_20d <- if (!is.na(prev_volume) && !is.na(avg_volume_20d) && avg_volume_20d > 0) prev_volume / avg_volume_20d else NA_real_

  tibble(
    entry_date = entry_date,
    entry_at = entry_at,
    feature_date = feature_date,
    feature_lag_days = feature_lag_days,
    prev_close = prev_close,
    prev_open = prev_open,
    prev_high = prev_high,
    prev_low = prev_low,
    prev_volume = prev_volume,
    ret_1d_prior = ret_1d_prior,
    ret_5d_prior = ret_5d_prior,
    ret_10d_prior = ret_10d_prior,
    ret_15d_prior = ret_15d_prior,
    ret_20d_prior = ret_20d_prior,
    volatility_5d = volatility_5d,
    volatility_10d = volatility_10d,
    volatility_15d = volatility_15d,
    volatility_20d = volatility_20d,
    volatility_5d_ann = volatility_5d * sqrt(252),
    volatility_10d_ann = volatility_10d * sqrt(252),
    volatility_15d_ann = volatility_15d * sqrt(252),
    volatility_20d_ann = volatility_20d * sqrt(252),
    sma10_prior = sma10_prior,
    prev_dist_sma10 = prev_dist_sma10,
    prev_range_pct = prev_range_pct,
    prev_intraday_return = prev_intraday_return,
    avg_volume_5d = avg_volume_5d,
    avg_volume_10d = avg_volume_10d,
    avg_volume_20d = avg_volume_20d,
    volume_ratio_5d = volume_ratio_5d,
    volume_ratio_10d = volume_ratio_10d,
    volume_ratio_20d = volume_ratio_20d,
    enough_5d_history = !is.na(volatility_5d),
    enough_10d_history = !is.na(volatility_10d),
    enough_15d_history = !is.na(volatility_15d),
    enough_20d_history = !is.na(volatility_20d)
  )
}

###############################################################################
# 5) Read signals to process ---------------------------------------------------
###############################################################################

limit_sql <- ""
if (is.finite(MAX_FEATURE_ROWS)) {
  limit_sql <- sprintf("LIMIT %s", as.integer(MAX_FEATURE_ROWS))
}

signals_sql <- sprintf(
  "
  SELECT s.*
  FROM %s s
  LEFT JOIN %s f
    ON f.signal_id = s.signal_id
  WHERE f.signal_id IS NULL
     OR s.created_at >= NOW() - INTERVAL '%s days'
  ORDER BY s.created_at DESC
  %s;
  ",
  signals_tbl_q,
  features_tbl_q,
  as.integer(FEATURE_LOOKBACK_DAYS),
  limit_sql
)

signals_from_tweets <- DBI::dbGetQuery(con, signals_sql) %>% tibble::as_tibble()

cat("Signals selected for feature refresh:", nrow(signals_from_tweets), "\n")

if (nrow(signals_from_tweets) == 0) {
  cat("No signals need feature refresh. Exiting successfully.\n")
  quit(save = "no", status = 0)
}

if (!"signal_id" %in% names(signals_from_tweets)) stop("Expected `signal_id` column in ", SIGNALS_TABLE)
if (!"created_at" %in% names(signals_from_tweets)) stop("Expected `created_at` column in ", SIGNALS_TABLE)
if (!"ticker" %in% names(signals_from_tweets)) stop("Expected `ticker` column in ", SIGNALS_TABLE)

signals_from_tweets <- signals_from_tweets %>% mutate(created_at = as.POSIXct(created_at, tz = "UTC"))

###############################################################################
# 6) Prepare signal base -------------------------------------------------------
###############################################################################

signals_vol_base <- signals_from_tweets %>%
  mutate(
    row_id = row_number(),
    ticker_raw = str_to_upper(as.character(ticker)),
    ticker = purrr::map_chr(ticker_raw, normalize_symbol_for_yahoo),
    created_at_utc = lubridate::with_tz(created_at, "UTC"),
    tweet_ts_et = lubridate::with_tz(created_at_utc, "America/New_York"),
    signal_date = as.Date(tweet_ts_et)
  ) %>%
  filter(!is.na(ticker), ticker != "", !is.na(tweet_ts_et))

cat("Valid feature signals after ticker normalization:", nrow(signals_vol_base), "\n")

if (nrow(signals_vol_base) == 0) {
  cat("No valid tickers for feature calculation. Exiting successfully.\n")
  quit(save = "no", status = 0)
}

print(signals_vol_base %>% summarise(total_rows = n(), unique_tickers = n_distinct(ticker), min_signal_date = min(signal_date, na.rm = TRUE), max_signal_date = max(signal_date, na.rm = TRUE)))

###############################################################################
# 7) Fetch price data ----------------------------------------------------------
###############################################################################

safe_yahoo_to_date <- function(to_date) {
  to_date <- as.Date(to_date)

  safe_to <- min(to_date, Sys.Date() - lubridate::days(1), na.rm = TRUE)

  while (lubridate::wday(safe_to, week_start = 1) %in% c(6, 7)) {
    safe_to <- safe_to - lubridate::days(1)
  }

  as.Date(safe_to)
}

ticker_ranges <- signals_vol_base %>%
  group_by(ticker) %>%
  summarise(
    from_date = min(signal_date, na.rm = TRUE) - lubridate::days(260),
    to_date = safe_yahoo_to_date(
      max(
        AS_OF_DATE,
        max(signal_date, na.rm = TRUE),
        na.rm = TRUE
      )
    ),
    .groups = "drop"
  )

cat("Unique tickers to fetch:", nrow(ticker_ranges), "\n")

price_list <- ticker_ranges %>%
  mutate(
    px = pmap(
      list(ticker, from_date, to_date),
      function(ticker, from_date, to_date) {
        message("Fetching prices for ", ticker, " from ", from_date, " to ", to_date)
        fetch_px_yahoo_chart(ticker, from_date, to_date)
      }
    )
  ) %>%
  select(ticker, px)

price_check <- price_list %>%
  mutate(
    n_prices = purrr::map_int(px, nrow),
    first_price_date = as.Date(purrr::map_chr(px, ~ if (nrow(.x) > 0) as.character(min(.x$date)) else NA_character_)),
    last_price_date = as.Date(purrr::map_chr(px, ~ if (nrow(.x) > 0) as.character(max(.x$date)) else NA_character_))
  )

cat("Price coverage summary:\n")
print(price_check %>% summarise(tickers = n(), tickers_with_prices = sum(n_prices > 0), tickers_without_prices = sum(n_prices == 0), min_price_date = min(first_price_date, na.rm = TRUE), max_price_date = max(last_price_date, na.rm = TRUE)))

###############################################################################
# 8) Calculate features --------------------------------------------------------
###############################################################################

signals_with_features <- signals_vol_base %>%
  left_join(price_list, by = "ticker") %>%
  mutate(
    feature_data = map2(
      tweet_ts_et,
      px,
      ~ calc_signal_features_one(
        ticker = NA_character_,
        tweet_ts_et = .x,
        px = .y,
        market_holidays = us_market_holidays,
        max_feature_lag_days = MAX_FEATURE_LAG_DAYS
      )
    )
  ) %>%
  select(-px) %>%
  tidyr::unnest(feature_data)

cat("Feature rows produced:", nrow(signals_with_features), "\n")

if (nrow(signals_with_features) == 0) {
  cat("No feature rows produced. Exiting successfully.\n")
  quit(save = "no", status = 0)
}

###############################################################################
# 9) Prepare upload ------------------------------------------------------------
###############################################################################

new_entries_upload <- signals_with_features %>%
  mutate(
    signal_id = as.character(signal_id),
    username = if ("username" %in% names(.)) as.character(username) else NA_character_,
    conversation_id = if ("conversation_id" %in% names(.)) as.character(conversation_id) else NA_character_,
    text = if ("text" %in% names(.)) as.character(text) else NA_character_,
    ticker = as.character(ticker),
    direction = if ("direction" %in% names(.)) as.character(direction) else NA_character_,
    horizon = if ("horizon" %in% names(.)) as.character(horizon) else NA_character_,
    rationale = if ("rationale" %in% names(.)) as.character(rationale) else NA_character_,
    evidence_types = if ("evidence_types" %in% names(.)) as.character(evidence_types) else NA_character_,
    evidence_snippet = if ("evidence_snippet" %in% names(.)) as.character(evidence_snippet) else NA_character_,
    ticker_raw = as.character(ticker_raw),
    created_at = as.POSIXct(created_at, tz = "UTC"),
    created_at_utc = as.POSIXct(created_at_utc, tz = "UTC"),
    tweet_ts_et = as.POSIXct(tweet_ts_et, tz = "America/New_York"),
    signal_date = as.Date(signal_date),
    entry_date = as.Date(entry_date),
    feature_date = as.Date(feature_date)
  ) %>%
  distinct(signal_id, .keep_all = TRUE)

cat("Upload rows:", nrow(new_entries_upload), "\n")

print(new_entries_upload %>% summarise(rows = n(), unique_signal_ids = n_distinct(signal_id), duplicate_signal_ids = n() - n_distinct(signal_id), min_signal_date = min(signal_date, na.rm = TRUE), max_signal_date = max(signal_date, na.rm = TRUE), n_with_feature_date = sum(!is.na(feature_date)), n_with_volatility_20d = sum(!is.na(volatility_20d))))

###############################################################################
# 10) Upload/upsert ------------------------------------------------------------
###############################################################################

ensure_missing_columns(con, features_tbl, new_entries_upload)

safe_idx_base <- gsub("[^A-Za-z0-9_]", "_", FEATURES_TABLE)
DBI::dbExecute(con, sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_signal_id ON public.%s(signal_id);", safe_idx_base, FEATURES_TABLE))

staging_table <- paste0("stg_", FEATURES_TABLE, "_", format(Sys.time(), "%Y%m%d%H%M%S"))
staging_tbl <- DBI::Id(schema = "public", table = staging_table)
staging_tbl_q <- as.character(DBI::dbQuoteIdentifier(con, staging_tbl))

DBI::dbWriteTable(con, staging_tbl, new_entries_upload, overwrite = TRUE, row.names = FALSE)

target_cols <- DBI::dbGetQuery(
  con,
  sprintf(
    "
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = %s
    ORDER BY ordinal_position;
    ",
    as.character(DBI::dbQuoteString(con, FEATURES_TABLE))
  )
)$column_name

cols <- intersect(names(new_entries_upload), target_cols)
cols <- setdiff(cols, "updated_at")

if (length(cols) == 0) stop("No overlapping columns found between upload data and ", FEATURES_TABLE)

cols_sql <- paste(DBI::dbQuoteIdentifier(con, cols), collapse = ", ")
update_cols <- setdiff(cols, "signal_id")

update_sql <- paste(
  paste0(DBI::dbQuoteIdentifier(con, update_cols), " = EXCLUDED.", DBI::dbQuoteIdentifier(con, update_cols)),
  collapse = ", "
)

upsert_sql <- sprintf(
  "
  INSERT INTO %s (%s)
  SELECT %s
  FROM %s
  ON CONFLICT (signal_id)
  DO UPDATE SET
    %s,
    updated_at = NOW();
  ",
  features_tbl_q,
  cols_sql,
  cols_sql,
  staging_tbl_q,
  update_sql
)

DBI::dbExecute(con, upsert_sql)
DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s;", staging_tbl_q))

###############################################################################
# 11) Final checks -------------------------------------------------------------
###############################################################################

cat("Final feature table checks:\n")

print(DBI::dbGetQuery(con, sprintf(
  "
  SELECT
    COUNT(*) AS n_rows,
    COUNT(feature_date) AS n_with_feature_date,
    COUNT(volatility_20d) AS n_with_volatility_20d,
    MIN(signal_date) AS first_signal_date,
    MAX(signal_date) AS last_signal_date,
    MAX(updated_at) AS latest_updated_at
  FROM %s;
  ",
  features_tbl_q
)))

print(DBI::dbGetQuery(con, sprintf(
  "
  SELECT
    COUNT(*) AS n_recent_rows,
    COUNT(feature_date) AS n_recent_with_feature_date,
    COUNT(volatility_20d) AS n_recent_with_volatility_20d
  FROM %s
  WHERE created_at >= NOW() - INTERVAL '%s days';
  ",
  features_tbl_q,
  as.integer(FEATURE_LOOKBACK_DAYS)
)))

cat("refresh_trade_signal_features.R completed successfully.\n")
